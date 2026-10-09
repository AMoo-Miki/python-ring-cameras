"""Track the latest image of each camera without waking battery cameras."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable

from ring_doorbell.const import KIND_DING, KIND_MOTION
from ring_doorbell.exceptions import AuthenticationError, RingError
from ring_doorbell.util import image_content_type

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ring_doorbell.doorbot import RingDoorBell
    from ring_doorbell.event import RingEvent
    from ring_doorbell.ring import Ring

_logger = logging.getLogger(__name__)

ImageCallable = Callable[[int], None]


@dataclass(frozen=True)
class RingImage:
    """An image of a camera and when it was taken."""

    content: bytes
    taken_at: datetime
    # "image/jpeg" for most cameras; "video/h264" for a raw H.264 frame
    content_type: str
    # "snapshot" (Snapshot Capture) or "event" (attached to a notification)
    source: str
    event_id: str | None = None


class RingSnapshotTracker:
    """Keep each camera's latest Snapshot Capture time and images up to date.

    Polls snapshot capture times for all cameras in one request a minute, but
    asks about a battery camera only once its next scheduled snapshot is due
    (last snapshot + its Snapshot Capture interval + a margin). Ring takes the
    scheduled snapshot by then, so the poll does not prompt an extra one. A
    battery camera without a snapshot time, or overdue for one, is asked every
    minute so its first new snapshot shows as soon as it exists.

    While started it also keeps the image attached to every motion/ding
    notification the event listener receives, downloaded straight away because
    Ring expires those within minutes. Images are kept in memory, the latest one
    per camera and source. Snapshot Capture images are downloaded on demand,
    once per new snapshot.

    Reads (:attr:`snapshot_timestamps`, :meth:`latest_image`) cost nothing.
    Camera data (power mode, Snapshot Capture interval) comes from the device
    list, so keep it updated with :meth:`Ring.async_update_devices`.
    """

    POLL_INTERVAL = 60
    # Margin after a battery camera's next scheduled snapshot before asking.
    BATTERY_MARGIN = 60
    # Assumed Snapshot Capture interval of a battery camera that reports none.
    DEFAULT_BATTERY_INTERVAL = 60 * 60
    # Longest wait between polls while polling fails.
    MAX_POLL_BACKOFF = 60 * 30
    # How long start() waits for the first poll.
    START_TIMEOUT = 10
    # Minimum time before retrying a failed Snapshot Capture image download.
    DOWNLOAD_RETRY_INTERVAL = 60

    def __init__(self, ring: Ring) -> None:
        """Initialise the tracker for a ring account."""
        self._ring = ring
        self.started = False
        self.snapshot_timestamps: dict[int, datetime | None] = {}
        self._snapshot_images: dict[int, RingImage] = {}
        self._event_images: dict[int, RingImage] = {}
        self._pending_event_images: dict[int, str] = {}
        self._failed_downloads: dict[int, tuple[datetime, float]] = {}
        self._callbacks: dict[int, ImageCallable] = {}
        self._callback_counter = 0
        self._task: asyncio.Task | None = None
        self._downloads: set[asyncio.Task] = set()
        self._snapshot_locks: dict[int, asyncio.Lock] = {}
        self._first_poll: asyncio.Event | None = None
        self._tracked: set[int] | None = None

    async def start(self, devices: Iterable[RingDoorBell | int] | None = None) -> None:
        """Start polling and keeping notification images.

        ``devices`` limits tracking to these cameras (objects or api ids); all
        cameras on the account by default. Waits up to START_TIMEOUT seconds
        for the first poll, so snapshot times are known when it returns
        (unless that poll fails or is slow).
        """
        if self.started:
            return
        self._tracked = (
            None
            if devices is None
            else {d if isinstance(d, int) else d.device_api_id for d in devices}
        )
        if self._ring.snapshot_tracker not in (None, self):
            msg = "Another snapshot tracker is already running for this account"
            raise RingError(msg)
        self.started = True
        self._ring.snapshot_tracker = self
        self._first_poll = first_poll = asyncio.Event()
        self._task = task = asyncio.create_task(self._run())
        waiter = asyncio.ensure_future(first_poll.wait())
        try:
            await asyncio.wait(
                {waiter, task},
                timeout=self.START_TIMEOUT,
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            await self.stop()  # do not leave a tracker nobody will stop
            raise
        finally:
            waiter.cancel()

    async def stop(self) -> None:
        """Stop polling and keeping notification images. Cached data is kept."""
        self.started = False
        if self._ring.snapshot_tracker is self:
            self._ring.snapshot_tracker = None
        tasks = [t for t in (self._task, *self._downloads) if t and not t.done()]
        self._task = None
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks)
        self._pending_event_images.clear()

    def add_callback(self, callback: ImageCallable) -> Callable[[], None]:
        """Call ``callback(device_api_id)`` when a camera has a newer image.

        That is a new Snapshot Capture time or a new notification image.
        Returns a function that removes the callback.
        """
        self._callback_counter += 1
        key = self._callback_counter
        self._callbacks[key] = callback

        def _remove() -> None:
            self._callbacks.pop(key, None)

        return _remove

    def latest_image(self, device_api_id: int) -> RingImage | None:
        """Return the newest image downloaded so far, of either source."""
        images = [
            image
            for image in (
                self._snapshot_images.get(device_api_id),
                self._event_images.get(device_api_id),
            )
            if image
        ]
        return max(images, key=lambda image: image.taken_at) if images else None

    def event_image(self, device_api_id: int) -> RingImage | None:
        """Return the image of the camera's latest notification, if kept."""
        return self._event_images.get(device_api_id)

    async def async_get_latest_image(self, device_api_id: int) -> RingImage | None:
        """Return the newest image, downloading a new Snapshot Capture if needed."""
        await self.async_get_snapshot_image(device_api_id)
        return self.latest_image(device_api_id)

    async def async_get_snapshot_image(
        self, device_api_id: int
    ) -> tuple[bytes | None, datetime | None]:
        """Return the latest Snapshot Capture image and its capture time.

        Downloads the image only when the known capture time is newer than the
        cached image, so it can be called as often as needed. A failed download
        is retried at most every DOWNLOAD_RETRY_INTERVAL seconds; meanwhile the
        previous image, if any, is returned.
        """
        if (lock := self._snapshot_locks.get(device_api_id)) is None:
            lock = self._snapshot_locks[device_api_id] = asyncio.Lock()
        async with lock:
            taken_at = self.snapshot_timestamps.get(device_api_id)
            cached = self._snapshot_images.get(device_api_id)
            if (
                taken_at is not None
                and (cached is None or cached.taken_at < taken_at)
                and not self._recently_failed(device_api_id, taken_at)
            ):
                cached = (
                    await self._download_snapshot(device_api_id, taken_at) or cached
                )
        if cached is None:
            return None, None
        return cached.content, cached.taken_at

    def _recently_failed(self, device_api_id: int, taken_at: datetime) -> bool:
        failed = self._failed_downloads.get(device_api_id)
        return bool(
            failed
            and failed[0] == taken_at
            and time.monotonic() - failed[1] < self.DOWNLOAD_RETRY_INTERVAL
        )

    def _camera(self, device_api_id: int) -> RingDoorBell | None:
        """Return the camera with this id, if it is still on the account."""
        from ring_doorbell.doorbot import RingDoorBell

        device = self._ring.get_device_by_api_id(device_api_id)
        if not isinstance(device, RingDoorBell):
            return None
        try:
            _ = device._attrs  # noqa: SLF001
        except KeyError:
            return None  # removed since the device list was built
        return device

    async def _download_snapshot(
        self, device_api_id: int, taken_at: datetime
    ) -> RingImage | None:
        device = self._camera(device_api_id)
        content = None
        if device is not None:
            try:
                content = await device.async_get_stored_snapshot()
            except RingError as ex:
                _logger.debug("Unable to fetch snapshot of %s: %s", device_api_id, ex)
        if not content:
            self._failed_downloads[device_api_id] = (taken_at, time.monotonic())
            return None
        self._failed_downloads.pop(device_api_id, None)
        image = RingImage(
            content,
            taken_at,
            image_content_type(content) or "application/octet-stream",
            "snapshot",
        )
        self._snapshot_images[device_api_id] = image
        return image

    def is_due(self, device: RingDoorBell, now: datetime) -> bool:
        """Return whether a camera's snapshot time should be polled now.

        Wired cameras always are; battery cameras once their next scheduled
        snapshot is due, and every time while they have none or are overdue.
        Cameras with Snapshot Capture turned off never are.
        """
        if device.snapshot_capture_enabled is False:
            return False  # no schedule to follow; notification images only
        if not device.operating_on_battery:
            return True
        last = self.snapshot_timestamps.get(device.device_api_id)
        if last is None:
            # TODO(snapshots): poll these more gently  # noqa: FIX002, TD003
            # They are battery cameras with no snapshot (asleep, low battery,
            # Snapshot Capture off); for now they are asked every minute.
            return True
        interval = device.snapshot_interval or self.DEFAULT_BATTERY_INTERVAL
        return (now - last).total_seconds() >= interval + self.BATTERY_MARGIN

    def _due_devices(self, now: datetime) -> list[RingDoorBell]:
        due = []
        for device in self._ring.video_devices():
            if self._tracked is not None and device.device_api_id not in self._tracked:
                continue
            try:
                if self.is_due(device, now):
                    due.append(device)
            except KeyError:
                # Removed from the account since the device list was built
                continue
        return due

    async def async_poll(self) -> None:
        """Poll the snapshot times of the cameras that are due."""
        if not self._ring.devices_data:
            return  # device list not loaded yet
        self._prune()
        due = self._due_devices(datetime.now(timezone.utc))
        if not due:
            return
        timestamps = await self._ring.async_get_snapshot_timestamps(due)
        for device_api_id, taken_at in timestamps.items():
            known = self.snapshot_timestamps.get(device_api_id)
            if taken_at is None:
                # Keep a known time: Ring left the camera out or had none
                self.snapshot_timestamps.setdefault(device_api_id, None)
            elif known is None or taken_at > known:
                self.snapshot_timestamps[device_api_id] = taken_at
                self._notify(device_api_id)

    def _prune(self) -> None:
        """Forget cameras that are no longer on the account."""
        current = {
            device_id
            for family in ("doorbots", "authorized_doorbots", "stickup_cams")
            for device_id in self._ring.devices_data.get(family, {})
        }
        for data in (
            self.snapshot_timestamps,
            self._snapshot_images,
            self._event_images,
            self._failed_downloads,
            self._snapshot_locks,
        ):
            for device_id in [d for d in data if d not in current]:
                del data[device_id]

    async def _run(self) -> None:
        delay = self.POLL_INTERVAL
        warned = False
        while self.started:
            started_at = time.monotonic()
            try:
                await self.async_poll()
            except Exception as ex:  # noqa: BLE001
                if isinstance(ex, AuthenticationError):
                    delay = self.MAX_POLL_BACKOFF
                else:
                    delay = min(delay * 2, self.MAX_POLL_BACKOFF)
                (_logger.debug if warned else _logger.warning)(
                    "Unable to poll snapshot times, retrying in %s seconds: %s",
                    delay,
                    ex,
                )
                warned = True
                wait = delay
            else:
                if warned:
                    _logger.info("Polling snapshot times again")
                delay = self.POLL_INTERVAL
                warned = False
                wait = max(0, self.POLL_INTERVAL - (time.monotonic() - started_at))
            if self._first_poll:
                self._first_poll.set()
            await asyncio.sleep(wait)

    def handle_event(self, event: RingEvent) -> None:
        """Keep the image of a motion/ding notification.

        Called by the event listener for every notification while started.
        """
        if (
            not self.started
            or event.kind not in (KIND_MOTION, KIND_DING)
            or not event.image_uuid
        ):
            return
        taken_at = datetime.fromtimestamp(
            event.image_taken_at or event.now, tz=timezone.utc
        )
        current = self._event_images.get(event.doorbot_id)
        if (current and current.taken_at >= taken_at) or (
            self._pending_event_images.get(event.doorbot_id) == event.image_uuid
        ):
            return  # a repeat push, or an older detection arriving late
        self._pending_event_images[event.doorbot_id] = event.image_uuid
        task = asyncio.create_task(self._download_event_image(event, taken_at))
        self._downloads.add(task)
        task.add_done_callback(self._downloads.discard)

    async def _download_event_image(self, event: RingEvent, taken_at: datetime) -> None:
        try:
            device = self._camera(event.doorbot_id)
            if device is None or not event.image_uuid:
                return
            try:
                content = await device.async_get_event_image(event.image_uuid)
            except RingError as ex:
                _logger.debug("Unable to fetch image of event %s: %s", event.id, ex)
                return
            if not content:
                return
            current = self._event_images.get(event.doorbot_id)
            if current and current.taken_at >= taken_at:
                return
            self._event_images[event.doorbot_id] = RingImage(
                content,
                taken_at,
                image_content_type(content) or "application/octet-stream",
                "event",
                event.id,
            )
            self._notify(event.doorbot_id)
        finally:
            if self._pending_event_images.get(event.doorbot_id) == event.image_uuid:
                del self._pending_event_images[event.doorbot_id]

    def _notify(self, device_api_id: int) -> None:
        for callback in list(self._callbacks.values()):
            try:
                callback(device_api_id)
            except Exception:  # noqa: PERF203
                _logger.exception("Error in snapshot tracker callback")
