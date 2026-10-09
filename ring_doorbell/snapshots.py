"""Track the latest image of each camera without waking battery cameras."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable

from ring_doorbell.const import KIND_DING, KIND_MOTION
from ring_doorbell.exceptions import AuthenticationError, RingError
from ring_doorbell.util import image_content_type, parse_datetime

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ring_doorbell.doorbot import RingDoorBell
    from ring_doorbell.event import RingEvent
    from ring_doorbell.ring import Ring

_logger = logging.getLogger(__name__)

ImageCallable = Callable[[int], None]

SOURCE_SNAPSHOT = "snapshot"
SOURCE_EVENT = "event"
SOURCE_RECORDING = "recording"
SOURCES = (SOURCE_SNAPSHOT, SOURCE_EVENT, SOURCE_RECORDING)


@dataclass(frozen=True)
class RingImage:
    """An image of a camera and when it was taken."""

    content: bytes
    taken_at: datetime
    # "image/jpeg"; "video/h264" for a raw H.264 notification frame that could
    # not be converted because ffmpeg is unavailable
    content_type: str
    # "snapshot" (Snapshot Capture), "event" (attached to a notification) or
    # "recording" (last frame of a motion, ding or live view recording)
    source: str
    # The ding id of an event or recording image; for a recording, pass it to
    # RingDoorBell.async_recording_url for the video itself
    event_id: str | None = None


class RingSnapshotTracker:
    """Keep the newest image of each camera in memory, of three sources.

    - Snapshot Capture: polls snapshot capture times for all cameras in one
      request a minute, but asks about a battery camera only once its next
      scheduled snapshot is due (last snapshot + its Snapshot Capture interval
      + a margin), because asking makes Ring refresh the camera's snapshot. A
      battery camera without a snapshot time, or overdue for one, is asked
      every minute so its first new snapshot shows as soon as it exists. Each
      new snapshot is downloaded as soon as its time is seen.
    - Notifications: the image attached to every motion/ding notification the
      event listener receives, downloaded straight away because Ring expires
      those within minutes.
    - Recordings: the last frame of each new motion, ding or live view
      recording, taken with ffmpeg as soon as the recording is ready. These
      are found in camera history, so they need history to be updated (e.g.
      ``async_history`` every minute); the tracker does not query it itself.

    An image replaces the previous one of its source only when it is valid.
    Raw H.264 notification frames are converted to JPEG with ffmpeg.

    Reads (:attr:`snapshot_timestamps`, :meth:`latest_image`, :meth:`image`)
    cost nothing and make no requests. Camera data (power mode, Snapshot
    Capture interval) comes from the device list, so keep it updated with
    :meth:`Ring.async_update_devices`.
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
    # Minimum time before retrying a failed snapshot or recording download.
    DOWNLOAD_RETRY_INTERVAL = 60
    # Longest an ffmpeg run may take.
    FFMPEG_TIMEOUT = 30

    def __init__(self, ring: Ring, ffmpeg_binary: str | None = "ffmpeg") -> None:
        """Initialise the tracker for a ring account.

        ``ffmpeg_binary`` is used to take frames from recordings and to convert
        raw H.264 notification frames; None does without both.
        """
        self._ring = ring
        self._ffmpeg = ffmpeg_binary
        self.started = False
        self.snapshot_timestamps: dict[int, datetime | None] = {}
        self._images: dict[str, dict[int, RingImage]] = {s: {} for s in SOURCES}
        self._pending_event_images: dict[int, str] = {}
        self._pending_recordings: dict[int, int] = {}
        self._failed: dict[tuple[str, int], tuple[Any, float]] = {}
        self._callbacks: dict[int, ImageCallable] = {}
        self._callback_counter = 0
        self._task: asyncio.Task | None = None
        self._downloads: set[asyncio.Task] = set()
        self._first_poll: asyncio.Event | None = None
        self._tracked: set[int] | None = None

    async def start(self, devices: Iterable[RingDoorBell | int] | None = None) -> None:
        """Start polling and keeping notification and recording images.

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
        """Stop polling and keeping images. Cached images are kept."""
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
        self._pending_recordings.clear()

    def add_callback(self, callback: ImageCallable) -> Callable[[], None]:
        """Call ``callback(device_api_id)`` when a camera has a newer image.

        That is a new Snapshot Capture time (once its image download was
        tried), or a new notification or recording image.
        Returns a function that removes the callback.
        """
        self._callback_counter += 1
        key = self._callback_counter
        self._callbacks[key] = callback

        def _remove() -> None:
            self._callbacks.pop(key, None)

        return _remove

    def latest_image(self, device_api_id: int) -> RingImage | None:
        """Return the camera's newest image of any source. Makes no requests."""
        images = [
            image
            for images in self._images.values()
            if (image := images.get(device_api_id))
        ]
        return max(images, key=lambda image: image.taken_at) if images else None

    def image(self, device_api_id: int, source: str) -> RingImage | None:
        """Return the camera's latest image of one source. Makes no requests."""
        return self._images[source].get(device_api_id)

    def event_image(self, device_api_id: int) -> RingImage | None:
        """Return the image of the camera's latest notification, if kept."""
        return self.image(device_api_id, SOURCE_EVENT)

    def _recently_failed(self, source: str, device_api_id: int, key: object) -> bool:
        failed = self._failed.get((source, device_api_id))
        return bool(
            failed
            and failed[0] == key
            and time.monotonic() - failed[1] < self.DOWNLOAD_RETRY_INTERVAL
        )

    def _store(self, device_api_id: int, image: RingImage) -> bool:
        """Keep an image unless one of its source at least as new is kept."""
        images = self._images[image.source]
        current = images.get(device_api_id)
        if current and current.taken_at >= image.taken_at:
            return False
        images[device_api_id] = image
        return True

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

    async def _async_ffmpeg(
        self, *args: str, data: bytes | None = None
    ) -> bytes | None:
        """Run ffmpeg to output one JPEG frame; return it, or None on failure."""
        if not self._ffmpeg:
            return None
        try:
            proc = await asyncio.create_subprocess_exec(
                self._ffmpeg,
                "-hide_banner",
                "-loglevel",
                "error",
                *args,
                "-frames:v",
                "1",
                "-q:v",
                "2",
                "-f",
                "image2",
                "-c:v",
                "mjpeg",
                "pipe:1",
                stdin=asyncio.subprocess.PIPE
                if data is not None
                else asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except OSError as ex:
            _logger.warning(
                "Unable to run ffmpeg (%s), recording frames and H.264 "
                "notification images will not be converted: %s",
                self._ffmpeg,
                ex,
            )
            self._ffmpeg = None
            return None
        try:
            out, err = await asyncio.wait_for(
                proc.communicate(data), self.FFMPEG_TIMEOUT
            )
        except (asyncio.TimeoutError, asyncio.CancelledError):
            proc.kill()
            await proc.wait()
            raise
        if proc.returncode or not _is_jpeg(out):
            _logger.debug(
                "ffmpeg exited with %s: %s", proc.returncode, err.decode()[-500:]
            )
            return None
        return out

    async def _async_image(
        self,
        content: bytes | None,
        taken_at: datetime,
        source: str,
        event_id: str | None = None,
    ) -> RingImage | None:
        """Return downloaded image data as a RingImage, or None if invalid."""
        if not content:
            return None
        content_type = image_content_type(content)
        if content_type == "video/h264":
            try:
                jpeg = await self._async_ffmpeg(
                    "-f", "h264", "-i", "pipe:0", data=content
                )
            except asyncio.TimeoutError:
                jpeg = None
            if jpeg:
                content, content_type = jpeg, "image/jpeg"
            elif self._ffmpeg:
                return None  # ffmpeg could not decode it
        elif not _is_jpeg(content):
            return None
        return RingImage(content, taken_at, content_type or "", source, event_id)

    async def _async_download_snapshot(
        self, device_api_id: int, taken_at: datetime
    ) -> bool:
        """Download a camera's Snapshot Capture image; return if it is kept."""
        device = self._camera(device_api_id)
        content = None
        if device is not None:
            try:
                content = await device.async_get_stored_snapshot()
            except RingError as ex:
                _logger.debug("Unable to fetch snapshot of %s: %s", device_api_id, ex)
        image = await self._async_image(content, taken_at, SOURCE_SNAPSHOT)
        if image is None:
            self._failed[SOURCE_SNAPSHOT, device_api_id] = (taken_at, time.monotonic())
            return False
        self._failed.pop((SOURCE_SNAPSHOT, device_api_id), None)
        return self._store(device_api_id, image)

    def _snapshots_to_download(self) -> list[tuple[int, datetime]]:
        """Return the cameras whose latest snapshot time has no image yet."""
        cached = self._images[SOURCE_SNAPSHOT]
        return [
            (device_api_id, taken_at)
            for device_api_id, taken_at in self.snapshot_timestamps.items()
            if taken_at is not None
            and (
                (image := cached.get(device_api_id)) is None
                or image.taken_at < taken_at
            )
            and not self._recently_failed(SOURCE_SNAPSHOT, device_api_id, taken_at)
        ]

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
        """Poll the snapshot times of the cameras that are due.

        Then download the image of every new snapshot time, retrying failed
        downloads at most every DOWNLOAD_RETRY_INTERVAL seconds.
        """
        if not self._ring.devices_data:
            return  # device list not loaded yet
        self._prune()
        changed: set[int] = set()
        if due := self._due_devices(datetime.now(timezone.utc)):
            timestamps = await self._ring.async_get_snapshot_timestamps(due)
            for device_api_id, taken_at in timestamps.items():
                known = self.snapshot_timestamps.get(device_api_id)
                if taken_at is None:
                    # Keep a known time: Ring left the camera out or had none
                    self.snapshot_timestamps.setdefault(device_api_id, None)
                elif known is None or taken_at > known:
                    self.snapshot_timestamps[device_api_id] = taken_at
                    changed.add(device_api_id)
        downloads = self._snapshots_to_download()
        kept = await asyncio.gather(
            *(self._async_download_snapshot(*download) for download in downloads)
        )
        changed.update(d for (d, _), ok in zip(downloads, kept) if ok)
        for device_api_id in changed:
            self._notify(device_api_id)

    def _prune(self) -> None:
        """Forget cameras that are no longer on the account."""
        current = {
            device_id
            for family in ("doorbots", "authorized_doorbots", "stickup_cams")
            for device_id in self._ring.devices_data.get(family, {})
        }
        per_camera: list[dict[int, Any]] = [
            self.snapshot_timestamps,
            *self._images.values(),
        ]
        for data in per_camera:
            for device_id in [d for d in data if d not in current]:
                del data[device_id]
        for key in [k for k in self._failed if k[1] not in current]:
            del self._failed[key]

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

    def _spawn(self, coro: Any) -> None:
        task = asyncio.create_task(coro)
        self._downloads.add(task)
        task.add_done_callback(self._downloads.discard)

    def _is_tracked(self, device_api_id: int) -> bool:
        return self._tracked is None or device_api_id in self._tracked

    def handle_event(self, event: RingEvent) -> None:
        """Keep the image of a motion/ding notification.

        Called by the event listener for every notification while started.
        """
        if (
            not self.started
            or event.kind not in (KIND_MOTION, KIND_DING)
            or not event.image_uuid
            or not self._is_tracked(event.doorbot_id)
        ):
            return
        taken_at = datetime.fromtimestamp(
            event.image_taken_at or event.now, tz=timezone.utc
        )
        current = self._images[SOURCE_EVENT].get(event.doorbot_id)
        if (current and current.taken_at >= taken_at) or (
            self._pending_event_images.get(event.doorbot_id) == event.image_uuid
        ):
            return  # a repeat push, or an older detection arriving late
        self._pending_event_images[event.doorbot_id] = event.image_uuid
        self._spawn(self._download_event_image(event, taken_at))

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
            image = await self._async_image(content, taken_at, SOURCE_EVENT, event.id)
            if image and self._store(event.doorbot_id, image):
                self._notify(event.doorbot_id)
        finally:
            if self._pending_event_images.get(event.doorbot_id) == event.image_uuid:
                del self._pending_event_images[event.doorbot_id]

    def handle_history(self, device_api_id: int, history: list[dict[str, Any]]) -> None:
        """Keep the last frame of the camera's newest ready recording.

        Called with every history response while started.
        """
        if not self.started or not self._ffmpeg or not self._is_tracked(device_api_id):
            return
        newest: tuple[int, datetime] | None = None
        for entry in history:
            recording = entry.get("recording") or {}
            if recording.get("status") != "ready" or not entry.get("id"):
                continue
            if (end := _recording_end(entry)) and (newest is None or end > newest[1]):
                newest = (entry["id"], end)
        if newest is None:
            return
        recording_id, end = newest
        current = self._images[SOURCE_RECORDING].get(device_api_id)
        if (
            (current and current.taken_at >= end)
            or self._pending_recordings.get(device_api_id) == recording_id
            or self._recently_failed(SOURCE_RECORDING, device_api_id, recording_id)
        ):
            return
        self._pending_recordings[device_api_id] = recording_id
        self._spawn(self._download_recording_frame(device_api_id, recording_id, end))

    async def _download_recording_frame(
        self, device_api_id: int, recording_id: int, end: datetime
    ) -> None:
        image = None
        try:
            if device := self._camera(device_api_id):
                # The URL expires within minutes, so it is fetched right away
                url = await device.async_recording_url(recording_id)
                if url:
                    # The first frame of the last second
                    jpeg = await self._async_ffmpeg("-sseof", "-1", "-i", url)
                    image = await self._async_image(
                        jpeg, end, SOURCE_RECORDING, str(recording_id)
                    )
        except (RingError, asyncio.TimeoutError) as ex:
            _logger.debug(
                "Unable to take a frame of recording %s: %s", recording_id, ex
            )
        finally:
            if self._pending_recordings.get(device_api_id) == recording_id:
                del self._pending_recordings[device_api_id]
        if image is None:
            self._failed[SOURCE_RECORDING, device_api_id] = (
                recording_id,
                time.monotonic(),
            )
            return
        self._failed.pop((SOURCE_RECORDING, device_api_id), None)
        if self._store(device_api_id, image):
            self._notify(device_api_id)

    def _notify(self, device_api_id: int) -> None:
        for callback in list(self._callbacks.values()):
            try:
                callback(device_api_id)
            except Exception:  # noqa: PERF203
                _logger.exception("Error in snapshot tracker callback")


def _is_jpeg(content: bytes | None) -> bool:
    """Return whether this is a complete JPEG (start and end markers)."""
    return bool(
        content
        and content.startswith(b"\xff\xd8\xff")
        and content.endswith(b"\xff\xd9")
    )


def _recording_end(entry: dict[str, Any]) -> datetime | None:
    """Return when a history entry's recording ends (its newest frame)."""
    created_at = entry.get("created_at")
    if isinstance(created_at, str):
        created_at = parse_datetime(created_at)
    if not isinstance(created_at, datetime):
        return None
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    return created_at + timedelta(seconds=float(entry.get("duration") or 0))
