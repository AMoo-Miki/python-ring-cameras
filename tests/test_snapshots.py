"""Tests for the snapshot tracker."""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from freezegun.api import FrozenDateTimeFactory
from ring_doorbell import Ring, RingEvent, RingSnapshotTracker
from ring_doorbell.exceptions import RingError
from ring_doorbell.listen import RingEventListener
from ring_doorbell.util import image_content_type

from tests.conftest import load_alert_v2

JPEG = b"\xff\xd8\xff\xe0jpeg"
H264 = b"\x00\x00\x00\x01\x67\x4d\x00\x1fh264"
BATTERY_CAM = 987653
WIRED_CAM = 987652
NOW = datetime(2026, 10, 9, 12, 0, 0, tzinfo=timezone.utc)


def _battery(ring: Ring, interval: int | None = 3600) -> None:
    attrs = ring.devices_data["authorized_doorbots"][BATTERY_CAM]
    attrs["settings"]["power_mode"] = "battery"
    if interval is not None:
        attrs["settings"]["lite_24x7"] = {"frequency_secs": interval}


def _event(image_uuid: str | None, taken_at: datetime, **kwargs) -> RingEvent:  # noqa: ANN003
    return RingEvent(
        id=kwargs.get("id", "1"),
        doorbot_id=kwargs.get("doorbot_id", WIRED_CAM),
        device_name="Front Door",
        device_kind="lpd_v1",
        now=taken_at.timestamp(),
        expires_in=180,
        kind=kwargs.get("kind", "motion"),
        state="human",
        is_update=kwargs.get("is_update", False),
        riid=kwargs.get("riid"),
        image_uuid=image_uuid,
        image_taken_at=taken_at.timestamp(),
    )


def test_image_content_type():
    assert image_content_type(JPEG) == "image/jpeg"
    assert image_content_type(H264) == "video/h264"
    assert image_content_type(b"\x00\x00\x01\x67") == "video/h264"
    assert image_content_type(b"GIF89a") == "application/octet-stream"
    assert image_content_type(b"") is None
    assert image_content_type(None) is None


def test_battery_and_interval(ring):
    dev = ring.get_device_by_api_id(BATTERY_CAM)
    assert dev.operating_on_battery is False  # lpd_v1 is wired by kind
    assert dev.snapshot_interval is None
    _battery(ring, 600)
    assert dev.operating_on_battery is True
    assert dev.snapshot_interval == 600
    dev._attrs["settings"]["power_mode"] = "wired"
    assert dev.operating_on_battery is False


def test_is_due(ring):
    tracker = RingSnapshotTracker(ring)
    _battery(ring, 3600)
    battery = ring.get_device_by_api_id(BATTERY_CAM)
    wired = ring.devices()["doorbots"][0]

    # Wired cameras are always polled
    tracker.snapshot_timestamps[wired.device_api_id] = NOW
    assert tracker.is_due(wired, NOW) is True

    # Battery cameras: when nothing is known, then after interval + margin
    assert tracker.is_due(battery, NOW) is True
    tracker.snapshot_timestamps[BATTERY_CAM] = NOW
    assert tracker.is_due(battery, NOW + timedelta(seconds=3659)) is False
    assert tracker.is_due(battery, NOW + timedelta(seconds=3660)) is True

    # A battery camera without a known interval is assumed hourly
    del battery._attrs["settings"]["lite_24x7"]
    assert tracker.is_due(battery, NOW + timedelta(seconds=3659)) is False
    assert tracker.is_due(battery, NOW + timedelta(seconds=3660)) is True

    # A camera with no snapshot yet is not exempted
    tracker.snapshot_timestamps[BATTERY_CAM] = None
    assert tracker.is_due(battery, NOW) is True


async def test_poll_skips_battery_cameras_not_due(ring, mocker, freezer):
    freezer.move_to(NOW)
    _battery(ring, 3600)
    tracker = RingSnapshotTracker(ring)
    changed: list[int] = []
    tracker.add_callback(changed.append)
    timestamps = mocker.patch.object(
        ring,
        "async_get_snapshot_timestamps",
        AsyncMock(
            return_value={WIRED_CAM: NOW - timedelta(seconds=20), BATTERY_CAM: NOW}
        ),
    )

    await tracker.async_poll()
    polled = {d.device_api_id for d in timestamps.call_args.args[0]}
    assert polled == {WIRED_CAM, BATTERY_CAM}
    assert sorted(changed) == [WIRED_CAM, BATTERY_CAM]

    # A minute later the battery camera, snapshotted at NOW, is not asked about
    freezer.move_to(NOW + timedelta(minutes=1))
    timestamps.return_value = {WIRED_CAM: NOW}
    await tracker.async_poll()
    polled = {d.device_api_id for d in timestamps.call_args.args[0]}
    assert polled == {WIRED_CAM}

    # Once its next scheduled snapshot is due it is asked about again
    freezer.move_to(NOW + timedelta(seconds=3660))
    timestamps.return_value = {WIRED_CAM: NOW, BATTERY_CAM: NOW}
    await tracker.async_poll()
    polled = {d.device_api_id for d in timestamps.call_args.args[0]}
    assert polled == {WIRED_CAM, BATTERY_CAM}
    assert changed.count(BATTERY_CAM) == 1  # unchanged time: no callback


async def test_snapshot_image_cached_and_used_by_latest_snapshot(ring, mocker):
    tracker = RingSnapshotTracker(ring)
    dev = ring.get_device_by_api_id(BATTERY_CAM)
    download = mocker.patch.object(
        type(dev), "async_get_stored_snapshot", AsyncMock(return_value=JPEG)
    )
    timestamps = mocker.patch.object(ring, "async_get_snapshot_timestamps")

    tracker.snapshot_timestamps[BATTERY_CAM] = NOW
    tracker.started = True
    ring.snapshot_tracker = tracker
    for _ in range(5):  # bombard it
        assert await dev.async_get_latest_snapshot() == (JPEG, NOW)
    assert download.call_count == 1
    assert timestamps.call_count == 0  # no timestamps request

    # A newer snapshot time: downloaded once more
    tracker.snapshot_timestamps[BATTERY_CAM] = NOW + timedelta(hours=1)
    await dev.async_get_latest_snapshot()
    await dev.async_get_latest_snapshot()
    assert download.call_count == 2

    # Unknown snapshot time: nothing
    assert await tracker.async_get_snapshot_image(WIRED_CAM) == (None, None)


async def test_event_images(ring, mocker):
    tracker = RingSnapshotTracker(ring)
    mocker.patch.object(tracker, "async_poll")
    await tracker.start()
    dev = ring.get_device_by_api_id(WIRED_CAM)
    images = {"a": JPEG, "b": H264}
    fetch = mocker.patch.object(
        type(dev),
        "async_get_event_image",
        AsyncMock(side_effect=lambda uuid: images[uuid]),
    )
    changed: list[int] = []
    tracker.add_callback(changed.append)

    tracker.handle_event(_event("a", NOW, riid="r1"))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    image = tracker.event_image(WIRED_CAM)
    assert (image.content, image.taken_at, image.content_type) == (
        JPEG,
        NOW,
        "image/jpeg",
    )
    assert image.source == "event"
    assert changed == [WIRED_CAM]

    # The AI-description repeat of the same detection: no download
    tracker.handle_event(_event("a", NOW, riid="r1", is_update=True))
    # A later detection under the same ding (e.g. the package): replaces it
    tracker.handle_event(_event("b", NOW + timedelta(seconds=3), riid="r2"))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert fetch.call_count == 2
    image = tracker.event_image(WIRED_CAM)
    assert (image.content, image.content_type) == (H264, "video/h264")

    # An older image arriving late is ignored; non motion/ding kinds too
    tracker.handle_event(_event("a", NOW - timedelta(minutes=1), riid="r0"))
    tracker.handle_event(_event("a", NOW + timedelta(hours=1), kind="on_demand"))
    await asyncio.sleep(0)
    assert fetch.call_count == 2

    # latest_image picks the newest of snapshot and event images
    tracker.snapshot_timestamps[WIRED_CAM] = NOW + timedelta(minutes=5)
    mocker.patch.object(
        type(dev), "async_get_stored_snapshot", AsyncMock(return_value=JPEG)
    )
    latest = await tracker.async_get_latest_image(WIRED_CAM)
    assert latest.source == "snapshot"
    await tracker.stop()


async def test_expired_event_image_is_ignored(ring, mocker):
    tracker = RingSnapshotTracker(ring)
    mocker.patch.object(tracker, "async_poll")
    await tracker.start()
    dev = ring.get_device_by_api_id(WIRED_CAM)
    mocker.patch.object(
        type(dev), "async_get_event_image", AsyncMock(side_effect=RingError("404"))
    )
    tracker.handle_event(_event("gone", NOW))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert tracker.event_image(WIRED_CAM) is None
    await tracker.stop()


async def test_listener_feeds_running_tracker(
    ring, mocker, freezer: FrozenDateTimeFactory
):
    """Notifications reach the tracker through the listener's ring callback."""
    freezer.move_to("2026-10-09T03:15:06Z")
    tracker = RingSnapshotTracker(ring)
    handle = mocker.patch.object(tracker, "handle_event")
    listener = RingEventListener(ring)
    await listener.start()

    msg = load_alert_v2("camera_motion", WIRED_CAM)
    msg["data"]["img"] = json.dumps(
        {"snapshot_uuid": "u-1:987652", "timestamp": 1791515702713}
    )
    listener._on_notification(msg, "1")
    assert handle.call_count == 0  # not started

    mocker.patch.object(tracker, "async_poll")
    await tracker.start()
    listener._on_notification(msg, "2")
    event = handle.call_args.args[0]
    assert event.image_uuid == "u-1:987652"
    assert event.image_taken_at == pytest.approx(1791515702.713)

    await tracker.stop()
    assert ring.snapshot_tracker is None
    await listener.stop()


async def test_start_stop(ring, mocker):
    poll = mocker.patch.object(RingSnapshotTracker, "async_poll", AsyncMock())
    tracker = RingSnapshotTracker(ring)
    tracker.POLL_INTERVAL = 0.01
    await tracker.start()
    await tracker.start()  # idempotent
    assert ring.snapshot_tracker is tracker
    with pytest.raises(RingError):
        await RingSnapshotTracker(ring).start()
    await asyncio.sleep(0.05)
    assert poll.call_count >= 2
    await tracker.stop()
    calls = poll.call_count
    await asyncio.sleep(0.05)
    assert poll.call_count == calls
    assert ring.snapshot_tracker is None


async def test_poll_errors_keep_running(ring, mocker, caplog):
    tracker = RingSnapshotTracker(ring)
    tracker.POLL_INTERVAL = 0.01
    mocker.patch.object(
        ring, "async_get_snapshot_timestamps", AsyncMock(side_effect=RingError("x"))
    )
    await tracker.start()
    await asyncio.sleep(0.05)
    assert "Unable to poll snapshot times" in caplog.text
    assert not tracker._task.done()
    await tracker.stop()


def _poll_count(tracker, battery, start, minutes):  # noqa: ANN202
    """Count the ticks at which the battery camera would be asked about."""
    return sum(
        tracker.is_due(battery, start + timedelta(minutes=m)) for m in range(minutes)
    )


def test_poll_cadence_over_time(ring):
    """Battery cameras: hourly when on schedule; every tick when none/overdue.

    Overdue and no-snapshot cameras being asked every minute is deliberate:
    Ring has had its chance to take the scheduled snapshot, so the next one is
    shown as soon as the camera is back.
    """
    tracker = RingSnapshotTracker(ring)
    _battery(ring, 3600)
    battery = ring.get_device_by_api_id(BATTERY_CAM)

    # On schedule: Ring takes a snapshot every hour; the tracker learns the
    # latest one whenever it asks -> asked once per hour, 61s after each
    tracker.snapshot_timestamps[BATTERY_CAM] = None
    asked = []
    for minute in range(180):
        now = NOW + timedelta(minutes=minute)
        if tracker.is_due(battery, now):
            asked.append(minute)
            tracker.snapshot_timestamps[BATTERY_CAM] = NOW + timedelta(
                minutes=minute // 60 * 60
            )
    assert asked == [0, 61, 121]

    # Overdue (no new snapshot after the first): asked every minute once due
    tracker.snapshot_timestamps[BATTERY_CAM] = NOW
    assert _poll_count(tracker, battery, NOW, 61) == 0
    assert _poll_count(tracker, battery, NOW + timedelta(minutes=61), 60) == 60

    # No snapshot at all: asked every minute
    tracker.snapshot_timestamps[BATTERY_CAM] = None
    assert _poll_count(tracker, battery, NOW, 60) == 60


async def test_poll_waits_for_device_list(mocker):
    """Polling before the device list loads must not cache an empty one."""
    from ring_doorbell import Auth

    ring = Ring(Auth("test"))
    tracker = RingSnapshotTracker(ring)
    devices = mocker.spy(ring, "devices")
    await tracker.async_poll()
    assert devices.call_count == 0


async def test_poll_survives_removed_device(ring, mocker):
    tracker = RingSnapshotTracker(ring)
    ring.video_devices()  # build the device objects
    del ring.devices_data["authorized_doorbots"][BATTERY_CAM]
    timestamps = mocker.patch.object(
        ring, "async_get_snapshot_timestamps", AsyncMock(return_value={})
    )
    await tracker.async_poll()
    polled = {d.device_api_id for d in timestamps.call_args.args[0]}
    assert BATTERY_CAM not in polled
    assert WIRED_CAM in polled


async def test_poll_keeps_known_times(ring, mocker):
    """A camera left out of Ring's answer keeps its time; times never go back."""
    tracker = RingSnapshotTracker(ring)
    changed: list[int] = []
    tracker.add_callback(changed.append)
    tracker.snapshot_timestamps[WIRED_CAM] = NOW
    mocker.patch.object(
        ring,
        "async_get_snapshot_timestamps",
        AsyncMock(return_value={WIRED_CAM: None, BATTERY_CAM: None}),
    )
    await tracker.async_poll()
    assert tracker.snapshot_timestamps[WIRED_CAM] == NOW
    assert tracker.snapshot_timestamps[BATTERY_CAM] is None

    ring.async_get_snapshot_timestamps.return_value = {
        WIRED_CAM: NOW - timedelta(minutes=5)
    }
    await tracker.async_poll()
    assert tracker.snapshot_timestamps[WIRED_CAM] == NOW
    assert changed == []


async def test_failed_snapshot_download_is_not_retried_at_once(ring, mocker, freezer):
    tracker = RingSnapshotTracker(ring)
    dev = ring.get_device_by_api_id(BATTERY_CAM)
    download = mocker.patch.object(
        type(dev), "async_get_stored_snapshot", AsyncMock(side_effect=RingError("x"))
    )
    tracker.snapshot_timestamps[BATTERY_CAM] = NOW
    for _ in range(5):
        assert await tracker.async_get_snapshot_image(BATTERY_CAM) == (None, None)
    assert download.call_count == 1

    # Retried after DOWNLOAD_RETRY_INTERVAL; the error never reaches the caller
    mocker.patch(
        "ring_doorbell.snapshots.time.monotonic",
        return_value=time.monotonic() + tracker.DOWNLOAD_RETRY_INTERVAL,
    )
    download.side_effect = None
    download.return_value = JPEG
    assert await tracker.async_get_snapshot_image(BATTERY_CAM) == (JPEG, NOW)
    assert download.call_count == 2


async def test_duplicate_push_while_downloading(ring, mocker):
    """A repeat push arriving before the first download finishes is skipped."""
    tracker = RingSnapshotTracker(ring)
    mocker.patch.object(tracker, "async_poll")
    await tracker.start()
    dev = ring.get_device_by_api_id(WIRED_CAM)
    fetch = mocker.patch.object(
        type(dev), "async_get_event_image", AsyncMock(return_value=JPEG)
    )
    tracker.handle_event(_event("a", NOW, riid="r1"))
    tracker.handle_event(_event("a", NOW, riid="r1", is_update=True))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert fetch.call_count == 1
    await tracker.stop()


async def test_tracker_error_does_not_block_other_callbacks(ring, mocker):
    tracker = RingSnapshotTracker(ring)
    mocker.patch.object(tracker, "async_poll")
    await tracker.start()
    mocker.patch.object(tracker, "handle_event", side_effect=ValueError("bad"))
    ring._add_event_to_dings_data(_event("a", NOW))
    assert len(ring.push_dings_data) == 1
    await tracker.stop()


@pytest.mark.parametrize(
    ("img", "expected"),
    [
        ({"snapshot_uuid": "u", "timestamp": 1791515702713}, ("u", 1791515702.713)),
        ({"snapshot_uuid": "u", "timestamp": "1791515702713"}, ("u", None)),
        ({"snapshot_uuid": "u"}, ("u", None)),
        ({"timestamp": 1}, (None, None)),
        ("not json", (None, None)),
        (["list"], (None, None)),
    ],
)
async def test_push_image_fields(ring, img, expected):
    """Odd img payloads never break notification parsing."""
    listener = RingEventListener(ring)
    events: list[RingEvent] = []
    await listener.start()
    listener.add_notification_callback(events.append)
    msg = load_alert_v2("camera_motion", WIRED_CAM)
    msg["data"]["img"] = img if isinstance(img, str) else json.dumps(img)
    listener._on_notification(msg, "1")
    assert (events[0].image_uuid, events[0].image_taken_at) == expected
    await listener.stop()


async def test_poll_errors_back_off_and_warn_once(ring, mocker, caplog):
    tracker = RingSnapshotTracker(ring)
    tracker.POLL_INTERVAL = 0.001
    tracker.MAX_POLL_BACKOFF = 0.004
    mocker.patch.object(
        ring, "async_get_snapshot_timestamps", AsyncMock(side_effect=RingError("x"))
    )
    await tracker.start()
    await asyncio.sleep(0.05)
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    assert ring.async_get_snapshot_timestamps.call_count >= 2
    await tracker.stop()


async def test_image_paths_ignore_removed_and_non_cameras(ring, mocker):
    tracker = RingSnapshotTracker(ring)
    mocker.patch.object(tracker, "async_poll")
    await tracker.start()
    ring.video_devices()
    tracker.snapshot_timestamps[BATTERY_CAM] = NOW
    del ring.devices_data["authorized_doorbots"][BATTERY_CAM]
    assert await tracker.async_get_snapshot_image(BATTERY_CAM) == (None, None)

    # A ding from an intercom (not a camera) with an image id
    tracker.handle_event(_event("x", NOW, doorbot_id=185036587, kind="ding"))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert tracker.event_image(185036587) is None
    await tracker.stop()


async def test_latest_snapshot_before_tracker_knows(ring, mocker):
    """Wired cameras fall back to the direct path; battery cameras wait."""
    tracker = RingSnapshotTracker(ring)
    mocker.patch.object(tracker, "async_poll")
    await tracker.start()
    wired = ring.devices()["doorbots"][0]
    direct = mocker.patch.object(
        ring,
        "async_query",
        AsyncMock(
            side_effect=[
                mocker.Mock(json=lambda: {"timestamps": []}),
            ]
        ),
    )
    assert await wired.async_get_latest_snapshot() == (None, None)
    assert direct.call_count == 1  # asked Ring directly

    _battery(ring)
    battery = ring.get_device_by_api_id(BATTERY_CAM)
    assert await battery.async_get_latest_snapshot() == (None, None)
    assert direct.call_count == 1  # did not ask Ring
    await tracker.stop()


async def test_stop_clears_pending_downloads(ring, mocker):
    tracker = RingSnapshotTracker(ring)
    mocker.patch.object(tracker, "async_poll")
    await tracker.start()
    tracker.handle_event(_event("a", NOW))
    await tracker.stop()  # cancels the download before it ran
    assert tracker._pending_event_images == {}


async def test_cancelled_start_leaves_nothing_running(ring, mocker):
    blocked = asyncio.Event()

    async def _slow_poll(*_: object) -> None:
        await blocked.wait()

    mocker.patch.object(RingSnapshotTracker, "async_poll", _slow_poll)
    tracker = RingSnapshotTracker(ring)
    start = asyncio.create_task(tracker.start())
    await asyncio.sleep(0.01)
    start.cancel()
    with pytest.raises(asyncio.CancelledError):
        await start
    assert tracker.started is False
    assert ring.snapshot_tracker is None


async def test_prune_removed_cameras(ring, mocker):
    tracker = RingSnapshotTracker(ring)
    tracker.snapshot_timestamps[BATTERY_CAM] = NOW
    tracker.snapshot_timestamps[555] = NOW  # no longer on the account
    mocker.patch.object(
        ring, "async_get_snapshot_timestamps", AsyncMock(return_value={})
    )
    await tracker.async_poll()
    assert 555 not in tracker.snapshot_timestamps
    assert BATTERY_CAM in tracker.snapshot_timestamps


async def test_push_to_image_end_to_end(ring, mocker, freezer: FrozenDateTimeFactory):
    """A real push through the listener ends up as the camera's event image."""
    freezer.move_to("2026-10-09T03:15:06Z")
    tracker = RingSnapshotTracker(ring)
    mocker.patch.object(tracker, "async_poll")
    await tracker.start()
    dev = ring.get_device_by_api_id(WIRED_CAM)
    fetch = mocker.patch.object(
        type(dev), "async_get_event_image", AsyncMock(return_value=H264)
    )
    listener = RingEventListener(ring)
    await listener.start()

    msg = load_alert_v2("camera_motion", WIRED_CAM)
    msg["data"]["img"] = json.dumps(
        {"snapshot_uuid": "u-1:987652", "timestamp": 1791515702713}
    )
    listener._on_notification(msg, "1")
    for _ in range(3):
        await asyncio.sleep(0)
    fetch.assert_called_once_with("u-1:987652")
    image = tracker.latest_image(WIRED_CAM)
    assert image.content_type == "video/h264"
    assert image.taken_at == datetime.fromtimestamp(1791515702.713, tz=timezone.utc)
    await listener.stop()
    await tracker.stop()


def test_capture_off_is_never_polled(ring):
    tracker = RingSnapshotTracker(ring)
    _battery(ring, 3600)
    battery = ring.get_device_by_api_id(BATTERY_CAM)
    wired = ring.devices()["doorbots"][0]
    assert battery.snapshot_capture_enabled is None
    battery._attrs["settings"]["lite_24x7"]["enabled"] = False
    wired._attrs["settings"]["lite_24x7"] = {"enabled": False}
    assert battery.snapshot_capture_enabled is False
    # Neither with no snapshot, nor overdue, nor wired
    assert tracker.is_due(battery, NOW) is False
    tracker.snapshot_timestamps[BATTERY_CAM] = NOW - timedelta(days=1)
    assert tracker.is_due(battery, NOW) is False
    assert tracker.is_due(wired, NOW) is False


async def test_start_with_subset(ring, mocker):
    timestamps = mocker.patch.object(
        ring, "async_get_snapshot_timestamps", AsyncMock(return_value={})
    )
    tracker = RingSnapshotTracker(ring)
    await tracker.start(devices=[ring.get_device_by_api_id(BATTERY_CAM)])
    polled = {d.device_api_id for d in timestamps.call_args.args[0]}
    assert polled == {BATTERY_CAM}
    await tracker.stop()

    tracker = RingSnapshotTracker(ring)
    await tracker.start(devices=[WIRED_CAM])
    polled = {d.device_api_id for d in timestamps.call_args.args[0]}
    assert polled == {WIRED_CAM}
    await tracker.stop()
