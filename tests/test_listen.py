"""The tests for the Ring platform."""

import asyncio
import datetime
import json
import time
from collections.abc import AsyncGenerator

import aiohttp
import pytest
from firebase_messaging.fcmpushclient import FcmPushClientRunState
from freezegun.api import FrozenDateTimeFactory
from ring_doorbell import Ring
from ring_doorbell.exceptions import AuthenticationError, RingError
from ring_doorbell.listen import RingEventListener, RingEventListenerConfig

from tests.conftest import load_alert_v1, load_alert_v2, load_fixture


async def test_listen(auth, mocker):
    import firebase_messaging

    ring = Ring(auth)
    listener = RingEventListener(ring)

    await listener.start()
    assert firebase_messaging.FcmPushClient.checkin_or_register.call_count == 1
    assert firebase_messaging.FcmPushClient.start.call_count == 1
    assert listener.subscribed is True
    assert listener.started is True

    with pytest.raises(RingError, match="ID 10 is not a valid callback id"):
        listener.remove_notification_callback(10)

    with pytest.raises(
        RingError,
        match="Cannot remove the default callback for ring-doorbell with value 1",
    ):
        listener.remove_notification_callback(1)

    cbid = listener.add_notification_callback(lambda: 2)
    del listener._callbacks[1]
    listener.remove_notification_callback(cbid)


async def test_active_dings(auth, mocker):
    import firebase_messaging

    ring = Ring(auth)
    listener = RingEventListener(ring)
    await listener.start()
    assert firebase_messaging.FcmPushClient.checkin_or_register.call_count == 1
    assert firebase_messaging.FcmPushClient.start.call_count == 1
    assert listener.subscribed is True
    assert listener.started is True
    num_active = len(ring.active_alerts())
    assert num_active == 0
    alertstoadd = 2
    for i in range(alertstoadd):
        msg = load_alert_v1("doorbot_ding", 123456781, ding_id_inc=i)
        listener._on_notification(msg, "1234567" + str(i))
        msg = load_alert_v2("camera_motion", 123456782, ding_id_inc=i)
        listener._on_notification(msg, "1234567" + str(i))
        msg = load_alert_v1("intercom_unlock", 185036587, ding_id_inc=i)
        listener._on_notification(msg, "1234567" + str(i))

    dings = ring.active_alerts()
    assert len(dings) == num_active + alertstoadd * 3
    # Test with the same id which should overwrite
    # previous and keep the overall count the same
    for i in range(alertstoadd):
        msg = load_alert_v1("doorbot_ding", 123456781, ding_id_inc=i)
        listener._on_notification(msg, "1234567" + str(i))
        msg = load_alert_v2("camera_motion", 123456782, ding_id_inc=i)
        listener._on_notification(msg, "1234567" + str(i))

    dings = ring.active_alerts()
    assert len(dings) == num_active + alertstoadd * 3
    await listener.stop()


async def test_ding_expirey(auth, mocker, freezer: FrozenDateTimeFactory):
    ring = Ring(auth)
    listener = RingEventListener(ring)
    await listener.start()

    assert listener.subscribed is True
    assert listener.started is True

    assert len(ring.push_dings_data) == 0
    assert len(ring.active_alerts()) == 0

    alertstoadd = 2
    for i in range(alertstoadd):
        msg = load_alert_v1("doorbot_ding", 123456781, ding_id_inc=i)
        listener._on_notification(msg, "1234567" + str(i))
        msg = load_alert_v2("camera_motion", 123456782, ding_id_inc=i)
        listener._on_notification(msg, "1234567" + str(i))
        msg = load_alert_v1("intercom_unlock", 185036587, ding_id_inc=i)
        listener._on_notification(msg, "1234567" + str(i))

    assert len(ring.push_dings_data) == 6
    assert len(ring.active_alerts()) == 6

    freezer.tick(datetime.timedelta(minutes=5))

    msg = load_alert_v1("doorbot_ding", 123456781, ding_id_inc=alertstoadd + 1)
    listener._on_notification(msg, "123456781" + str(alertstoadd + 1))

    assert len(ring.push_dings_data) == 1
    assert len(ring.active_alerts()) == 1


def _fast(listener: RingEventListener) -> RingEventListener:
    """Shrink the supervisor's timings so tests run quickly."""
    listener.RETRY_MIN_DELAY = 0.01
    listener.RETRY_MAX_DELAY = 0.05
    listener.WATCHDOG_INTERVAL = 0.01
    listener.RECEIVER_DOWN_GRACE = 0.02
    listener.CONNECTING_POLL_INTERVAL = 0.01
    listener.RECONNECT_MIN_DELAY = 0.01
    listener.RECONNECT_MAX_DELAY = 0.05
    return listener


@pytest.mark.nolistenmock
async def test_listen_subscribe_fail(auth, mocker, caplog, putpatch_status_fixture):
    mocker.patch(
        "firebase_messaging.FcmPushClient.checkin_or_register", return_value="foobar"
    )
    connectmock = mocker.patch("firebase_messaging.FcmPushClient.start")
    mocker.patch("firebase_messaging.FcmPushClient.is_started", return_value=True)

    putpatch_status_fixture.overrides["https://api.ring.com/clients_api/device"] = 401

    ring = Ring(auth)
    listener = _fast(RingEventListener(ring))
    assert await listener.start() is False
    assert listener.started is True
    assert listener.subscribed is False
    assert listener.connected is False
    assert connectmock.call_count == 0
    assert (
        "Unable to subscribe to ring push notifications, response was 401 "
        in caplog.text
    )

    # Keeps retrying in the background and recovers once ring accepts it
    del putpatch_status_fixture.overrides["https://api.ring.com/clients_api/device"]
    await asyncio.sleep(0.2)
    assert listener.subscribed is True
    assert listener.connected is True
    assert connectmock.call_count == 1
    await listener.stop()


@pytest.mark.nolistenmock
async def test_listen_gcm_fail(auth, mocker):
    # Check in gets and error so register is called, the subscribe gets an error
    credentials = json.loads(load_fixture("ring_listen_credentials.json"))
    checkinmock = mocker.patch(
        "firebase_messaging.fcmregister.FcmRegister.gcm_check_in", return_value=None
    )
    registermock = mocker.patch(
        "firebase_messaging.fcmregister.FcmRegister.register", return_value=credentials
    )
    connectmock = mocker.patch("firebase_messaging.FcmPushClient.start")
    mocker.patch("firebase_messaging.FcmPushClient.is_started", return_value=True)

    ring = Ring(auth)
    listener = RingEventListener(ring, credentials)
    assert await listener.start() is True
    # Check in gets and error so register is called
    assert checkinmock.call_count == 1
    assert registermock.call_count == 1
    assert listener.subscribed is True
    assert listener.started is True
    assert connectmock.call_count == 1
    await listener.stop()


@pytest.mark.nolistenmock
async def test_listen_fcm_fail(auth, mocker, caplog):
    checkinmock = mocker.patch(
        "firebase_messaging.FcmPushClient.checkin_or_register", return_value=None
    )
    connectmock = mocker.patch("firebase_messaging.FcmPushClient.start")
    mocker.patch("firebase_messaging.FcmPushClient.is_started", return_value=True)

    ring = Ring(auth)
    listener = _fast(RingEventListener(ring))
    assert await listener.start() is False
    assert listener.subscribed is False
    assert listener.connected is False
    assert connectmock.call_count == 0
    assert "Ring listener unable to check in to fcm" in caplog.text

    await asyncio.sleep(0.1)
    assert checkinmock.call_count >= 2  # still retrying
    await listener.stop()
    assert listener.started is False


@pytest.mark.nolistenmock
@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("Unable to establish subscription with Google Cloud Messaging."),
        aiohttp.ClientConnectionError("down"),
        TimeoutError(),
        RingError("boom"),
        KeyError("token"),
    ],
)
async def test_listen_register_errors_retried(auth, mocker, caplog, error):
    """Any registration error is retried until it succeeds."""
    checkinmock = mocker.patch(
        "firebase_messaging.FcmPushClient.checkin_or_register",
        side_effect=[error, error, "foobar"],
    )
    mocker.patch("firebase_messaging.FcmPushClient.start")
    mocker.patch("firebase_messaging.FcmPushClient.is_started", return_value=True)

    ring = Ring(auth)
    listener = _fast(RingEventListener(ring))
    assert await listener.start() is False
    await asyncio.sleep(0.2)
    assert checkinmock.call_count == 3
    assert listener.connected is True
    assert listener.fcm_token == "foobar"
    assert "retrying in" in caplog.text
    await listener.stop()


@pytest.mark.nolistenmock
async def test_listen_auth_error_backs_off(auth, mocker):
    """A revoked login is retried at the maximum delay, not hammered."""
    checkinmock = mocker.patch(
        "firebase_messaging.FcmPushClient.checkin_or_register", return_value="foobar"
    )
    mocker.patch("firebase_messaging.FcmPushClient.start")
    ring = Ring(auth)
    mocker.patch.object(
        ring, "async_create_session", side_effect=AuthenticationError("revoked")
    )
    listener = _fast(RingEventListener(ring))
    listener.RETRY_MAX_DELAY = 60
    assert await listener.start() is False
    await asyncio.sleep(0.1)
    assert checkinmock.call_count == 1
    await listener.stop()


@pytest.mark.nolistenmock
async def test_stop_during_start(auth, mocker):
    """stop() while start() is still registering cleans up and does not raise."""
    registering = asyncio.Event()

    async def _slow_checkin(*_: object) -> None:
        registering.set()
        await asyncio.sleep(10)

    mocker.patch(
        "firebase_messaging.FcmPushClient.checkin_or_register",
        side_effect=_slow_checkin,
    )
    connectmock = mocker.patch("firebase_messaging.FcmPushClient.start")

    ring = Ring(auth)
    listener = RingEventListener(ring)
    start_task = asyncio.create_task(listener.start())
    await registering.wait()

    await listener.stop()  # FcmPushClient.stop is not mocked: must not crash
    assert await start_task is False
    assert listener.started is False
    assert listener._supervisor_task is None
    assert connectmock.call_count == 0


async def test_start_twice(auth):
    """A second start() does not orphan the first supervisor."""
    ring = Ring(auth)
    listener = RingEventListener(ring)
    assert await listener.start() is True
    task = listener._supervisor_task
    assert await listener.start() is True
    assert listener._supervisor_task is task
    await listener.stop()
    assert task.done()


async def test_session_refresh_repeats(auth, mocker):
    """The session and push subscription are refreshed repeatedly, not once."""
    ring = Ring(auth)
    listener = _fast(RingEventListener(ring))
    listener.SESSION_REFRESH_INTERVAL = 0.01
    create_session = mocker.spy(ring, "async_create_session")
    subscribe = mocker.spy(listener, "add_subscription_to_ring")

    assert await listener.start() is True
    await asyncio.sleep(0.2)

    # Initial session/subscription plus several refreshes
    assert create_session.call_count >= 3
    assert subscribe.call_count >= 3
    assert not listener._supervisor_task.done()
    await listener.stop()


async def test_session_refresh_survives_errors(auth, mocker, caplog):
    """A failed refresh is retried instead of ending the supervisor."""
    ring = Ring(auth)
    listener = _fast(RingEventListener(ring))
    listener.SESSION_REFRESH_INTERVAL = 0.01
    assert await listener.start() is True

    create_session = mocker.patch.object(
        ring, "async_create_session", side_effect=RingError("boom")
    )
    await asyncio.sleep(0.1)
    assert create_session.call_count >= 2
    assert not listener._supervisor_task.done()
    assert "Ring push listener error, retrying in" in caplog.text
    assert "Traceback" not in caplog.text  # expected errors are not dumped

    create_session.side_effect = None
    calls = create_session.call_count
    await asyncio.sleep(0.1)
    assert create_session.call_count > calls
    assert listener.connected is True
    await listener.stop()


async def test_watchdog_reconnects_dead_receiver(auth, mocker):
    """A push client that firebase-messaging gave up on is replaced."""
    ring = Ring(auth)
    listener = _fast(RingEventListener(ring))
    subscribe = mocker.spy(listener, "add_subscription_to_ring")
    assert await listener.start() is True
    dead = listener._receiver

    # What FcmPushClient._terminate() leaves behind after failed reconnects
    dead.run_state = FcmPushClientRunState.STOPPING
    mocker.patch(
        "firebase_messaging.FcmPushClient.checkin_or_register",
        return_value="newtoken",
    )
    await asyncio.sleep(0.1)

    assert listener._receiver is not dead
    subscribe.assert_called_with("newtoken")
    assert listener.connected is True
    await listener.stop()


async def test_watchdog_reconnects_stuck_receiver(auth, mocker, caplog):
    """A push client stuck reconnecting past the grace period is replaced."""
    ring = Ring(auth)
    listener = _fast(RingEventListener(ring))
    assert await listener.start() is True
    stuck = listener._receiver

    stuck.run_state = FcmPushClientRunState.RESETTING
    mocker.patch.object(stuck, "is_started", return_value=False)
    await asyncio.sleep(0.2)

    assert listener._receiver is not stuck
    assert "Push connection stuck, reconnecting" in caplog.text
    await listener.stop()


async def test_listen_resubscribes_on_token_change(auth, mocker):
    """A new FCM token after restart is sent to ring."""
    ring = Ring(auth)
    listener = RingEventListener(ring)
    subscribe = mocker.spy(listener, "add_subscription_to_ring")

    await listener.start()
    await listener.stop()
    subscribe.assert_called_with("foobar")

    mocker.patch(
        "firebase_messaging.FcmPushClient.checkin_or_register",
        return_value="newtoken",
    )
    await listener.start()
    subscribe.assert_called_with("newtoken")
    await listener.stop()


@pytest.fixture(autouse=True)
async def _stop_listeners(monkeypatch) -> AsyncGenerator[None, None]:
    """Stop every listener a test leaves running, even when it fails."""
    created: list[RingEventListener] = []
    original_init = RingEventListener.__init__

    def _init(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        original_init(self, *args, **kwargs)
        created.append(self)

    monkeypatch.setattr(RingEventListener, "__init__", _init)
    yield
    for listener in created:
        if listener.started:
            await listener.stop()


async def test_start_during_stop_keeps_default_callback(auth):
    """start() issued while stop() is running must not lose the ring callback.

    Home Assistant does this when its last entity is removed and a new one
    added straight away.
    """
    ring = Ring(auth)
    listener = _fast(RingEventListener(ring))
    assert await listener.start() is True

    stop_task = asyncio.create_task(listener.stop())
    start_task = asyncio.create_task(listener.start())
    await asyncio.gather(stop_task, start_task)

    assert listener.started is True
    assert ring._add_event_to_dings_data in listener._callbacks.values()
    assert not listener._supervisor_task.done()


async def test_connection_callback(auth):
    """Connection changes are reported and survive stop()/start()."""
    ring = Ring(auth)
    listener = _fast(RingEventListener(ring))
    states: list[bool] = []
    remove = listener.add_connection_callback(states.append)

    assert await listener.start() is True
    await asyncio.sleep(0.05)
    assert states == [True]
    await listener.stop()
    assert states == [True, False]

    await listener.start()
    await asyncio.sleep(0.05)
    assert states == [True, False, True]

    remove()
    await listener.stop()
    assert states == [True, False, True]


async def test_refresh_failure_keeps_connected(auth, caplog, putpatch_status_fixture):
    """A rejected re-subscribe does not report a working connection as down."""
    ring = Ring(auth)
    listener = _fast(RingEventListener(ring))
    listener.SESSION_REFRESH_INTERVAL = 0.01
    assert await listener.start() is True

    putpatch_status_fixture.overrides["https://api.ring.com/clients_api/device"] = 500
    await asyncio.sleep(0.1)
    assert "Unable to re-subscribe to ring push notifications" in caplog.text
    assert listener.subscribed is True
    assert listener.connected is True


@pytest.fixture
def listen_credentials():
    return json.loads(load_fixture("ring_listen_credentials.json"))


async def test_stale_receiver_reconnects_without_checkin(
    auth, mocker, listen_credentials
):
    """A connection that still says STARTED but receives nothing is replaced.

    A plain reconnect reuses the credentials: no FCM check-in, no new token.
    """
    import firebase_messaging

    ring = Ring(auth)
    listener = _fast(RingEventListener(ring, listen_credentials))
    assert await listener.start() is True
    checkins = firebase_messaging.FcmPushClient.checkin_or_register.call_count
    stale = listener._receiver
    stale.run_state = FcmPushClientRunState.STARTED
    stale.last_message_time = time.time() - listener.RECEIVER_STALE_AFTER - 1
    subscribe = mocker.spy(listener, "add_subscription_to_ring")

    await asyncio.sleep(0.1)
    assert listener._receiver is not stale
    assert firebase_messaging.FcmPushClient.checkin_or_register.call_count == checkins
    assert subscribe.call_count == 0


async def test_dead_monitor_task_reconnects(auth, listen_credentials):
    """A push client whose internal tasks died is replaced."""
    ring = Ring(auth)
    listener = _fast(RingEventListener(ring, listen_credentials))
    assert await listener.start() is True
    dead = listener._receiver
    dead.run_state = FcmPushClientRunState.STARTED
    monitor = asyncio.create_task(asyncio.sleep(0))
    await monitor
    dead.tasks = [monitor]

    await asyncio.sleep(0.1)
    assert listener._receiver is not dead


@pytest.mark.nolistenmock
async def test_blocked_push_port_backs_off(auth, mocker, listen_credentials):
    """With MCS unreachable the listener backs off instead of looping.

    Drives a real FcmPushClient whose connection attempts fail, which leaves it
    stuck without ever stopping itself.
    """
    mocker.patch(
        "firebase_messaging.FcmPushClient.checkin_or_register", return_value="foobar"
    )
    mocker.patch(
        "firebase_messaging.FcmPushClient._open_connection",
        side_effect=OSError("blocked"),
    )
    config = RingEventListenerConfig.default_config()
    config.connection_retry_count = 1
    config.start_seconds_before_retry_connect = 0.001

    ring = Ring(auth)
    listener = _fast(RingEventListener(ring, listen_credentials, config=config))
    listener.RETRY_MAX_DELAY = 1
    reconnects: list[float] = []
    original = listener._reconnect

    async def _reconnect() -> None:
        reconnects.append(time.monotonic())
        await original()

    listener._reconnect = _reconnect

    assert await listener.start() is True  # registered and launched
    await asyncio.sleep(0.6)
    assert listener.connected is False
    # Each takeover that never connects backs off further
    assert len(reconnects) >= 2
    assert listener._stuck_count >= 2


@pytest.mark.nolistenmock
async def test_corrupt_credentials_reregister(auth, mocker, listen_credentials):
    """Unusable stored credentials are dropped so a fresh registration happens."""
    register = mocker.patch(
        "firebase_messaging.fcmregister.FcmRegister.register",
        return_value=listen_credentials,
    )
    mocker.patch("firebase_messaging.FcmPushClient.start")
    mocker.patch("firebase_messaging.FcmPushClient.is_started", return_value=True)
    updated: list[dict] = []

    ring = Ring(auth)
    listener = _fast(RingEventListener(ring, {"gcm": {}}, updated.append))
    assert await listener.start() is True  # re-registered straight away
    await asyncio.sleep(0.05)
    assert register.call_count == 1
    assert updated == [listen_credentials]
    assert listener.connected is True


@pytest.mark.nolistenmock
async def test_unexpected_error_logs_traceback_once(auth, mocker, caplog):
    mocker.patch(
        "firebase_messaging.FcmPushClient.checkin_or_register",
        side_effect=[ZeroDivisionError(), ZeroDivisionError(), "foobar"],
    )
    mocker.patch("firebase_messaging.FcmPushClient.start")
    mocker.patch("firebase_messaging.FcmPushClient.is_started", return_value=True)

    ring = Ring(auth)
    listener = _fast(RingEventListener(ring))
    await listener.start()
    await asyncio.sleep(0.1)
    assert listener.connected is True
    unexpected = [r for r in caplog.records if "Unexpected" in r.message]
    assert len(unexpected) == 2
    assert [r.exc_info is not None for r in unexpected] == [True, False]


def test_reconnect_delay(auth):
    """Dead clients are replaced at once after a drop, with backoff otherwise."""
    listener = RingEventListener(Ring(auth))
    listener._seen_started = True
    assert listener._reconnect_delay() == 0

    listener._seen_started = False
    delays = []
    for stuck in range(6):
        listener._stuck_count = stuck
        delays.append(listener._reconnect_delay())
    assert delays == [30, 60, 120, 240, 300, 300]


async def test_watchdog_runs_during_refresh_backoff(auth, mocker, listen_credentials):
    """A long refresh backoff (revoked login) does not stop dead-client recovery."""
    ring = Ring(auth)
    listener = _fast(RingEventListener(ring, listen_credentials))
    listener.SESSION_REFRESH_INTERVAL = 0.01
    listener.RETRY_MAX_DELAY = 60
    assert await listener.start() is True
    mocker.patch.object(
        ring, "async_create_session", side_effect=AuthenticationError("revoked")
    )
    await asyncio.sleep(0.05)  # refresh fails, next attempt in 60s

    dead = listener._receiver
    dead.run_state = FcmPushClientRunState.STOPPING
    await asyncio.sleep(0.1)
    assert listener._receiver is not dead


async def test_stop_propagates_its_own_cancellation(auth):
    """Cancelling stop() itself is not swallowed."""
    listener = RingEventListener(Ring(auth))
    assert await listener.start() is True

    async def _slow_to_cancel() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            await asyncio.sleep(0.2)
            raise

    listener._supervisor_task.cancel()
    listener._supervisor_task = asyncio.create_task(_slow_to_cancel())
    await asyncio.sleep(0)
    stop_task = asyncio.create_task(listener.stop())
    await asyncio.sleep(0.05)
    stop_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stop_task


async def test_connect_reuses_fresh_session(auth, mocker):
    """Connecting does not create a second session right after one was made."""
    ring = Ring(auth)
    await ring.async_create_session()
    create_session = mocker.spy(ring, "async_create_session")
    listener = RingEventListener(ring)
    assert await listener.start() is True
    assert create_session.call_count == 0


async def test_callbacks_survive_internal_reconnect(auth, listen_credentials):
    """Replacing a dead push client keeps every registered notification callback."""
    ring = Ring(auth)
    listener = _fast(RingEventListener(ring, listen_credentials))
    assert await listener.start() is True
    received: list = []
    listener.add_notification_callback(received.append)
    callbacks = dict(listener._callbacks)

    dead = listener._receiver
    dead.run_state = FcmPushClientRunState.STOPPING
    await asyncio.sleep(0.1)
    assert listener._receiver is not dead
    assert listener._callbacks == callbacks

    listener._receiver.callback(load_alert_v2("camera_motion", 123456782), "1")
    assert len(received) == 1


def _push(device_id: int, push: dict) -> dict:
    """Build a v2 FCM message from a recorded push."""
    msg = load_alert_v2("camera_motion", device_id)
    data = json.loads(msg["data"]["data"])
    data["event"] = push["event"]
    msg["data"]["data"] = json.dumps(data)
    android_config = json.loads(msg["data"]["android_config"])
    android_config["body"] = push["body"]
    msg["data"]["android_config"] = json.dumps(android_config)
    return msg


async def test_package_delivery_pushes(auth, freezer: FrozenDateTimeFactory):
    """Replay a real package delivery: two detections under one ding.

    Ring sends one push per detection (person, then package), each with its own
    riid, followed by a repeat of each carrying an AI description.
    """
    freezer.move_to("2026-10-09T03:15:06Z")
    pushes = json.loads(load_fixture("listen/package_delivery_pushes.json"))
    ring = Ring(auth)
    listener = RingEventListener(ring)
    await listener.start()
    events: list = []
    listener.add_notification_callback(events.append)

    for i, push in enumerate(pushes):
        listener._on_notification(_push(123456782, push), str(i))

    assert [e.id for e in events] == ["7694493300000098294"] * 4
    assert [e.kind for e in events] == ["motion"] * 4
    assert [e.state for e in events] == [
        "human",
        "package_delivery",
        "human",
        "package_delivery",
    ]
    assert [e.riid[-4:] for e in events] == ["6164", "e063", "6164", "e063"]
    assert [e.is_update for e in events] == [False, False, True, True]
    assert [e.description_provider for e in events] == [
        "default",
        "default",
        "llm",
        "llm",
    ]
    assert events[1].description == (
        "A package has been detected in your Front Door Package Zone"
    )
    assert events[2].description == (
        "An Amazon delivery person is standing next to Amazon boxes."
    )


@pytest.mark.parametrize(
    ("subtype", "detection_type", "expected"),
    [
        ("package_delivery", "package_delivery", "package_delivery"),
        ("human", "human", "human"),
        ("vehicle", "vehicle", "vehicle"),
        ("motion", "motion", "other_motion"),
        (None, "package_delivery", "package_delivery"),
        ("loitering", "loitering", "other_motion"),
    ],
)
async def test_motion_state_v2(auth, subtype, detection_type, expected):
    ring = Ring(auth)
    listener = RingEventListener(ring)
    await listener.start()
    events: list = []
    listener.add_notification_callback(events.append)

    msg = load_alert_v2("camera_motion", 123456782)
    data = json.loads(msg["data"]["data"])
    data["event"]["ding"]["subtype"] = subtype
    data["event"]["ding"]["detection_type"] = detection_type
    msg["data"]["data"] = json.dumps(data)
    listener._on_notification(msg, "1")
    assert [e.state for e in events] == [expected]
    assert events[0].riid == data["event"]["riid"]
    assert events[0].description == "There is motion at your Garden Floodcam"


async def test_motion_state_legacy_package(auth):
    ring = Ring(auth)
    listener = RingEventListener(ring)
    await listener.start()
    events: list = []
    listener.add_notification_callback(events.append)

    msg = load_alert_v1("camera_motion", 123456782)
    gcm_data = json.loads(msg["data"]["gcmData"])
    gcm_data["subtype"] = "package_delivery"
    msg["data"]["gcmData"] = json.dumps(gcm_data)
    listener._on_notification(msg, "1")
    assert events[0].state == "package_delivery"


def _legacy(gcm_data: dict) -> dict:
    msg = json.loads(load_fixture("listen/fcmdata_v1.json"))
    msg["data"]["gcmData"] = json.dumps(gcm_data)
    return msg


# Recorded from a Garden West battery swap (device ids replaced)
POWER_LOST = {
    "aps": {
        "alert": "Garden West stopped receiving power and is now in Low Power Mode."
    },
    "action": "com.ring.push.DEVICE_SWITCHED_TO_LOW_POWER_MODE",
    "data": {
        "device_name": "Garden West",
        "device_kind": "cocoa_camera_v2",
        "doorbot_id": 987652,
        "location_id": "2dabf1c7-25f6-4db9-889d-de1cc2e4af06",
    },
}
LOW_BATTERY = {
    "aps": {"title": "Battery at 30% - Garden West needs charging."},
    "action": "com.ring.push.LOW_BATTERY_ALERT",
    "data": {
        "doorbot_id": 987652,
        "device_kind": "cocoa_camera_v2",
        "device_name": "Garden West",
        "battery_level": 30,
        "timestamp_epoch_ms": 1791576337000,
    },
}


async def test_device_alerts(auth, freezer: FrozenDateTimeFactory):
    freezer.move_to("2026-10-09T20:05:40Z")  # when the recorded pushes arrived
    ring = Ring(auth)
    listener = RingEventListener(ring)
    await listener.start()
    events: list = []
    listener.add_notification_callback(events.append)

    listener._on_notification(_legacy(POWER_LOST), "1")
    listener._on_notification(_legacy(LOW_BATTERY), "2")
    listener._on_notification(_legacy(LOW_BATTERY), "3")  # Ring sends twice
    unknown = {
        **POWER_LOST,
        "action": "com.ring.push.DEVICE_SWITCHED_TO_FULL_POWER_MODE",
    }
    listener._on_notification(_legacy(unknown), "4")

    power, battery, battery_again, other = events
    assert (power.kind, power.state, power.doorbot_id) == (
        "device_alert",
        "power_lost",
        987652,
    )
    assert power.description == (
        "Garden West stopped receiving power and is now in Low Power Mode."
    )
    assert (battery.state, battery.battery_level, battery.now) == (
        "low_battery",
        30,
        1791576337.0,
    )
    assert battery.description == "Battery at 30% - Garden West needs charging."
    assert battery_again.is_update is True
    # Not mapped yet: still delivered, named after the action
    assert other.state == "device_switched_to_full_power_mode"
    await listener.stop()


async def test_non_device_legacy_pushes_ignored(auth):
    ring = Ring(auth)
    listener = RingEventListener(ring)
    await listener.start()
    events: list = []
    listener.add_notification_callback(events.append)
    listener._on_notification(_legacy({"action": "com.ring.push.X", "data": {}}), "1")
    listener._on_notification(_legacy({"community_alert": {}}), "2")
    listener._on_notification(
        _legacy({"action": "other", "data": {"doorbot_id": 1}}), "3"
    )
    assert events == []
    await listener.stop()
