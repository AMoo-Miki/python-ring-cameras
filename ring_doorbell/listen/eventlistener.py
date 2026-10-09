"""Module for listening to firebase cloud messages and updating dings."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

from aiohttp import ClientError
from firebase_messaging import (
    FcmPushClient,
    FcmPushClientRunState,
    FcmRegisterConfig,
)

from ring_doorbell.const import (
    API_URI,
    API_VERSION,
    DEFAULT_LISTEN_EVENT_EXPIRES_IN,
    FCM_API_KEY,
    FCM_APP_ID,
    FCM_PROJECT_ID,
    FCM_RING_SENDER_ID,
    KIND_DING,
    KIND_INTERCOM_UNLOCK,
    KIND_MOTION,
    KIND_MOTION_OTHER,
    MOTION_SUBTYPES,
    PUSH_ACTION_DING,
    PUSH_ACTION_INTERCOM_UNLOCK,
    PUSH_ACTION_MOTION,
    PUSH_NOTIFICATION_KINDS,
    SUBSCRIPTION_ENDPOINT,
)
from ring_doorbell.event import RingEvent, RingEventKey
from ring_doorbell.exceptions import AuthenticationError, RingError
from ring_doorbell.util import parse_datetime

from .listenerconfig import RingEventListenerConfig

if TYPE_CHECKING:
    from ring_doorbell.ring import Ring

_logger = logging.getLogger(__name__)

OnNotificationCallable = Callable[[RingEvent], None]
CredentialsUpdatedCallable = Callable[[dict[str, Any]], None]
ConnectionCallable = Callable[[bool], None]


@dataclass
class _Schedule:
    """Backoff and timing state of the listener's supervisor."""

    connect_delay: float
    refresh_delay: float
    next_refresh: float
    next_connect: float = 0.0
    down_since: float | None = None
    dead_since: float | None = None


class RingEventListener:
    """Class to connect to firebase cloud messaging."""

    # Ring appears to drop session info after ~24 hours, which silently stops
    # push notifications (ring-client-api refreshes every 12 hours for the same
    # reason), so the session and subscription are refreshed for as long as the
    # listener runs.
    SESSION_REFRESH_INTERVAL = 60 * 60 * 12
    # Backoff between failed attempts.
    RETRY_MIN_DELAY = 30
    RETRY_MAX_DELAY = 60 * 30
    # How often a healthy push connection is checked.
    WATCHDOG_INTERVAL = 60
    # How often it is checked while not connected, so a new connection is
    # reported promptly.
    CONNECTING_POLL_INTERVAL = 1
    # firebase-messaging retries a dropped connection itself (5 attempts with
    # growing sleeps; longer if connects time out) and then gives up for good.
    # While it is still retrying it gets this long before the listener takes
    # over.
    RECEIVER_DOWN_GRACE = 60 * 5
    # Once a push client is dead it is replaced at once if it had connected,
    # otherwise after a delay that doubles with each takeover that never
    # connects, so a long outage neither spins nor delays recovery much.
    RECONNECT_MIN_DELAY = 30
    RECONNECT_MAX_DELAY = 60 * 5
    # With the configured 60 second server heartbeat, a connection that has
    # received nothing for this long is dead even if it still reports STARTED.
    RECEIVER_STALE_AFTER = 60 * 5
    # On this many takeovers in a row without a connection, check in with FCM
    # and re-subscribe once, in case the registration itself is the problem.
    FULL_RECONNECT_AFTER = 3

    _EXPECTED_ERRORS = (
        RingError,
        ClientError,
        asyncio.TimeoutError,
        TimeoutError,
        OSError,
        RuntimeError,
    )

    def __init__(
        self,
        ring: Ring,
        credentials: dict[str, Any] | None = None,
        credentials_updated_callback: CredentialsUpdatedCallable | None = None,
        *,
        config: RingEventListenerConfig | None = None,
    ) -> None:
        """Initialise the event listener with credentials.

        Provide a callback for when credentials are updated by FCM.
        """
        self._ring = ring

        self._callbacks: dict[int, OnNotificationCallable] = {}
        self._connection_callbacks: dict[int, ConnectionCallable] = {}
        self._connection_callback_counter = 0
        self.subscribed = False
        self.started = False
        self._device_model = self._ring.auth.get_device_model()

        self._credentials = credentials
        self._credentials_updated_callback = credentials_updated_callback

        self._receiver: FcmPushClient | None = None
        self._config: RingEventListenerConfig = (
            config or RingEventListenerConfig.default_config()
        )

        self._subscription_counter = 1
        self._intercom_unlock_counter: dict[int, int] = {}

        # asyncio primitives are created lazily inside the running loop
        self._lifecycle_lock: asyncio.Lock | None = None
        self._first_attempt_done: asyncio.Event | None = None
        self._supervisor_task: asyncio.Task | None = None
        self._connect_ok = False
        self._seen_started = False
        self._stuck_count = 0
        self._last_connected = False
        self._unexpected_logged: set[type[BaseException]] = set()
        self.fcm_token: str | None = None

        self._seen_events: set[RingEventKey] = set()

    @property
    def connected(self) -> bool:
        """Return True when the push connection is up and subscribed with ring.

        ``started`` only means the listener is running; it may still be
        registering or recovering from a dropped connection.
        """
        return bool(
            self.started
            and self.subscribed
            and self._receiver
            and self._receiver.is_started()
        )

    def add_connection_callback(
        self, callback: ConnectionCallable
    ) -> Callable[[], None]:
        """Call ``callback(connected)`` whenever ``connected`` changes.

        Returns a function that removes the callback. Unlike notification
        callbacks these are kept across stop() and start().
        """
        self._connection_callback_counter += 1
        key = self._connection_callback_counter
        self._connection_callbacks[key] = callback

        def _remove() -> None:
            self._connection_callbacks.pop(key, None)

        return _remove

    def _update_connected(self) -> None:
        connected = self.connected
        if connected == self._last_connected:
            return
        self._last_connected = connected
        _logger.debug("Push connection %s", "up" if connected else "down")
        for callback in list(self._connection_callbacks.values()):
            try:
                callback(connected)
            except Exception:  # noqa: PERF203
                _logger.exception("Error in ring connection callback")

    def _credentials_updated_cb(self, creds: dict[str, Any]) -> None:
        self._credentials = creds
        if self._credentials_updated_callback:
            self._credentials_updated_callback(creds)

    async def add_subscription_to_ring(self, token: str) -> None:
        """Add subscription to ring."""
        if not self._ring.session:
            await self._ring.async_create_session()

        session_patch_data = {
            "device": {
                "metadata": {
                    "api_version": API_VERSION,
                    "device_model": self._device_model,
                    "pn_dict_version": "2.0.0",
                    "pn_service": "fcm",
                },
                "os": "android",
                "push_notification_token": token,
            }
        }
        resp = await self._ring.auth.async_query(
            API_URI + SUBSCRIPTION_ENDPOINT,
            method="PATCH",
            json=session_patch_data,
            raise_for_status=False,
        )
        if resp.status_code != 204:
            _logger.error(
                "Unable to subscribe to ring push notifications, response was %s %s",
                resp.status_code,
                resp.text,
            )
            self.subscribed = False
            return

        self.subscribed = True
        # Update devices for the intercom unlock events
        if not self._ring.devices_data:
            await self._ring.async_update_devices()

    def add_notification_callback(self, callback: OnNotificationCallable) -> int:
        """Add a callback to be notified on event."""
        sub_id = self._subscription_counter

        self._callbacks[sub_id] = callback
        self._subscription_counter += 1

        return sub_id

    def remove_notification_callback(self, subscription_id: int) -> None:
        """Remove a notification callback by id."""
        if subscription_id == 1:
            msg = "Cannot remove the default callback for ring-doorbell with value 1"
            raise RingError(msg)

        if subscription_id not in self._callbacks:
            msg = f"ID {subscription_id} is not a valid callback id"
            raise RingError(msg)

        del self._callbacks[subscription_id]

    def _lock(self) -> asyncio.Lock:
        if self._lifecycle_lock is None:
            self._lifecycle_lock = asyncio.Lock()
        return self._lifecycle_lock

    async def stop(self) -> None:
        """Stop the listener and clear the notification callbacks.

        Safe to call at any time, including while start() is still connecting.
        start() and stop() are serialised, so a start() issued during a stop()
        runs after it completes.
        """
        async with self._lock():
            self.started = False
            self._connect_ok = False

            task = self._supervisor_task
            self._supervisor_task = None
            if task and not task.done():
                task.cancel()
                if task is not asyncio.current_task():
                    # Unlike awaiting the task, this does not swallow a
                    # cancellation of stop() itself
                    await asyncio.wait({task})

            await self._stop_receiver()
            self._callbacks = {}
            self._update_connected()

    async def start(
        self,
        *,
        timeout: int = 10,
    ) -> bool:
        """Start the listener.

        Registering, subscribing, refreshing the session and recovering a dead
        push connection happen in a background supervisor task that retries
        until stop() is called. This waits up to ``timeout`` seconds for the
        first attempt and returns whether it registered, subscribed and launched
        the push client; on False the listener keeps trying in the background.
        Use ``connected`` or add_connection_callback() to follow the actual
        connection state.
        """
        async with self._lock():
            if self.started:
                return self._connect_ok
            _logger.debug("Starting event listener")
            self.started = True
            self._connect_ok = False
            self._seen_started = False
            self._stuck_count = 0
            self._first_attempt_done = first_attempt = asyncio.Event()
            self.add_notification_callback(self._ring._add_event_to_dings_data)  # noqa: SLF001
            self._supervisor_task = task = asyncio.create_task(self._supervise())

        waiter = asyncio.ensure_future(first_attempt.wait())
        try:
            await asyncio.wait(
                {waiter, task}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            waiter.cancel()
        if self._supervisor_task is task and self._connect_ok:
            _logger.debug("Started event listener")
            return True
        return False

    async def _supervise(self) -> None:
        """Keep the push subscription alive until the listener is stopped.

        Connecting and refreshing each keep their own schedule and backoff, so
        a failure in one never stops the connection from being watched.
        """
        sched = _Schedule(
            connect_delay=self.RETRY_MIN_DELAY,
            refresh_delay=self.RETRY_MIN_DELAY,
            next_refresh=time.monotonic() + self.SESSION_REFRESH_INTERVAL,
        )
        while self.started:
            problem = self._receiver_problem()
            if problem is None:
                await self._when_healthy(sched)
            else:
                await self._when_unhealthy(sched, problem)
            self._update_connected()
            if self._first_attempt_done:
                self._first_attempt_done.set()
            if self.connected:
                wait = min(
                    self.WATCHDOG_INTERVAL,
                    max(0, sched.next_refresh - time.monotonic()),
                )
            else:
                wait = self.CONNECTING_POLL_INTERVAL
            await asyncio.sleep(wait)

    async def _when_healthy(self, sched: _Schedule) -> None:
        sched.down_since = sched.dead_since = None
        self._seen_started = True
        self._stuck_count = 0
        if time.monotonic() < sched.next_refresh:
            return
        try:
            await self._refresh_session()
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001
            wait = self._error_wait(ex, sched.refresh_delay)
            self._log_error(ex, wait)
            sched.next_refresh = time.monotonic() + wait
            sched.refresh_delay = min(sched.refresh_delay * 2, self.RETRY_MAX_DELAY)
        else:
            sched.refresh_delay = self.RETRY_MIN_DELAY
            sched.next_refresh = time.monotonic() + self.SESSION_REFRESH_INTERVAL

    async def _when_unhealthy(self, sched: _Schedule, problem: str) -> None:
        now = time.monotonic()
        if self._receiver is None:
            due = 0.0
        elif problem == "dead":
            sched.dead_since = sched.dead_since or now
            due = sched.dead_since + self._reconnect_delay()
        else:
            # firebase-messaging is still retrying; give it time first
            sched.down_since = sched.down_since or now
            due = sched.down_since + self.RECEIVER_DOWN_GRACE
        if now < max(due, sched.next_connect):
            return
        try:
            if self._receiver is None:
                await self._full_connect()
                sched.next_refresh = time.monotonic() + self.SESSION_REFRESH_INTERVAL
            else:
                _logger.warning(
                    "Push connection %s, reconnecting",
                    "lost" if problem == "dead" else "stuck",
                )
                if not self._seen_started:
                    self._stuck_count += 1
                await self._reconnect()
        except asyncio.CancelledError:
            raise
        except Exception as ex:  # noqa: BLE001
            wait = self._error_wait(ex, sched.connect_delay)
            self._log_error(ex, wait)
            sched.next_connect = time.monotonic() + wait
            sched.connect_delay = min(sched.connect_delay * 2, self.RETRY_MAX_DELAY)
        else:
            sched.connect_delay = self.RETRY_MIN_DELAY
            sched.down_since = sched.dead_since = None

    def _reconnect_delay(self) -> float:
        """Return how long to wait before replacing a dead push client.

        Immediately after a working connection drops; otherwise backing off, as
        each takeover that never connects means the network is still down.
        """
        if self._seen_started:
            return 0
        return min(
            self.RECONNECT_MIN_DELAY * 2**self._stuck_count, self.RECONNECT_MAX_DELAY
        )

    def _error_wait(self, ex: Exception, delay: float) -> float:
        # A revoked login does not fix itself quickly; do not hammer the API
        return self.RETRY_MAX_DELAY if isinstance(ex, AuthenticationError) else delay

    def _log_error(self, ex: Exception, wait: float) -> None:
        if isinstance(ex, self._EXPECTED_ERRORS):
            _logger.warning(
                "Ring push listener error, retrying in %s seconds: %s", wait, ex
            )
        elif type(ex) not in self._unexpected_logged:
            # Full traceback once per error type, then a plain line
            self._unexpected_logged.add(type(ex))
            _logger.exception(
                "Unexpected ring push listener error, retrying in %s seconds", wait
            )
        else:
            _logger.warning(
                "Unexpected ring push listener error, retrying in %s seconds: %r",
                wait,
                ex,
            )

    def _receiver_problem(self) -> str | None:
        """Return "dead", "down" or None for a healthy push client.

        firebase-messaging leaves the client STOPPING after it gives up
        reconnecting, returns from its listen task without stopping when the
        first connection fails, and stops heartbeating for good if its monitor
        task errors, so its run state alone cannot be trusted.
        """
        receiver = self._receiver
        if receiver is None or receiver.run_state in (
            FcmPushClientRunState.STOPPING,
            FcmPushClientRunState.STOPPED,
        ):
            return "dead"
        if any(task.done() for task in receiver.tasks):
            return "dead"
        if not receiver.is_started():
            return "down"
        last = receiver.last_message_time
        if last is not None and time.time() - last > self.RECEIVER_STALE_AFTER:
            return "dead"
        return None

    def _new_receiver(self) -> FcmPushClient:
        return FcmPushClient(
            self._on_notification,
            FcmRegisterConfig(
                FCM_PROJECT_ID, FCM_APP_ID, FCM_API_KEY, FCM_RING_SENDER_ID
            ),
            self._credentials,
            self._credentials_updated_cb,
            config=self._config,
            http_client_session=self._ring.auth._session,  # noqa: SLF001
        )

    async def _stop_receiver(self) -> None:
        receiver = self._receiver
        self._receiver = None
        # FcmPushClient.stop() fails if start() was never called on it
        if receiver and receiver.run_state != FcmPushClientRunState.CREATED:
            try:
                await receiver.stop()
            except Exception:  # noqa: BLE001
                _logger.debug("Error stopping push client", exc_info=True)

    async def _full_connect(self) -> None:
        """Check in to FCM, subscribe the token with ring and start the client."""
        self._connect_ok = False
        self._seen_started = False
        await self._stop_receiver()
        receiver = self._new_receiver()
        try:
            token = await receiver.checkin_or_register()
        except (KeyError, TypeError, ValueError) as ex:
            if not self._credentials:
                raise
            # Corrupt stored credentials never recover; register afresh now
            _logger.warning(
                "Stored push credentials are unusable (%r), registering afresh", ex
            )
            self._credentials = None
            receiver = self._new_receiver()
            token = await receiver.checkin_or_register()
        if not token:
            msg = "Ring listener unable to check in to fcm"
            raise RingError(msg)
        self.fcm_token = token

        # Check-in may have registered a new token, so always re-subscribe; only
        # replace the session when it is old enough to have been dropped.
        refreshed = self._ring.session_refresh_time
        if (
            refreshed is None
            or time.monotonic() - refreshed > self.SESSION_REFRESH_INTERVAL
        ):
            await self._ring.async_create_session()
        await self.add_subscription_to_ring(token)
        if not self.subscribed:
            msg = "Unable to subscribe to ring push notifications"
            raise RingError(msg)

        self._receiver = receiver
        await receiver.start()
        self._connect_ok = True

    async def _reconnect(self) -> None:
        """Replace a dead push client, reusing the registered credentials."""
        self._seen_started = False
        if (
            not self._credentials
            or not self.subscribed
            or self._stuck_count == self.FULL_RECONNECT_AFTER
        ):
            await self._full_connect()
            return
        await self._stop_receiver()
        receiver = self._new_receiver()
        self._receiver = receiver
        await receiver.start()

    async def _refresh_session(self) -> None:
        if TYPE_CHECKING:
            assert self.fcm_token
        _logger.debug("Refreshing ring session")
        await self._ring.async_create_session()
        was_subscribed = self.subscribed
        await self.add_subscription_to_ring(self.fcm_token)
        if not self.subscribed:
            # The existing subscription most likely still works
            self.subscribed = was_subscribed
            msg = "Unable to re-subscribe to ring push notifications"
            raise RingError(msg)

    def _get_ding_event(self, gcm_data: dict[str, Any]) -> RingEvent:
        ding = gcm_data["ding"]
        action = gcm_data["action"]
        subtype = gcm_data["subtype"]
        if action.lower() == PUSH_ACTION_MOTION.lower():
            kind = KIND_MOTION
            state = subtype if subtype in MOTION_SUBTYPES else KIND_MOTION_OTHER
        elif action.lower() == PUSH_ACTION_DING.lower():
            kind = KIND_DING
            state = "ringing"
        else:
            kind = action
            state = subtype

        created_at = ding["created_at"]
        create_seconds = parse_datetime(created_at).timestamp()
        return RingEvent(
            id=str(ding["id"]),
            kind=kind,
            doorbot_id=ding["doorbot_id"],
            device_name=ding["device_name"],
            device_kind=ding["device_kind"],
            now=create_seconds,
            expires_in=DEFAULT_LISTEN_EVENT_EXPIRES_IN,
            state=state,
        )

    def _get_intercom_unlock_event(self, gcm_data: dict[str, Any]) -> RingEvent | None:
        device_api_id = gcm_data["alarm_meta"]["device_zid"]
        if (device := self._ring.get_device_by_api_id(device_api_id)) is None:
            _logger.debug("Event received for unknown device id: %s", device_api_id)
            return None

        if device_api_id not in self._intercom_unlock_counter:
            self._intercom_unlock_counter[device_api_id] = 0
        self._intercom_unlock_counter[device_api_id] += 1
        return RingEvent(
            id=str(self._intercom_unlock_counter[device_api_id]),
            kind=KIND_INTERCOM_UNLOCK,
            doorbot_id=device_api_id,
            device_name=device.name,
            device_kind=device.kind,
            now=time.time(),
            expires_in=DEFAULT_LISTEN_EVENT_EXPIRES_IN,
            state="unlock",
        )

    def _check_is_update(self, ring_event: RingEvent) -> None:
        """Battery doorbells send two events.

        First without an image and the second with an image.
        """
        now = time.time()
        seen_events = {
            key
            for key in self._seen_events
            if (now - key.now) < DEFAULT_LISTEN_EVENT_EXPIRES_IN
        }
        event_key = ring_event.get_key()
        if event_key in seen_events:
            ring_event.is_update = True
        else:
            seen_events.add(event_key)
        self._seen_events = seen_events

    def _on_notification(
        self,
        notification: dict[str, dict[str, str]],
        persistent_id: str,  # noqa: ARG002
        obj: Any | None = None,  # noqa: ARG002
    ) -> None:
        msg_data = notification["data"]
        if "gcmData" in msg_data:
            gcm_data = json.loads(notification["data"]["gcmData"])
            ring_event = self._get_legacy_ring_event(gcm_data)
        else:
            ring_event = self._get_ring_event(msg_data)

        if ring_event:
            self._check_is_update(ring_event)
            _logger.debug("Event received %s", ring_event)
            for callback in self._callbacks.values():
                callback(ring_event)
        else:
            _logger.debug("Unknown event received %s", msg_data)

    def _get_ring_event(self, msg_data: dict) -> RingEvent | None:
        if (android_config_str := msg_data.get("android_config")) is None or (
            data_str := msg_data.get("data")
        ) is None:
            _logger.debug(
                "Unexpected alert type in fcm message data.  Full message is:\n%s",
                json.dumps(msg_data),
            )
            return None

        android_config = json.loads(android_config_str)
        _logger.debug("Event data: %s", data_str)
        data = json.loads(data_str)
        event_category = android_config["category"]
        event_kind = PUSH_NOTIFICATION_KINDS.get(event_category, "Unknown")
        device = data["device"]
        event = data["event"]
        event_id = str(event["ding"].get("id") or event["riid"])

        subtype = event["ding"]["subtype"]
        if event_kind == KIND_MOTION:
            subtype = subtype if subtype in MOTION_SUBTYPES else KIND_MOTION_OTHER

        created_at = event["ding"]["created_at"]
        create_seconds = parse_datetime(created_at).timestamp()
        return RingEvent(
            event_id,
            device["id"],
            device_name=device.get("name"),
            device_kind=device.get("kind"),
            kind=event_kind,
            now=create_seconds,
            expires_in=DEFAULT_LISTEN_EVENT_EXPIRES_IN,
            state=subtype,
        )

    def _get_legacy_ring_event(self, gcm_data: dict) -> RingEvent | None:
        re: RingEvent | None = None
        if "ding" in gcm_data:
            re = self._get_ding_event(gcm_data)
        elif gcm_data.get("action") == PUSH_ACTION_INTERCOM_UNLOCK:
            re = self._get_intercom_unlock_event(gcm_data)
        elif "community_alert" not in gcm_data:
            _logger.debug(
                "Unexpected alert type in gcmData.  Full message is:\n%s",
                json.dumps(gcm_data),
            )
            return None
        return re
