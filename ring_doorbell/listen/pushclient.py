"""firebase-messaging push client with fixes for Ring's pushes."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable

from firebase_messaging import FcmPushClient

if TYPE_CHECKING:
    from firebase_messaging.proto.mcs_pb2 import (  # type: ignore[import-untyped]
        DataMessageStanza,
    )

_logger = logging.getLogger(__name__)


# The header parameter firebase-messaging strips and decodes, by header
_HEADER_PARAMS = {"crypto-key": "dh", "encryption": "salt"}


def _header_param(value: str, name: str) -> str | None:
    """Return the ``name=`` parameter of a header, e.g. dh of Crypto-Key."""
    for part in value.replace(",", ";").split(";"):
        key, _, param = part.strip().partition("=")
        if key.lower() == name:
            return param.strip().strip('"')
    return None


def _pad_base64(value: str) -> str:
    """Return base64 with the padding that urlsafe_b64decode requires."""
    return value + "=" * (-len(value) % 4)


class RingFcmPushClient(FcmPushClient):
    """Push client that survives pushes it cannot decrypt.

    firebase-messaging (up to at least 0.4.5) takes everything after "dh=" in
    a push's Crypto-Key header as the key, and decodes it and the salt without
    padding. Some of Ring's pushes carry a second key in that header
    ("dh=...;p256ecdsa=..."), so they could never be decrypted, and as any
    error while handling a push shuts the whole client down without
    acknowledging the push, FCM delivered it again after every reconnect,
    together with every other push not yet acknowledged: the client crashed on
    each reconnect and replayed old pushes as if they were new.
    """

    def __init__(
        self,
        *args: Any,
        on_terminate: Callable[[], None] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialise; ``on_terminate`` is called when the client shuts down."""
        super().__init__(*args, **kwargs)
        self._on_terminate = on_terminate

    def _app_data_by_key(
        self,
        p: DataMessageStanza,
        key: str,
        do_not_raise: bool = False,  # noqa: FBT001, FBT002
    ) -> str:
        """Return a push's app data; just the dh/salt of the crypto headers."""
        value = super()._app_data_by_key(p, key, do_not_raise)
        if (name := _HEADER_PARAMS.get(key)) and (
            param := _header_param(value, name)
        ) is not None:
            return f"{name}={param}"
        return value

    @staticmethod
    def _decrypt_raw_data(
        credentials: dict[str, dict[str, str]],
        crypto_key_str: str,
        salt_str: str,
        raw_data: bytes,
    ) -> bytes:
        return FcmPushClient._decrypt_raw_data(  # noqa: SLF001
            credentials, _pad_base64(crypto_key_str), _pad_base64(salt_str), raw_data
        )

    def _handle_data_message(self, msg: DataMessageStanza) -> None:
        """Handle a push; one that cannot be read is skipped, not fatal.

        The caller acknowledges the push afterwards either way, so FCM does
        not deliver it again.
        """
        try:
            super()._handle_data_message(msg)
        except Exception:  # noqa: BLE001
            _logger.warning(
                "Skipping push %s that could not be read",
                msg.persistent_id,
                exc_info=True,
            )

    def _terminate(self) -> None:
        """Shut down after an unrecoverable error, and say so at once."""
        super()._terminate()
        if self._on_terminate:
            try:
                self._on_terminate()
            except Exception:
                _logger.exception("Error in push client terminate callback")
