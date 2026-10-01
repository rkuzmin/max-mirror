"""The only module that imports PyMax. Swapping the library = rewriting this file.

Hands the rest of the service plain protocol-shaped dicts (camelCase JSON, as
the server sends them). Read-only: nothing here sends messages, read marks or
typing; PyMax telemetry is off (MSG-5).
"""

import asyncio
import logging
import os
import time

from pymax import ApiError, ConsoleQrHandler, ExtraConfig, WebClient
from pymax.api.messages.payloads import ChatHistoryPayload
from pymax.protocol import Command, Opcode

SESSION_FILE = "session.db"
REVOKED_ERRORS = ("FAIL_LOGIN_TOKEN", "FAIL_LOGOUT_ALL")


class SessionRevoked(Exception):
    pass


class _QrPrinter(ConsoleQrHandler):
    async def show_qr(self, qr_url: str) -> None:
        logging.getLogger("max_mirror").warning(
            "qr_login_required", extra={"fields": {"hint": "MAX app -> Settings -> Devices -> scan QR below"}})
        await super().show_qr(qr_url)


class _HookedClient(WebClient):
    """Taps every server-pushed frame before PyMax parses it.

    PyMax's own on_raw fires after its typed parsing, so a payload PyMax fails
    to validate would never reach on_raw; tapping the connection keeps STORE-2.
    """

    sink = None

    def _build_app(self):
        app = super()._build_app()
        parse = app.connection.on_event

        async def on_event(frame):
            if frame.cmd == Command.REQUEST and self.sink:
                self.sink(frame.opcode, frame.payload or {})
            await parse(frame)

        app.connection.on_event = on_event
        return app


class MaxAdapter:
    def __init__(self, work_dir: str, requests_per_second: float = 1.0):
        self.on_event = lambda opcode, payload: None
        self.session_path = os.path.join(work_dir, SESSION_FILE)
        self._interval = 1 / requests_per_second
        self._last_request = 0.0
        self.client = _HookedClient(
            session_name=SESSION_FILE,
            work_dir=work_dir,
            # reconnect/relogin are ours: PyMax retries at a fixed 1 s and, with a
            # revoked token, would loop logins forever (AUTH-3)
            extra_config=ExtraConfig(reconnect=False, relogin=False, telemetry=False,
                                     request_timeout=15, log_level="WARNING"),
            qr_provider=_QrPrinter(),
        )
        self.client.sink = lambda opcode, payload: self.on_event(opcode, payload)

    async def connect(self) -> str:
        """Log in (QR on first run, AUTH-1) and return the account id."""
        try:
            await self.client.connect()
        except ApiError as e:
            if e.error in REVOKED_ERRORS or e.message in REVOKED_ERRORS:
                raise SessionRevoked(e.error) from e
            raise
        if not self.client.is_connected or not self.client.me:
            raise ConnectionError("login did not complete")
        return str(self.client.me.contact.id)

    async def wait_closed(self):
        while self.client.is_connected:
            await asyncio.sleep(1)
        await self.client.close()

    async def close(self):
        await self.client.close()

    def forget_session(self):
        """Drop a revoked session so the next start shows a fresh QR."""
        if os.path.exists(self.session_path):
            os.remove(self.session_path)

    async def _throttle(self):
        # HIST-3: one limiter for every read request, retries included
        wait = self._last_request + self._interval - time.monotonic()
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_request = time.monotonic()

    async def history(self, chat_id: str, from_ms: int, backward: int = 0, forward: int = 0) -> dict:
        """Raw CHAT_HISTORY response payload: {"messages": [...]}."""
        await self._throttle()
        payload = ChatHistoryPayload(chat_id=int(chat_id), forward=forward, backward=backward, from_=from_ms)
        frame = await self.client._app.invoke(Opcode.CHAT_HISTORY, payload.to_payload())
        return frame.payload or {}

    async def all_chats(self) -> list[dict]:
        """Chats from login plus older pages (login returns only the first 40)."""
        chats = {c.id: c for c in self.client.chats or []}
        marker = min((c.last_event_time for c in chats.values()), default=None)
        while marker:
            await self._throttle()
            page = [c for c in await self.client.fetch_chats(marker) if c.id not in chats]
            if not page:
                break
            chats.update((c.id, c) for c in page)
            marker = min(c.last_event_time for c in page)
        return [_dump(c) for c in chats.values()]

    async def chats_by_id(self, chat_ids: list[str]) -> list[dict]:
        await self._throttle()
        return [_dump(c) for c in await self.client.get_chats([int(i) for i in chat_ids])]

    def contacts(self) -> list[dict]:
        return [_dump(u) for u in self.client.contacts if u]

    async def users(self, user_ids: list[str]) -> list[dict]:
        await self._throttle()
        return [_dump(u) for u in await self.client.fetch_users([int(i) for i in user_ids])]


def _dump(model) -> dict:
    # embedded messages may carry raw bytes previews; chats/users don't need them
    return model.model_dump(by_alias=True, mode="json", exclude={"last_message", "pinned_message"})
