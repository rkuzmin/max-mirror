"""End-to-end check without MAX: fake adapter -> normalizer -> SQLite -> signed webhook.

    uv run python test_pipeline.py
"""

import asyncio
import json
import tempfile

from aiohttp import web

from max_mirror import webhook
from max_mirror.adapter import SessionRevoked
from max_mirror.service import Config, Service
from max_mirror.store import now_ms

SECRET = "s3cret"
NOW = now_ms()
HOUR = 3_600_000


def msg(mid, t, text="hi", **kw):
    return {"id": str(mid), "time": t, "sender": 2, "text": text, "type": "USER", "attaches": [], **kw}


class FakeAdapter:
    def __init__(self):
        self.on_event = None
        self.calls = []
        self.fail_after = None
        self.revoked = False
        self.hist = {
            "10": [msg(1000 + i, NOW - i * HOUR, f"old {i}") for i in range(100)] + [msg(999, NOW - 40 * 24 * HOUR)],
            "-20": [msg(2000 + i, NOW - i * HOUR) for i in range(120)],
            "666": [msg(6000, NOW - HOUR, "secret")],
        }

    async def connect(self):
        if self.revoked:
            raise SessionRevoked("FAIL_LOGIN_TOKEN")
        return "1"

    async def all_chats(self):
        return [
            {"id": 10, "type": "DIALOG", "participants": {"1": 0, "2": 0}},
            {"id": -20, "type": "CHAT", "title": "Группа", "participants": {}},
            {"id": 666, "type": "DIALOG", "participants": {"1": 0, "3": 0}},
        ]

    def contacts(self):
        return [{"id": 2, "names": [{"firstName": "Иван", "lastName": "Петров"}]}]

    async def users(self, ids):
        return []

    async def chats_by_id(self, ids):
        return []

    async def history(self, chat_id, from_ms, backward=0, forward=0):
        self.calls.append((chat_id, from_ms, backward, forward))
        if self.fail_after is not None and len(self.calls) > self.fail_after:
            raise ConnectionError("network down")
        msgs = sorted(self.hist[chat_id], key=lambda m: m["time"])
        if backward:
            return {"messages": [m for m in msgs if m["time"] <= from_ms][-backward:]}
        return {"messages": [m for m in msgs if m["time"] >= from_ms][:forward]}

    def forget_session(self):
        pass

    async def close(self):
        pass


async def receiver():
    got, alerts, fails = [], [], [2]  # first two webhook calls answer 500

    async def hook(request):
        body = await request.read()
        assert request.headers["X-Signature"] == webhook.sign(SECRET, body), "bad signature"
        if fails[0]:
            fails[0] -= 1
            return web.Response(status=500)
        got.append(json.loads(body))
        return web.Response(status=204)

    async def alert(request):
        alerts.append(await request.json())
        return web.Response()

    app = web.Application()
    app.router.add_post("/hook", hook)
    app.router.add_post("/alert", alert)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return f"http://127.0.0.1:{port}", got, alerts


async def main():
    base, got, alerts = await receiver()
    cfg = Config(data_dir=tempfile.mkdtemp(), webhook_url=base + "/hook", webhook_secret=SECRET,
                 alert_url=base + "/alert", chats_deny={"666"}, history_rps=1000)
    fake = FakeAdapter()
    svc = Service(cfg, adapter=fake)
    db, count = svc.store.db, lambda sql, *a: svc.store.one(sql, *a)[0]
    svc.me = await fake.connect()

    # HIST-1: 30-day backfill, quiet (no webhook), filtered chat untouched
    await svc.sync_history()
    assert count("SELECT COUNT(*) FROM messages WHERE chat_id='10' AND sent_at>?", NOW - 30 * 24 * HOUR) == 100
    assert count("SELECT COUNT(*) FROM messages WHERE chat_id='666'") == 0
    assert count("SELECT COUNT(*) FROM outbox") == 0
    assert svc.store.chat_title("10") == "Иван Петров"

    # STORE-1: a second backfill adds nothing
    db.execute("UPDATE chats SET backfill_done=0, backfill_cursor=NULL")
    rows = count("SELECT COUNT(*) FROM messages")
    await svc.sync_history()
    assert count("SELECT COUNT(*) FROM messages") == rows

    # MSG-1/2: live events of every type
    live = {
        "text": msg(1, NOW + 1, "Добрый день"),
        "photo": msg(2, NOW + 2, "", attaches=[{"_type": "PHOTO", "baseUrl": "https://x/p.jpg"}]),
        "video": msg(3, NOW + 3, "", attaches=[{"_type": "VIDEO", "videoId": 1}]),
        "file": msg(4, NOW + 4, "", attaches=[{"_type": "FILE", "name": "a.pdf"}]),
        "voice": msg(5, NOW + 5, "", attaches=[{"_type": "AUDIO", "duration": 3}]),
        "sticker": msg(6, NOW + 6, "", attaches=[{"_type": "STICKER", "stickerId": 7}]),
        "forwarded": msg(7, NOW + 7, "", link={"type": "FORWARD", "chatId": -5, "message": msg(70, NOW, "fwd")}),
        "reply": msg(8, NOW + 8, "ответ", link={"type": "REPLY", "chatId": 10, "message": msg(1, NOW + 1)}),
        "unknown": msg(9, NOW + 9, "", attaches=[{"_type": "CONTROL", "event": "joined"}]),
    }
    for m in live.values():
        svc.on_event(128, {"chatId": 10, "message": m})
    svc.on_event(128, {"chatId": 10, "message": live["text"]})  # duplicate
    for kind, m in live.items():
        assert count("SELECT type FROM messages WHERE chat_id='10' AND message_id=?", m["id"]) == kind
    assert count("SELECT text FROM messages WHERE message_id='7'") == "fwd"
    assert count("SELECT reply_to_id FROM messages WHERE message_id='8'") == "1"

    # MSG-3: edit and delete; the original text stays in raw_events
    svc.on_event(128, {"chatId": 10, "message": msg(1, NOW + 1, "Добрый вечер", status="EDITED")})
    svc.on_event(142, {"chat": {"id": 10}, "messageIds": ["2"]})
    assert count("SELECT text FROM messages WHERE message_id='1'") == "Добрый вечер"
    assert count("SELECT edited_at IS NOT NULL FROM messages WHERE message_id='1'") == 1
    assert count("SELECT deleted_at IS NOT NULL FROM messages WHERE message_id='2'") == 1
    assert count("SELECT COUNT(*) FROM raw_events WHERE payload LIKE '%Добрый день%'") >= 1

    # MSG-4: filtered chat leaves no trace; STORE-2: broken payload is kept and flagged
    svc.on_event(128, {"chatId": 666, "message": msg(6001, NOW, "secret")})
    assert count("SELECT COUNT(*) FROM raw_events WHERE payload LIKE '%secret%'") == 0
    svc.on_event(128, {"chatId": 10, "message": {"id": "x"}})
    assert count("SELECT COUNT(*) FROM raw_events WHERE parse_error IS NOT NULL") == 1

    # HIST-2: 50 messages sent while we were down reach the DB and the webhook
    fake.hist["10"] += [msg(3000 + i, NOW + 1000 + i) for i in range(50)]
    await svc.gap_fill(svc.store.one("SELECT * FROM chats WHERE chat_id='10'"))
    assert count("SELECT COUNT(*) FROM messages WHERE message_id LIKE '30%'") == 50

    # HIST-3: crash mid-backfill, then resume from the cursor without restarting from now
    db.execute("UPDATE chats SET backfill_done=0, backfill_cursor=NULL WHERE chat_id='-20'")
    db.execute("DELETE FROM messages WHERE chat_id='-20'")
    fake.calls, fake.fail_after = [], 2
    try:
        await svc.backfill(svc.store.one("SELECT * FROM chats WHERE chat_id='-20'"))
        raise AssertionError("expected a crash")
    except ConnectionError:
        pass
    fake.calls, fake.fail_after = [], None
    await svc.backfill(svc.store.one("SELECT * FROM chats WHERE chat_id='-20'"))
    assert fake.calls[0][1] < NOW, "resumed from now instead of the saved cursor"
    assert count("SELECT COUNT(*) FROM messages WHERE chat_id='-20'") == 120

    # OUT-1..3: everything delivered in order despite two 500s
    expected = [(r["event_type"], r["message_id"]) for r in db.execute("SELECT * FROM outbox ORDER BY id")]
    sender = asyncio.create_task(webhook.deliver_forever(svc.store, cfg.webhook_url, SECRET, svc.metrics))
    for _ in range(100):
        if svc.store.outbox_size() == 0:
            break
        await asyncio.sleep(0.1)
    sender.cancel()
    assert [(e["event"], e["message"]["id"]) for e in got] == expected
    assert {e["event"] for e in got} == {"message.created", "message.edited", "message.deleted"}
    assert got[0]["chat"] == {"id": "10", "type": "dialog", "title": "Иван Петров"}
    assert got[0]["message"]["sender"]["name"] == "Иван Петров"
    assert webhook.sign(SECRET, b'{"tampered":1}') != webhook.sign(SECRET, b'{"tampered":2}')

    # OUT-4: export one chat for a period
    assert len(list(svc.store.export("10", NOW - 30 * 24 * HOUR, NOW + 10 * HOUR))) == 100 + 9 + 50

    # AUTH-3: revoked session -> one alert, no retries
    fake.revoked = True
    await asyncio.wait_for(svc.connection_loop(), 5)
    assert svc.state == "session_revoked" and len(alerts) == 1
    assert svc.health()["status"] == "session_revoked"
    print("ok:", len(got), "webhook events,", count("SELECT COUNT(*) FROM messages"), "messages")


asyncio.run(main())
