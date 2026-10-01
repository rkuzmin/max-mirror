"""SQLite storage. Times are Unix milliseconds; ids are strings."""

import json
import sqlite3
import time
from datetime import datetime, timezone

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS chats(
    chat_id TEXT PRIMARY KEY, type TEXT, title TEXT, peer_id TEXT, last_event_at INTEGER,
    last_message_id TEXT, backfill_cursor INTEGER, backfill_done INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS users(user_id TEXT PRIMARY KEY, display_name TEXT);
CREATE TABLE IF NOT EXISTS messages(
    chat_id TEXT NOT NULL, message_id TEXT NOT NULL, sender_id TEXT, sent_at INTEGER, type TEXT,
    text TEXT, reply_to_id TEXT, forwarded_from TEXT, attachments TEXT, edited_at INTEGER,
    deleted_at INTEGER, received_at INTEGER, PRIMARY KEY(chat_id, message_id));
CREATE INDEX IF NOT EXISTS messages_chat_time ON messages(chat_id, sent_at);
CREATE TABLE IF NOT EXISTS raw_events(
    id INTEGER PRIMARY KEY, received_at INTEGER, opcode INTEGER, payload TEXT, parse_error TEXT);
CREATE TABLE IF NOT EXISTS outbox(
    id INTEGER PRIMARY KEY, event_type TEXT, chat_id TEXT, message_id TEXT, payload TEXT,
    attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER, delivered_at INTEGER, failed_at INTEGER);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(chat_id, id) WHERE delivered_at IS NULL AND failed_at IS NULL;
"""


def now_ms() -> int:
    return int(time.time() * 1000)


def iso(ms):
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class Store:
    def __init__(self, path: str, outbox: bool = True):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self.outbox_enabled = outbox

    def one(self, sql, *args):
        return self.db.execute(sql, args).fetchone()

    # --- raw events (STORE-2) ---

    def raw(self, opcode: int, payload) -> int:
        with self.db:
            cur = self.db.execute(
                "INSERT INTO raw_events(received_at, opcode, payload) VALUES (?,?,?)",
                (now_ms(), opcode, json.dumps(payload, ensure_ascii=False, default=str)),
            )
        return cur.lastrowid

    def raw_error(self, raw_id: int, error: str):
        with self.db:
            self.db.execute("UPDATE raw_events SET parse_error=? WHERE id=?", (error, raw_id))

    def drop_raw(self, raw_id: int):
        with self.db:
            self.db.execute("DELETE FROM raw_events WHERE id=?", (raw_id,))

    # --- messages (STORE-1, MSG-3) ---

    def save_message(self, m: dict, notify: bool) -> str | None:
        """Insert or apply an edit; returns the event type written, or None for a duplicate."""
        now = now_ms()
        edited_at = (m.get("updated_at") or now) if m["status"] == "EDITED" else None
        attachments = json.dumps(m["attachments"], ensure_ascii=False, default=str)
        with self.db:
            old = self.one("SELECT text, attachments FROM messages WHERE chat_id=? AND message_id=?",
                           m["chat_id"], m["message_id"])
            if old is None:
                event = "message.created"
                self.db.execute(
                    "INSERT INTO messages VALUES (?,?,?,?,?,?,?,?,?,?,NULL,?)",
                    (m["chat_id"], m["message_id"], m["sender_id"], m["sent_at"], m["type"], m["text"],
                     m["reply_to_id"], m["forwarded_from"], attachments, edited_at, now),
                )
                self.db.execute(
                    "INSERT INTO chats(chat_id, last_message_id) VALUES (?,?) ON CONFLICT(chat_id) DO UPDATE "
                    "SET last_message_id=excluded.last_message_id WHERE chats.last_message_id IS NULL OR ? >= "
                    "(SELECT sent_at FROM messages WHERE chat_id=chats.chat_id AND message_id=chats.last_message_id)",
                    (m["chat_id"], m["message_id"], m["sent_at"]),
                )
            elif edited_at and (old["text"], old["attachments"]) != (m["text"], attachments):
                event = "message.edited"
                self.db.execute(
                    "UPDATE messages SET text=?, attachments=?, type=?, edited_at=? WHERE chat_id=? AND message_id=?",
                    (m["text"], attachments, m["type"], edited_at, m["chat_id"], m["message_id"]),
                )
            else:
                return None
            if notify:
                self._enqueue(event, m["chat_id"], m["message_id"])
        return event

    def delete_messages(self, chat_id: str, ids: list[str]) -> int:
        n = 0
        with self.db:
            for mid in ids:
                cur = self.db.execute(
                    "UPDATE messages SET deleted_at=? WHERE chat_id=? AND message_id=? AND deleted_at IS NULL",
                    (now_ms(), chat_id, mid),
                )
                if cur.rowcount:
                    n += 1
                    self._enqueue("message.deleted", chat_id, mid)
        return n

    def last_sent_at(self, chat_id: str):
        return self.one("SELECT MAX(sent_at) FROM messages WHERE chat_id=?", chat_id)[0]

    # --- chats and users ---

    def upsert_chat(self, c: dict):
        with self.db:
            self.db.execute(
                "INSERT INTO chats(chat_id, type, title, peer_id, last_event_at) VALUES (?,?,?,?,?) "
                "ON CONFLICT(chat_id) DO UPDATE SET type=excluded.type, title=excluded.title, "
                "peer_id=COALESCE(excluded.peer_id, peer_id), last_event_at=COALESCE(excluded.last_event_at, last_event_at)",
                (c["chat_id"], c["type"], c["title"], c["peer_id"], c["last_event_at"]),
            )

    def upsert_users(self, users: list[dict]):
        with self.db:
            self.db.executemany(
                "INSERT INTO users VALUES (:user_id, :display_name) ON CONFLICT(user_id) "
                "DO UPDATE SET display_name=COALESCE(excluded.display_name, display_name)",
                users,
            )

    def chats(self):
        return self.db.execute("SELECT * FROM chats ORDER BY last_event_at DESC").fetchall()

    def chat_type(self, chat_id: str):
        row = self.one("SELECT type FROM chats WHERE chat_id=?", chat_id)
        return row and row["type"]

    def set_backfill(self, chat_id: str, cursor: int, done: bool):
        with self.db:
            self.db.execute("UPDATE chats SET backfill_cursor=?, backfill_done=? WHERE chat_id=?",
                            (cursor, int(done), chat_id))

    def unknown_user_ids(self, limit=100) -> list[str]:
        rows = self.db.execute(
            "SELECT id FROM (SELECT sender_id id FROM messages UNION SELECT peer_id FROM chats) "
            "WHERE id IS NOT NULL AND id NOT IN (SELECT user_id FROM users) LIMIT ?", (limit,))
        return [r[0] for r in rows]

    def unknown_chat_ids(self, limit=100) -> list[str]:
        return [r[0] for r in self.db.execute("SELECT chat_id FROM chats WHERE type IS NULL LIMIT ?", (limit,))]

    # --- outbox (OUT-1, OUT-2) ---

    def _enqueue(self, event: str, chat_id: str, message_id: str):
        if not self.outbox_enabled:
            return
        payload = json.dumps(self.event_payload(event, chat_id, message_id), ensure_ascii=False)
        self.db.execute(
            "INSERT INTO outbox(event_type, chat_id, message_id, payload, created_at) VALUES (?,?,?,?,?)",
            (event, chat_id, message_id, payload, now_ms()),
        )

    def chat_title(self, chat_id: str):
        row = self.one("SELECT c.title, u.display_name FROM chats c LEFT JOIN users u ON u.user_id=c.peer_id "
                       "WHERE c.chat_id=?", chat_id)
        return row and (row["title"] or row["display_name"])

    def user_name(self, user_id):
        row = self.one("SELECT display_name FROM users WHERE user_id=?", user_id)
        return row and row["display_name"]

    def message_json(self, row) -> dict:
        return {
            "id": row["message_id"],
            "sender": {"id": row["sender_id"], "name": self.user_name(row["sender_id"])},
            "sent_at": iso(row["sent_at"]),
            "type": row["type"],
            "text": row["text"],
            "reply_to_id": row["reply_to_id"],
            "forwarded_from": row["forwarded_from"],
            "attachments": json.loads(row["attachments"] or "[]"),
            "edited_at": iso(row["edited_at"]),
            "deleted_at": iso(row["deleted_at"]),
        }

    def event_payload(self, event: str, chat_id: str, message_id: str) -> dict:
        row = self.one("SELECT * FROM messages WHERE chat_id=? AND message_id=?", chat_id, message_id)
        return {
            "event": event,
            "chat": {"id": chat_id, "type": self.chat_type(chat_id), "title": self.chat_title(chat_id)},
            "message": self.message_json(row),
        }

    def enrich(self, p: dict) -> dict:
        """Fill names resolved after the event was queued."""
        p["chat"]["type"] = p["chat"]["type"] or self.chat_type(p["chat"]["id"])
        p["chat"]["title"] = p["chat"]["title"] or self.chat_title(p["chat"]["id"])
        sender = p["message"]["sender"]
        sender["name"] = sender["name"] or self.user_name(sender["id"])
        return p

    def due_outbox(self, now: int, limit=50):
        """Head of each chat's queue, if due: a failing head blocks its chat, keeping order."""
        return self.db.execute(
            "SELECT o.* FROM outbox o JOIN (SELECT MIN(id) id FROM outbox WHERE delivered_at IS NULL "
            "AND failed_at IS NULL GROUP BY chat_id) h ON o.id=h.id WHERE o.next_attempt_at<=? ORDER BY o.id LIMIT ?",
            (now, limit),
        ).fetchall()

    def outbox_done(self, outbox_id: int, field: str):
        assert field in ("delivered_at", "failed_at")
        with self.db:
            self.db.execute(f"UPDATE outbox SET {field}=? WHERE id=?", (now_ms(), outbox_id))

    def outbox_retry(self, outbox_id: int, attempts: int, next_at: int):
        with self.db:
            self.db.execute("UPDATE outbox SET attempts=?, next_attempt_at=? WHERE id=?",
                            (attempts, next_at, outbox_id))

    def outbox_size(self) -> int:
        return self.one("SELECT COUNT(*) FROM outbox WHERE delivered_at IS NULL AND failed_at IS NULL")[0]

    # --- ops ---

    def parse_errors_since(self, since: int) -> int:
        return self.one("SELECT COUNT(*) FROM raw_events WHERE parse_error IS NOT NULL AND received_at>?", since)[0]

    def purge(self, before: int):
        with self.db:
            self.db.execute("DELETE FROM messages WHERE sent_at<?", (before,))
            self.db.execute("DELETE FROM raw_events WHERE received_at<?", (before,))
            self.db.execute("DELETE FROM outbox WHERE created_at<? AND (delivered_at IS NOT NULL OR failed_at IS NOT NULL)",
                            (before,))

    def export(self, chat_id: str, start: int, end: int):
        rows = self.db.execute("SELECT * FROM messages WHERE chat_id=? AND sent_at>=? AND sent_at<? ORDER BY sent_at",
                               (chat_id, start, end))
        for row in rows:
            yield {"chat_id": chat_id, **self.message_json(row)}
