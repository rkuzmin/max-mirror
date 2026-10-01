"""Raw MAX protocol payloads -> unified model.

Knows protocol field names (camelCase JSON), not the library: switching
PyMax to maxion leaves this file alone because both speak the same protocol.
Any exception here means "protocol changed": the caller flags the raw event.
"""

NOTIF_MESSAGE = 128
MSG_EDIT = 67
NOTIF_CHAT = 135
NOTIF_MSG_DELETE = 142

CHAT_TYPES = {"DIALOG": "dialog", "CHAT": "group", "CHANNEL": "channel"}
MEDIA_TYPES = {"PHOTO": "photo", "VIDEO": "video", "FILE": "file", "AUDIO": "voice", "STICKER": "sticker"}


def _sid(value):
    return None if value is None else str(value)


def message(raw: dict, chat_id) -> dict:
    link = raw.get("link") or {}
    linked = link.get("message") or {}
    forwarded = link.get("type") == "FORWARD"
    attaches = raw.get("attaches") or (linked.get("attaches") if forwarded else None) or []
    text = raw.get("text") or (linked.get("text") if forwarded else "") or ""
    media = next((MEDIA_TYPES[a.get("_type")] for a in attaches if a.get("_type") in MEDIA_TYPES), None)

    if forwarded:
        kind = "forwarded"
    elif link.get("type") == "REPLY":
        kind = "reply"
    else:
        kind = media or ("text" if text else "unknown")

    return {
        "chat_id": str(chat_id),
        "message_id": str(raw["id"]),
        "sender_id": _sid(raw.get("sender")),
        "sent_at": int(raw["time"]),
        "type": kind,
        "text": text,
        "reply_to_id": _sid(linked.get("id")) if kind == "reply" else None,
        "forwarded_from": _sid(link.get("chatId") or linked.get("sender")) if forwarded else None,
        # unknown type keeps the whole raw message (MSG-2)
        "attachments": [raw] if kind == "unknown" else attaches,
        "status": raw.get("status"),  # EDITED / REMOVED / None
        "updated_at": raw.get("updateTime"),
    }


def chat(raw: dict, me_id=None) -> dict:
    kind = CHAT_TYPES.get(str(raw.get("type")), str(raw.get("type")).lower())
    peers = [str(uid) for uid in (raw.get("participants") or {}) if str(uid) != str(me_id)]
    return {
        "chat_id": str(raw["id"]),
        "type": kind,
        "title": raw.get("title"),
        "peer_id": peers[0] if kind == "dialog" and peers else None,
        "last_event_at": raw.get("lastEventTime") or None,
    }


def user(raw: dict) -> dict:
    n = (raw.get("names") or [{}])[0]
    name = n.get("name") or " ".join(filter(None, [n.get("firstName"), n.get("lastName")]))
    return {"user_id": str(raw["id"]), "display_name": name or None}


def event(opcode: int, p: dict, me_id=None) -> list[tuple]:
    """Server-pushed frame -> actions: ("message", m) | ("deleted", chat_id, ids) | ("chat", c)."""
    if opcode in (NOTIF_MESSAGE, MSG_EDIT) and isinstance(p.get("message"), dict):
        m = message(p["message"], p["chatId"])
        if m["status"] == "REMOVED":
            return [("deleted", m["chat_id"], [m["message_id"]])]
        return [("message", m)]
    if opcode == NOTIF_MSG_DELETE:
        chat_id = p.get("chatId") or p["chat"]["id"]
        ids = p.get("messageIds") or [p["message"]["id"]]
        return [("deleted", str(chat_id), [str(i) for i in ids])]
    if opcode == NOTIF_CHAT and isinstance(p.get("chat"), dict):
        return [("chat", chat(p["chat"], me_id))]
    return []
