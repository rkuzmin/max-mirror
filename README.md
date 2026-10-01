# max-mirror

Неофициальное «зеркало» аккаунта мессенджера [MAX](https://max.ru): подключается
как веб-устройство по QR-коду, **только читает** переписку, складывает её в SQLite
и отдаёт поток новых, изменённых и удалённых сообщений на webhook с подписью.

*Unofficial read-only mirror of a MAX messenger account: QR login, SQLite history,
HMAC-signed webhook stream. Built on [PyMax](https://github.com/MaxApiTeam/PyMax).*

> [!WARNING]
> Используется неофициальный внутренний протокол MAX. Он может измениться без
> предупреждения, а сама работа через него может нарушать условия сервиса и
> привести к блокировке аккаунта. Подключайте **отдельный номер**, не основной.
> В базу попадает переписка третьих лиц: вы отвечаете за её хранение (152-ФЗ).

## Что умеет

- Вход по QR, сессия переживает перезапуски.
- Личные диалоги, группы, каналы: текст, фото, видео, файлы, голосовые, стикеры,
  ответы, пересланные. Неизвестные типы сохраняются как `unknown` с исходным JSON.
- Правки и удаления: `edited_at`, `deleted_at`, исходный текст остаётся в сырых событиях.
- Догрузка истории на N дней и добор пропуска после простоя; курсор переживает сбой.
- Webhook: at-least-once, порядок внутри чата, повторы с растущей задержкой, HMAC-SHA256.
- Только чтение: ни отправки, ни отметок о прочтении, ни «печатает»; телеметрия PyMax выключена.
- `GET /health`, JSON-логи, алерты, фильтр чатов, срок хранения, выгрузка чата в JSONL.

## Быстрый старт

```bash
git clone https://github.com/rkuzmin/max-mirror && cd max-mirror
uv sync
uv run max-mirror run
```

В консоли появится QR-код: приложение MAX → Настройки → Устройства → отсканировать.
В логе появится `auth_ok`. Данные и файл сессии лежат в `./data` (права 600).

Docker: первый запуск без `-d`, чтобы увидеть QR.

```bash
cp .env.example .env
docker compose up
```

## Настройки

Переменные окружения (см. [.env.example](.env.example)):

| Переменная | По умолчанию | Назначение |
| --- | --- | --- |
| `MAX_DATA_DIR` | `./data` | Каталог для `max.db` и `session.db` |
| `MAX_WEBHOOK_URL` | — | Куда слать события; пусто — только SQLite |
| `MAX_WEBHOOK_SECRET` | — | Секрет подписи, обязателен вместе с URL |
| `MAX_ALERT_URL` | — | POST `{"text": "..."}` при алерте (Slack-совместимый webhook) |
| `MAX_HISTORY_DAYS` | `30` | Глубина первичной догрузки |
| `MAX_HISTORY_RPS` | `1` | Лимит запросов истории в секунду |
| `MAX_CHATS_ALLOW` / `MAX_CHATS_DENY` | — | Белый / чёрный список `chat_id` через запятую |
| `MAX_CHAT_TYPES` | `dialog,group,channel` | Какие типы чатов сохранять |
| `MAX_RETENTION_DAYS` | `0` | Срок хранения; 0 — бессрочно |
| `MAX_HTTP_PORT` | `8080` | Порт `/health` |

## Webhook

```json
{
  "event": "message.created",
  "chat": {"id": "123", "type": "dialog", "title": "Иван Петров"},
  "message": {
    "id": "456",
    "sender": {"id": "789", "name": "Иван Петров"},
    "sent_at": "2026-10-01T12:30:05Z",
    "type": "text",
    "text": "Добрый день",
    "reply_to_id": null,
    "forwarded_from": null,
    "attachments": [],
    "edited_at": null,
    "deleted_at": null
  }
}
```

`event`: `message.created`, `message.edited`, `message.deleted`. Первичная догрузка
истории в webhook не уходит, сообщения, пропущенные за время простоя, — уходят.
Ответ не 2xx — повтор через 1 с, 2 с, 4 с… до 5 минут; через 24 часа событие
помечается `failed_at` в таблице `outbox`.

Проверка подписи (заголовок `X-Signature`, hex):

```python
import hashlib, hmac
ok = hmac.compare_digest(hmac.new(secret.encode(), body, hashlib.sha256).hexdigest(), signature)
```

Тестовый приёмник: `MAX_WEBHOOK_SECRET=... uv run python receiver.py` и
`MAX_WEBHOOK_URL=http://127.0.0.1:9000/`.

## Выгрузка и состояние

```bash
uv run max-mirror export --chat <chat_id> --from 2026-09-01 --to 2026-09-30 --out chat.jsonl
curl localhost:8080/health
```

`/health` отдаёт статус соединения (`connected`, `disconnected`, `session_revoked`…),
время последнего события, размер очереди webhook и счётчики; при отсутствии
соединения — код 503. Алерты: нет соединения дольше 5 минут, сессия отозвана,
ошибки разбора за час, в очереди больше 1000 событий.

Если сессию завершили в «Устройствах», сервис шлёт один алерт и ждёт: перезапустите
его и отсканируйте новый QR. Повторных попыток входа нет.

## Как устроено

| Модуль | Что делает |
| --- | --- |
| [`adapter.py`](max_mirror/adapter.py) | Единственный модуль, который импортирует PyMax: вход, перехват сырых кадров, запросы истории |
| [`normalize.py`](max_mirror/normalize.py) | Сырой JSON протокола → единая модель; не зависит от библиотеки |
| [`store.py`](max_mirror/store.py) | SQLite: `chats`, `users`, `messages`, `raw_events`, `outbox` |
| [`webhook.py`](max_mirror/webhook.py) | Доставка из `outbox` |
| [`service.py`](max_mirror/service.py) | Переподключение, история, алерты, `/health` |

Каждое событие сервера сохраняется в `raw_events` до разбора; ошибка разбора не роняет
процесс, а помечает событие `parse_error`. Сообщение и задание на webhook пишутся в
одной транзакции. Время в БД — миллисекунды Unix, идентификаторы — строки.
Версия PyMax закреплена: обновлять её только после прогона тестов и живой проверки.

## Тесты

```bash
uv run python test_pipeline.py
```

Прогоняет весь конвейер на фейковом адаптере, без сети и аккаунта.

## Лицензия

[MIT](LICENSE). Проект не связан с MAX и VK.
