"""MAX -> Telegram: один чат MAX пересылается в одну тему Telegram-группы.

Режимы:
    python bridge.py login       — первый вход в MAX (SMS + пароль 2FA), интерактивно
    python bridge.py list-chats  — показать ID чатов MAX
    python bridge.py run         — основной режим (по умолчанию)
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import html
import json
import logging
import mimetypes
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
from pymax import (
    Client,
    ConsolePasswordProvider,
    ConsoleSmsCodeProvider,
    ExtraConfig,
    Message,
)
from pymax.types.domain import (
    AudioAttachment,
    CallAttachment,
    ContactAttachment,
    ControlAttachment,
    FileAttachment,
    PhotoAttachment,
    PollAttachment,
    ShareAttachment,
    StickerAttachment,
    VideoAttachment,
)
from pymax.types.domain.message import ForwardLink, ReplyLink

log = logging.getLogger("max2tg")

SESSION_NAME = "session.db"
TG_TEXT_LIMIT = 4096
TG_CAPTION_LIMIT = 1024
TG_ALBUM_LIMIT = 10


# --------------------------------------------------------------------------- config


def env(name: str, default: str | None = None, *, required: bool = False) -> str | None:
    value = os.environ.get(name, default)
    if value is not None:
        value = value.strip()
    if required and not value:
        sys.exit(f"Не задана переменная окружения {name}")
    return value or None


def env_bool(name: str, default: bool) -> bool:
    value = env(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on", "да"}


@dataclass
class Config:
    max_phone: str
    data_dir: Path
    max_chat_id: int | None = None
    tg_token: str | None = None
    tg_chat_id: str | None = None
    tg_thread_id: int | None = None
    max_password: str | None = None
    forward_own: bool = True
    forward_edits: bool = False
    max_upload_bytes: int = 50 * 1024 * 1024
    start_from: str = "now"
    catch_up: bool = True
    tg_min_interval: float = 3.0
    log_level: str = "INFO"

    @classmethod
    def load(cls, mode: str) -> Config:
        need_tg = mode == "run"
        thread = env("TG_THREAD_ID")
        chat = env("MAX_CHAT_ID", required=need_tg)
        return cls(
            max_phone=env("MAX_PHONE", required=True),  # type: ignore[arg-type]
            data_dir=Path(env("DATA_DIR", "/data")),  # type: ignore[arg-type]
            max_chat_id=int(chat) if chat else None,
            tg_token=env("TG_BOT_TOKEN", required=need_tg),
            tg_chat_id=env("TG_CHAT_ID", required=need_tg),
            tg_thread_id=int(thread) if thread else None,
            max_password=env("MAX_2FA_PASSWORD"),
            forward_own=env_bool("FORWARD_OWN", True),
            forward_edits=env_bool("FORWARD_EDITS", False),
            max_upload_bytes=int(env("TG_MAX_UPLOAD_MB", "50")) * 1024 * 1024,  # type: ignore[arg-type]
            start_from=env("START_FROM", "now") or "now",
            catch_up=env_bool("CATCH_UP", True),
            tg_min_interval=float(env("TG_MIN_INTERVAL", "3")),  # type: ignore[arg-type]
            log_level=(env("LOG_LEVEL", "INFO") or "INFO").upper(),
        )


# --------------------------------------------------------------------------- auth providers


class NoInteractiveSms:
    """В режиме run SMS-код не запрашиваем: без сессии нужно явно выполнить login."""

    async def get_code(self, phone: str) -> str:
        raise RuntimeError(
            "Сессия MAX недействительна. Выполните вход: docker compose run --rm max2tg login"
        )


class EnvPasswordProvider:
    def __init__(self, password: str) -> None:
        self.password = password

    async def get_password(self, hint: str | None = None) -> str:
        return self.password


class NoPassword:
    async def get_password(self, hint: str | None = None) -> str:
        raise RuntimeError(
            "MAX запросил пароль 2FA. Выполните: docker compose run --rm max2tg login"
        )


def make_client(cfg: Config, *, interactive: bool) -> Client:
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    if cfg.max_password:
        password_provider = EnvPasswordProvider(cfg.max_password)
    elif interactive:
        password_provider = ConsolePasswordProvider()
    else:
        password_provider = NoPassword()

    return Client(
        phone=cfg.max_phone,
        work_dir=str(cfg.data_dir),
        session_name=SESSION_NAME,
        sms_code_provider=ConsoleSmsCodeProvider() if interactive else NoInteractiveSms(),
        password_provider=password_provider,
        extra_config=ExtraConfig(
            log_level=cfg.log_level,
            # Без сессии в фоне не уходим в повторный вход: он отправил бы SMS.
            relogin=interactive,
            password_max_attempts=3,
            reconnect=True,
            reconnect_delay=5.0,
        ),
    )


# --------------------------------------------------------------------------- telegram


class TelegramError(Exception):
    pass


class Telegram:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        token: str,
        chat_id: str,
        thread_id: int | None,
        min_interval: float = 3.0,
    ):
        self.session = session
        # В одну группу Telegram пускает ~20 сообщений в минуту; держим темп сами.
        self.min_interval = min_interval
        self._next_send = 0.0
        self.base = f"https://api.telegram.org/bot{token}/"
        self.target: dict[str, str | int] = {"chat_id": chat_id}
        # Тема «General» имеет id 1 и передаётся без message_thread_id.
        if thread_id and thread_id != 1:
            self.target["message_thread_id"] = thread_id

    async def call(
        self,
        method: str,
        params: dict,
        files: dict[str, tuple[str, bytes]] | None = None,
    ) -> dict:
        params = {**self.target, **params}
        wait = self._next_send - time.monotonic()
        if wait > 0:
            await asyncio.sleep(wait)
        self._next_send = time.monotonic() + self.min_interval
        for attempt in range(6):
            if files:
                body = aiohttp.FormData()
                for key, value in params.items():
                    body.add_field(key, value if isinstance(value, str) else json.dumps(value))
                for key, (filename, content) in files.items():
                    body.add_field(key, content, filename=filename)
                request = self.session.post(self.base + method, data=body)
            else:
                request = self.session.post(self.base + method, json=params)

            try:
                async with request as resp:
                    data = await resp.json(content_type=None)
            except (TimeoutError, aiohttp.ClientError) as e:
                log.warning("telegram %s: сетевая ошибка %s, повтор", method, e)
                await asyncio.sleep(2**attempt)
                continue

            if data.get("ok"):
                return data["result"]

            code = data.get("error_code")
            if code == 429:
                delay = data.get("parameters", {}).get("retry_after", 5)
                log.warning("telegram flood limit, жду %s с", delay)
                await asyncio.sleep(delay + 1)
                continue
            if code and code >= 500:
                await asyncio.sleep(2**attempt)
                continue
            raise TelegramError(f"{method}: {data.get('description')}")
        raise TelegramError(f"{method}: не удалось после повторов")

    async def send_text(self, text: str) -> None:
        for chunk in split_text(text, TG_TEXT_LIMIT):
            await self.call(
                "sendMessage",
                {
                    "text": chunk,
                    "parse_mode": "HTML",
                    "link_preview_options": {"is_disabled": True},
                },
            )


def split_text(text: str, limit: int) -> list[str]:
    """Режет текст по строкам, не разрывая HTML-сущности (&amp;)."""
    chunks: list[str] = []
    while len(text) > limit:
        cut = text.rfind("\n", 0, limit)
        if cut < limit // 2:
            cut = text.rfind(" ", 0, limit)
        if cut < limit // 2:
            cut = limit
            amp = text.rfind("&", cut - 8, cut)
            if amp != -1 and ";" not in text[amp:cut]:
                cut = amp
        chunks.append(text[:cut])
        text = text[cut:].lstrip("\n")
    if text:
        chunks.append(text)
    return chunks


# --------------------------------------------------------------------------- bridge


@dataclass
class Media:
    kind: str  # photo | video | document | voice | sticker
    filename: str
    content: bytes


@dataclass
class Rendered:
    lines: list[str] = field(default_factory=list)
    media: list[Media] = field(default_factory=list)


class State:
    """Пересланные ID (защита от дублей) и время последнего сообщения (для догрузки)."""

    def __init__(self, path: Path, keep: int = 2000) -> None:
        self.path = path
        self.keep = keep
        try:
            data = json.loads(path.read_text())
        except (OSError, ValueError):
            data = {}
        self.ids: list[int] = data.get("forwarded", [])
        # Время (мс) последнего обработанного сообщения; None — мост ещё ни разу не запускался.
        self.last_time: int | None = data.get("last_time")
        self._set = set(self.ids)

    def seen(self, message_id: int) -> bool:
        return message_id in self._set

    def add(self, message_id: int, message_time: int) -> None:
        self.ids.append(message_id)
        self._set.add(message_id)
        if len(self.ids) > self.keep:
            for old in self.ids[: -self.keep]:
                self._set.discard(old)
            self.ids = self.ids[-self.keep :]
        self.last_time = max(self.last_time or 0, message_time)
        self.save()

    def save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"forwarded": self.ids, "last_time": self.last_time}))
        tmp.replace(self.path)


def ms(t: int | None) -> int:
    """Время MAX в миллисекундах; на случай секунд — нормализуем."""
    if not t:
        return 0
    return t * 1000 if t < 10**11 else t


def parse_start_from(value: str) -> tuple[str, int]:
    """START_FROM → (режим, параметр). Режимы: now | all | last (N) | time (мс)."""
    v = value.strip().lower()
    if v in ("", "now"):
        return "now", 0
    if v == "all":
        return "all", 0
    if v.startswith("last:"):
        return "last", int(v.split(":", 1)[1])
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(value.strip(), fmt)
        except ValueError:
            continue
        return "time", int(dt.timestamp() * 1000)  # локальное время контейнера (TZ)
    sys.exit(f"Не понял START_FROM={value!r}: now | all | last:N | 2026-11-01 | 2026-11-01 14:30")


class Bridge:
    def __init__(self, cfg: Config, client: Client, tg: Telegram, http: aiohttp.ClientSession):
        self.cfg = cfg
        self.client = client
        self.tg = tg
        self.http = http
        self.names: dict[int, str] = {}
        self.admins: set[int] | None = None
        self.lock = asyncio.Lock()
        self.state = State(cfg.data_dir / "state.json")
        # Живые сообщения ждут, пока догрузится пропущенное, чтобы не нарушать порядок.
        self.ready = asyncio.Event()

    # ---- helpers

    async def user_name(self, user_id: int | None) -> str:
        if user_id is None:
            return "MAX"
        if user_id in self.names:
            return self.names[user_id]
        name = f"id{user_id}"
        try:
            user = await self.client.get_user(user_id)
            if user and user.names:
                n = user.names[0]
                name = " ".join(p for p in (n.first_name, n.last_name) if p) or n.name or name
        except Exception as e:  # noqa: BLE001
            log.debug("не удалось получить имя %s: %s", user_id, e)
        self.names[user_id] = name
        return name

    async def chat_admins(self) -> set[int]:
        if self.admins is not None:
            return self.admins
        admins: set[int] = set()
        try:
            chat = await self.client.get_chat(self.cfg.max_chat_id)  # type: ignore[arg-type]
            admins = {chat.owner, *chat.admins, *chat.admin_participants}
        except Exception as e:  # noqa: BLE001
            log.warning("не удалось получить админов чата: %s", e)
            return admins  # не кешируем: попробуем ещё раз со следующим сообщением
        self.admins = admins
        return admins

    async def sender_header(self, user_id: int | None) -> str:
        header = f"<b>{html.escape(await self.user_name(user_id))}</b>"
        if user_id is not None and user_id in await self.chat_admins():
            header += " <i>админ</i>"
        return header

    async def download(self, url: str, fallback_name: str) -> tuple[str, bytes] | str:
        """Возвращает (имя, байты) или строку с причиной, почему не скачано."""
        try:
            async with self.http.get(url) as resp:
                if resp.status != 200:
                    return f"HTTP {resp.status}"
                size = resp.content_length
                if size and size > self.cfg.max_upload_bytes:
                    return f"слишком большой ({size // 1024 // 1024} МБ)"
                content = bytearray()
                async for chunk in resp.content.iter_chunked(1 << 16):
                    content.extend(chunk)
                    if len(content) > self.cfg.max_upload_bytes:
                        return "слишком большой"
                ctype = (resp.content_type or "").split(";")[0]
        except (TimeoutError, aiohttp.ClientError) as e:
            return f"ошибка загрузки: {e}"

        name = fallback_name
        if "." not in name:
            url_ext = Path(urlparse(url).path).suffix
            ext = url_ext if 1 < len(url_ext) <= 5 else mimetypes.guess_extension(ctype) or ""
            name += ext
        return name, bytes(content)

    async def fetch_media(
        self, kind: str, url: str | None, name: str, label: str, out: Rendered
    ) -> None:
        if not url:
            out.lines.append(f"[{label}: нет ссылки]")
            return
        result = await self.download(url, name)
        if isinstance(result, str):
            out.lines.append(f"[{label} не переслан: {html.escape(result)}]")
        else:
            out.media.append(Media(kind, *result))

    async def resolve_url(
        self, getter, message_ids: list[tuple[int, int]], attach_id: dict[str, int]
    ) -> str | None:
        """Ссылки на видео и файлы временные и выдаются по (chat_id, message_id)."""
        for chat_id, message_id in message_ids:
            try:
                info = await getter(chat_id=chat_id, message_id=message_id, **attach_id)
            except Exception as e:  # noqa: BLE001
                log.debug("ссылка не получена для %s/%s: %s", chat_id, message_id, e)
                continue
            if info and getattr(info, "url", None):
                return info.url
        return None

    # ---- rendering

    async def render(self, message: Message) -> Rendered:
        out = Rendered()
        header = await self.sender_header(message.sender)
        if message.time and time.time() * 1000 - ms(message.time) > 5 * 60 * 1000:
            stamp = datetime.fromtimestamp(ms(message.time) / 1000).strftime("%d.%m.%Y %H:%M")
            header += f" <i>· {stamp}</i>"
        out.lines.append(header)

        content = message
        ids = [(message.chat_id, message.id)]
        link = message.link

        if isinstance(link, ForwardLink):
            origin = link.chat_name or await self.user_name(link.message.sender)
            out.lines.append(f"<i>↪️ переслано от {html.escape(origin)}</i>")
            content = link.message
            ids.append((link.chat_id, link.message.id))
        elif isinstance(link, ReplyLink):
            who = await self.user_name(link.message.sender)
            quoted = (link.message.text or "вложение").replace("\n", " ")
            if len(quoted) > 120:
                quoted = quoted[:117] + "…"
            out.lines.append(
                f"<blockquote>↩️ {html.escape(who)}: {html.escape(quoted)}</blockquote>"
            )

        if content.text:
            out.lines.append(html.escape(content.text))

        for a in content.attaches:
            if isinstance(a, PhotoAttachment):
                await self.fetch_media("photo", a.base_url, f"photo_{a.photo_id}.jpg", "фото", out)
            elif isinstance(a, VideoAttachment):
                url = await self.resolve_url(
                    self.client.get_video_by_id, ids, {"video_id": a.video_id}
                )
                await self.fetch_media("video", url, f"video_{a.video_id}.mp4", "видео", out)
            elif isinstance(a, FileAttachment):
                url = await self.resolve_url(
                    self.client.get_file_by_id, ids, {"file_id": a.file_id}
                )
                await self.fetch_media("document", url, a.name or f"file_{a.file_id}", "файл", out)
            elif isinstance(a, AudioAttachment):
                await self.fetch_media("voice", a.url, f"voice_{a.audio_id or 0}", "голосовое", out)
            elif isinstance(a, StickerAttachment):
                await self.fetch_media("sticker", a.url, f"sticker_{a.sticker_id}", "стикер", out)
            elif isinstance(a, ShareAttachment):
                title = a.title or a.url or "ссылка"
                out.lines.append(
                    f"🔗 {html.escape(title)}"
                    + (f"\n{html.escape(a.url)}" if a.url and a.url != title else "")
                )
            elif isinstance(a, ContactAttachment):
                name = a.name or " ".join(p for p in (a.first_name, a.last_name) if p) or "контакт"
                out.lines.append(f"👤 Контакт: {html.escape(name)}")
            elif isinstance(a, PollAttachment):
                out.lines.append("📊 [опрос — откройте в MAX]")
            elif isinstance(a, ControlAttachment):
                out.lines.append(
                    f"<i>⚙️ {html.escape(a.event)}{': ' + html.escape(a.title) if a.title else ''}</i>"
                )
            elif isinstance(a, CallAttachment):
                out.lines.append("📞 [звонок]")
            else:
                out.lines.append(f"[вложение {html.escape(str(getattr(a, 'type', '?')))}]")
        return out

    # ---- sending

    async def send(self, r: Rendered) -> None:
        text = "\n".join(r.lines)
        if not r.media:
            await self.tg.send_text(text)
            return

        caption: str | None = text
        if len(text) > TG_CAPTION_LIMIT:
            await self.tg.send_text(text)
            caption = None

        album = [m for m in r.media if m.kind in ("photo", "video")]
        singles = [m for m in r.media if m.kind not in ("photo", "video")]
        if len(album) < 2:
            singles = album + singles
            album = []

        for i in range(0, len(album), TG_ALBUM_LIMIT):
            chunk = album[i : i + TG_ALBUM_LIMIT]
            files, items = {}, []
            for n, m in enumerate(chunk):
                key = f"f{n}"
                files[key] = (m.filename, m.content)
                item = {"type": m.kind, "media": f"attach://{key}"}
                if caption:
                    item.update(caption=caption, parse_mode="HTML")
                    caption = None
                items.append(item)
            await self.tg.call("sendMediaGroup", {"media": items}, files)

        for m in singles:
            await self.send_single(m, caption)
            caption = None

    async def send_single(self, m: Media, caption: str | None) -> None:
        extra = {"caption": caption, "parse_mode": "HTML"} if caption else {}
        attempts = {
            "photo": [("sendPhoto", "photo"), ("sendDocument", "document")],
            "video": [("sendVideo", "video"), ("sendDocument", "document")],
            "voice": [("sendVoice", "voice"), ("sendAudio", "audio"), ("sendDocument", "document")],
            "sticker": [
                ("sendSticker", "sticker"),
                ("sendPhoto", "photo"),
                ("sendDocument", "document"),
            ],
            "document": [("sendDocument", "document")],
        }[m.kind]
        last: Exception | None = None
        for method, field_name in attempts:
            params = {} if method == "sendSticker" else extra
            try:
                await self.tg.call(method, params, {field_name: (m.filename, m.content)})
                if method == "sendSticker" and caption:
                    await self.tg.send_text(caption)
                return
            except TelegramError as e:
                log.info("%s не прошёл (%s), пробую другой способ", method, e)
                last = e
        raise last or TelegramError("не удалось отправить вложение")

    # ---- handlers

    def wanted(self, message: Message) -> bool:
        if message.chat_id != self.cfg.max_chat_id:
            return False
        me = self.client.me.contact.id if self.client.me else None
        return self.cfg.forward_own or message.sender != me

    async def on_new(self, message: Message) -> None:
        if not self.wanted(message):
            return
        await self.ready.wait()
        await self.process(message)

    async def process(self, message: Message) -> None:
        async with self.lock:
            if self.state.seen(message.id):
                return
            try:
                await self.send(await self.render(message))
            except Exception as e:
                log.exception("не удалось переслать %s", message.id)
                try:
                    sender = await self.sender_header(message.sender)
                    await self.tg.send_text(
                        f"{sender}\n{html.escape(message.text or '')}\n"
                        f"<i>[ошибка пересылки: {html.escape(str(e))[:200]}]</i>"
                    )
                except Exception:
                    log.exception("не удалось отправить даже текст")
                    return
            self.state.add(message.id, ms(message.time))

    # ---- history

    async def history_since(self, since_ms: int, limit: int | None) -> list[Message]:
        """Листает историю назад от «сейчас» до since_ms (или до limit штук)."""
        chat_id = self.cfg.max_chat_id
        assert chat_id is not None
        collected: dict[int, Message] = {}
        cursor = int(time.time() * 1000)
        while True:
            page = await self.client.fetch_history(
                chat_id, from_time=cursor, backward=50, forward=0
            )
            page = [m for m in page if m.id not in collected]
            if not page:
                break
            for m in page:
                if m.chat_id is None:
                    m.chat_id = chat_id
                if ms(m.time) >= since_ms:
                    collected[m.id] = m
            oldest = min(ms(m.time) for m in page)
            if oldest < since_ms or (limit and len(collected) >= limit):
                break
            cursor = oldest - 1
            log.info("история: собрано %s сообщений, листаю дальше", len(collected))
            await asyncio.sleep(1)
        messages = sorted(collected.values(), key=lambda m: (ms(m.time), m.id))
        return messages[-limit:] if limit else messages

    async def backfill(self) -> None:
        self.ready.clear()
        try:
            if self.state.last_time is not None:
                if not self.cfg.catch_up:
                    return
                mode, since, limit = "catch-up", self.state.last_time, None
            else:
                mode, arg = parse_start_from(self.cfg.start_from)
                if mode == "now":
                    # Первый запуск без истории: отметка «отсюда», чтобы потом догружать простои.
                    self.state.last_time = int(time.time() * 1000)
                    self.state.save()
                    return
                since = arg if mode == "time" else 0
                limit = arg if mode == "last" else None

            messages = [m for m in await self.history_since(since, limit) if self.wanted(m)]
            todo = [m for m in messages if not self.state.seen(m.id)]
            if todo:
                eta = len(todo) * self.cfg.tg_min_interval / 60
                log.info("%s: перешлю %s сообщений (~%.0f мин)", mode, len(todo), eta)
            for i, m in enumerate(todo, 1):
                await self.process(m)
                if i % 50 == 0:
                    log.info("%s: %s/%s", mode, i, len(todo))
            if self.state.last_time is None:
                self.state.last_time = int(time.time() * 1000)
                self.state.save()
        except Exception:
            log.exception("догрузка истории не удалась, продолжаю с живыми сообщениями")
        finally:
            self.ready.set()

    async def on_edit(self, message: Message) -> None:
        if not self.cfg.forward_edits or not self.wanted(message):
            return
        async with self.lock:
            sender = await self.sender_header(message.sender)
            await self.tg.send_text(
                f"{sender} <i>✏️ изменил сообщение:</i>\n{html.escape(message.text or '')}"
            )


# --------------------------------------------------------------------------- modes


async def cmd_login(cfg: Config) -> None:
    client = make_client(cfg, interactive=True)
    try:
        await client.connect()
        me = client.me.contact if client.me else None
        name = me.names[0].name if me and me.names else "?"
        print(f"\nГотово: вход выполнен как {name} (id {me.id if me else '?'}).")
        print(f"Сессия сохранена в {cfg.data_dir / SESSION_NAME}")
    finally:
        await client.close()


async def cmd_list_chats(cfg: Config) -> None:
    require_session(cfg)
    client = make_client(cfg, interactive=False)
    try:
        await client.connect()
        chats = {c.id: c for c in (client.chats or [])}
        try:
            for c in await client.fetch_chats():
                chats[c.id] = c
        except Exception as e:  # noqa: BLE001
            log.warning("fetch_chats: %s", e)
        print(f"\n{'ID':>22}  {'тип':<10} название")
        for c in sorted(chats.values(), key=lambda c: -(c.last_event_time or 0)):
            ctype = getattr(c.type, "value", c.type)
            print(f"{c.id:>22}  {ctype!s:<10} {c.title or '(личный диалог)'}")
    finally:
        await client.close()


def require_session(cfg: Config) -> None:
    if not (cfg.data_dir / SESSION_NAME).exists():
        sys.exit("Нет сессии MAX. Сначала выполните: docker compose run --rm max2tg login")


async def cmd_run(cfg: Config) -> None:
    require_session(cfg)
    parse_start_from(cfg.start_from)  # проверить формат до подключения
    client = make_client(cfg, interactive=False)
    timeout = aiohttp.ClientTimeout(total=300, sock_connect=20)
    async with aiohttp.ClientSession(timeout=timeout) as http:
        tg = Telegram(
            http,
            cfg.tg_token,
            cfg.tg_chat_id,
            cfg.tg_thread_id,
            cfg.tg_min_interval,  # type: ignore[arg-type]
        )
        bridge = Bridge(cfg, client, tg, http)

        @client.on_start()
        async def started(c: Client) -> None:
            me = c.me.contact.id if c.me else "?"
            log.info("MAX подключён (id %s), слушаю чат %s", me, cfg.max_chat_id)
            await bridge.backfill()

        @client.on_message()
        async def new_message(message: Message, c: Client) -> None:
            await bridge.on_new(message)

        @client.on_message_edit()
        async def edited(message: Message, c: Client) -> None:
            await bridge.on_edit(message)

        try:
            await client.start()
        except Exception:
            log.exception("клиент MAX остановился")
            # Пауза, чтобы restart-политика не долбила сервер входами.
            await asyncio.sleep(300)
            raise


def main() -> None:
    parser = argparse.ArgumentParser(description="MAX -> Telegram topic bridge")
    parser.add_argument("mode", nargs="?", default="run", choices=["run", "login", "list-chats"])
    args = parser.parse_args()

    cfg = Config.load(args.mode)
    logging.basicConfig(
        level=cfg.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    runner = {"run": cmd_run, "login": cmd_login, "list-chats": cmd_list_chats}[args.mode]
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(runner(cfg))


if __name__ == "__main__":
    main()
