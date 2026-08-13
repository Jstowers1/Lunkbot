"""LunkserverManager Discord Bot.

Commands:
  /servers     -- live fleet container status from the dashboard backend
  /server      -- detail on a single container (status, uptime, RAM, CPU, IP)
  /start       -- start a server container
  /ping        -- gateway heartbeat + REST API round-trip latency
  /system      -- host system resources (CPU, RAM, disk)
  /leaderboard -- Lunkflix watch-time top-10 leaderboard
  /media       -- most-watched media by play count
  /stats       -- single-user Lunkflix watch profile
  /watched     -- recently watched items on Lunkflix
  /livetv      -- find what Live TV channel is playing a show/match
  /thischannel -- set this text channel for bot notifications/announcements
  /addresponse -- add a custom response to the bot's mention/reply pool
  /help        -- list all commands

Background:
  Lunkflix status -> bot rich presence (green "Watching Lunkflix" when up,
  DND "Lunkflix" when down). Updates every 3 min.
  Voice join -> random phrase (VC_JOIN_PHRASES env var) in the notification
  channel. Solo in a voice channel for 10 min -> lonely GIF in the channel.
  @mention or reply -> response picked by sentiment + keyword match from pool.
  Yes/no questions get a canned yes/no/maybe/idk answer.

Voice commands (when bot is in VC):
  "hey lunkbot <anything>" -> speaks a random quote from the pool via TTS.
  Text trigger "@lunkbot join vc" -> joins the speaker's voice channel.
  Text trigger "@lunkbot leave vc" -> leaves the voice channel.

All secrets come from env. No hardcoded tokens or IPs.
"""

import asyncio
import io
import json
import logging
import math
import os
import random
import re
import time
from datetime import datetime, timezone
from difflib import SequenceMatcher

logging.basicConfig(level=logging.WARNING)

import discord
from discord import option
from discord.ext import commands
from discord.sinks import Sink, Filters
from dotenv import load_dotenv
import edge_tts
import httpx
import speech_recognition as sr

import jellyfin_db

load_dotenv(override=True)

DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
BACKEND_URL = os.environ.get("BACKEND_URL", "http://127.0.0.1:8000")
BACKEND_TOKEN = os.environ.get("BACKEND_TOKEN", "")
PLAYBACK_DB_PATH = os.environ.get("PLAYBACK_DB_PATH", "")
JELLYFIN_DB_PATH = os.environ.get("JELLYFIN_DB_PATH", "")
JELLYFIN_URL = os.environ.get("JELLYFIN_URL", "").rstrip("/")
JELLYFIN_TOKEN = os.environ.get("JELLYFIN_TOKEN", "")
NOTIFY_CHANNEL_DB = os.environ.get("NOTIFY_CHANNEL_DB", "notify_channels.json")
RESPONSE_POOL_DB = os.environ.get("RESPONSE_POOL_DB", "response_pool.json")
QUOTES_CHANNEL_ID = int(os.environ.get("QUOTES_CHANNEL_ID", "0") or "0")

#Voice channel event config
SOLO_VC_TIMEOUT = 600
LONELY_GIF = os.environ.get("LONELY_GIF", "")
VC_JOIN_PHRASES = [
    p.strip()
    for p in os.environ.get("VC_JOIN_PHRASES", "hop on,jopon,gop on,joplin,japan").split(
        ","
    )
    if p.strip()
]

#Voice command config
WAKE_WORDS = [
    w.strip().lower()
    for w in os.environ.get(
        "VC_WAKE_WORDS", "hey lunkbot,at lunkbot,hey lunk bot,a lunkbot,lunchbox,lunk bots,lunk bots"
    ).split(",")
    if w.strip()
]

#Base responses loaded from a gitignored JSON file
DEFAULT_RESPONSES_DB = os.environ.get("DEFAULT_RESPONSES_DB", "default_responses.json")
DEFAULT_RESPONSES: list[str] = []

#Connection domains loaded from a gitignored JSON file
CUSTOM_DOMAINS_DB = os.environ.get("CUSTOM_DOMAINS_DB", "custom_domains.json")
CUSTOM_DOMAINS: dict[str, str] = {}
PLAY_DOMAIN = os.environ.get("PLAY_DOMAIN", "")
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "")

SERVER_ALIASES = {
    "lunkflix": "jellyfin",
}

STATE_EMOJI = {
    "running": "🟢",
    "stopped": "🔴",
    "restarting": "🟡",
    "unknown": "⚫",
}

intents = discord.Intents.default()
intents.voice_states = True
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

_notify_channels: dict[str, int] = {}
_solo_tasks: dict[int, asyncio.Task] = {}
_user_responses: list[str] = []
_smart_mode: bool = True

#Voice state: tracks the active Sink and per-user audio buffers
_voice_sink: "WakeupSink | None" = None


# ---------------------------------------------------------------------------
# Voice: custom Sink for per-user audio capture and wake-word detection
# ---------------------------------------------------------------------------

_SILENCE_BYTES = 3840  #20ms of silence at 48kHz 16-bit stereo = 3840 zero bytes
_UTTERANCE_TIMEOUT = 0.8  #seconds of silence before an utterance is considered done
_MIN_UTTERANCE_BYTES = 16000  #ignore tiny blips

#Fuzzy threshold for word-level wake-word matching
_WAKE_FUZZ_RATIO = 0.72

#Target words for fuzzy match
_WAKE_TARGETS = ("lunkbot", "lunkbots", "lunchbox")


def _fuzzy_wake_match(alnum_text: str) -> bool:
    """Return True if the transcription matches a wake word.

    Two passes:
    1. Exact substring on phrases and explicit list (handles "hey lunkbot").
    2. Fuzzy match each word against the core targets via difflib.
    """
    squashed = alnum_text.replace(" ", "")
    for w in WAKE_WORDS:
        if w.replace(" ", "") in squashed:
            return True

    words = alnum_text.split()
    for word in words:
        for target in _WAKE_TARGETS:
            if SequenceMatcher(None, word, target).ratio() >= _WAKE_FUZZ_RATIO:
                return True
    return False


class WakeupSink(Sink):
    """Captures per-user PCM audio. When a user stops talking (silence gap),
    the buffered audio is sent to the STT pipeline. If the transcription
    starts with a wake word, the bot speaks a random quote."""

    def __init__(self, bot_ref: commands.Bot):
        super().__init__()
        self.bot = bot_ref
        self._buffers: dict[int, bytearray] = {}
        self._last_audio: dict[int, float] = {}
        self._processing: set[int] = set()

    @Filters.container
    def write(self, data, user) -> None:
        """Voice packet received. data is VoiceData, user is the source."""
        pcm = getattr(data, "pcm", b"")
        if not pcm:
            return
        user_id = getattr(user, "id", user)
        print(f"[SINK] write: {len(pcm)} bytes from user {user_id}")
        #ponytail: serialize all audio, drop everyone while STT/TTS runs
        if self._processing:
            return
        buf = self._buffers.setdefault(user_id, bytearray())
        buf.extend(pcm)
        self._last_audio[user_id] = time.monotonic()

    def check_silence(self):
        """Called on a timer. For each user, if no audio arrived for
        _UTTERANCE_TIMEOUT seconds and the buffer is large enough, hand it
        to the STT pipeline."""
        now = time.monotonic()
        for user_id in list(self._buffers.keys()):
            last = self._last_audio.get(user_id, 0)
            if now - last < _UTTERANCE_TIMEOUT:
                continue
            buf = self._buffers.pop(user_id, None)
            if buf is None:
                continue
            print(f"[SINK] check_silence: user {user_id} buf={len(buf)} bytes, idle {now-last:.1f}s")
            if len(buf) < _MIN_UTTERANCE_BYTES:
                self._last_audio.pop(user_id, None)
                continue
            self._processing.add(user_id)
            asyncio.ensure_future(
                self._process_utterance(user_id, bytes(buf)), loop=self.bot.loop
            )

    async def _process_utterance(self, user_id: int, pcm: bytes):
        """Transcribe PCM, check for wake word, act on it."""
        try:
            text = await self.bot.loop.run_in_executor(None, _transcribe_pcm, pcm)
        except Exception as e:
            print(f"STT error for user {user_id}: {e}")
            return
        finally:
            self._processing.discard(user_id)

        if not text:
            return
        print(f"Voice STT ({user_id}): {text!r}")
        text_lower = text.lower().strip()

        #Wake word check (fuzzy: STT often mishears "lunkbot")
        alnum = re.sub(r"[^a-z0-9 ]", "", text_lower)
        matched = _fuzzy_wake_match(alnum)

        if not matched:
            return

        pool = _full_pool()
        if not pool:
            return
        quote = random.choice(pool)
        quote = _resolve_emojis(quote, None)
        await self._speak(quote)

    async def _speak(self, text: str):
        """Generate TTS audio and play it in the VC."""
        vc = _get_voice_client()
        if vc is None or not vc.is_connected():
            return
        #Stop listening while we speak to avoid feedback
        try:
            vc.stop_listening()
        except Exception:
            pass
        try:
            audio_data = await _generate_tts(text)
            if audio_data is None:
                return
            pcm = discord.FFmpegPCMAudio(audio_data, pipe=True)
            vc.play(discord.PCMVolumeTransformer(pcm, volume=2.0))
            #Wait for playback to finish before resuming listening
            while vc.is_playing():
                await asyncio.sleep(0.2)
        except Exception as e:
            print(f"TTS playback error: {e}")
        finally:
            #Resume listening
            try:
                vc.start_listening(self)
            except Exception as e:
                print(f"Resume listening error: {e}")


def _transcribe_pcm(pcm: bytes) -> str:
    """Convert raw 48kHz stereo PCM to 16kHz mono WAV, then run Google STT."""
    import array
    import wave
    import struct

    #Pycord delivers 48kHz 16-bit signed stereo PCM
    SAMPLE_RATE = 48000
    CHANNELS = 2
    SAMPLE_WIDTH = 2

    #Convert bytes to int16 array
    try:
        samples = array.array("h")
        samples.frombytes(pcm)
    except ValueError:
        return ""

    #Downmix stereo to mono: average left and right
    mono = []
    for i in range(0, len(samples) - 1, 2):
        mono.append((samples[i] + samples[i + 1]) // 2)
    if not mono:
        return ""

    #Write mono WAV to in-memory buffer
    wav_buf = io.BytesIO()
    with wave.open(wav_buf, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(SAMPLE_WIDTH)
        wav.setframerate(SAMPLE_RATE)
        wav.writeframes(array.array("h", mono).tobytes())
    wav_buf.seek(0)

    recognizer = sr.Recognizer()
    audio = sr.AudioData(wav_buf.read(), SAMPLE_RATE, SAMPLE_WIDTH)
    try:
        return recognizer.recognize_google(audio)
    except sr.UnknownValueError:
        return ""
    except sr.RequestError as e:
        print(f"Google STT request failed: {e}")
        return ""


async def _generate_tts(text: str) -> io.BytesIO | None:
    """Generate TTS audio via edge-tts. Returns a BytesIO of MP3 data."""
    mp3_buf = io.BytesIO()
    try:
        communicate = edge_tts.Communicate(text, voice="en-US-GuyNeural")
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                mp3_buf.write(chunk["data"])
    except Exception as e:
        print(f"edge-tts error: {e}")
        return None
    if mp3_buf.tell() == 0:
        return None
    mp3_buf.seek(0)
    return mp3_buf


def _get_voice_client() -> "discord.voice.VoiceClient | None":
    """Return the first connected VoiceClient, if any."""
    for vc in bot.voice_clients:
        if vc.is_connected():
            return vc
    return None


async def _silence_checker():
    """Background task that checks for completed utterances every 500ms."""
    await bot.wait_until_ready()
    while not bot.is_closed():
        sink = _voice_sink
        if sink is not None:
            sink.check_silence()
        await asyncio.sleep(0.2)


# ---------------------------------------------------------------------------
# Bot lifecycle
# ---------------------------------------------------------------------------

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} (id={bot.user.id})")
    _load_notify_channels()
    _load_default_responses()
    _load_custom_domains()
    _load_user_responses()
    bot.loop.create_task(_parse_quotes_channel())
    bot.loop.create_task(_silence_checker())
    try:
        await bot.sync_commands()
        print(f"Synced slash commands.")
    except Exception as e:
        print(f"Slash command sync failed: {e}")
    bot.loop.create_task(_lunkflix_presence())


#HTTP helpers

def _auth_headers() -> dict:
    if BACKEND_TOKEN:
        return {"Authorization": f"Bearer {BACKEND_TOKEN}"}
    return {}


async def _fetch_json(path: str):
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(f"{BACKEND_URL}{path}", headers=_auth_headers())
        resp.raise_for_status()
        return resp.json()


async def _post_json(path: str):
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(f"{BACKEND_URL}{path}", headers=_auth_headers())
        resp.raise_for_status()
        return resp.json()


def _connection_url(server_id: str, client_port: int | None) -> str:
    if server_id in CUSTOM_DOMAINS:
        return CUSTOM_DOMAINS[server_id]
    if client_port and PLAY_DOMAIN:
        return f"{PLAY_DOMAIN}:{client_port}"
    return "N/A"


#Notification channel persistence

def _load_notify_channels():
    global _notify_channels
    try:
        with open(NOTIFY_CHANNEL_DB, "r") as f:
            raw = json.load(f)
        _notify_channels = {str(k): int(v) for k, v in raw.items()}
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        _notify_channels = {}


def _save_notify_channels():
    with open(NOTIFY_CHANNEL_DB, "w") as f:
        json.dump(_notify_channels, f)


#Response pool

def _load_default_responses():
    global DEFAULT_RESPONSES
    try:
        with open(DEFAULT_RESPONSES_DB, "r") as f:
            DEFAULT_RESPONSES = list(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        DEFAULT_RESPONSES = []


def _load_custom_domains():
    global CUSTOM_DOMAINS
    try:
        with open(CUSTOM_DOMAINS_DB, "r") as f:
            CUSTOM_DOMAINS = dict(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        CUSTOM_DOMAINS = {}


def _load_user_responses():
    global _user_responses
    try:
        with open(RESPONSE_POOL_DB, "r") as f:
            _user_responses = list(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        _user_responses = []


def _save_user_responses():
    with open(RESPONSE_POOL_DB, "w") as f:
        json.dump(_user_responses, f)


def _full_pool() -> list[str]:
    return DEFAULT_RESPONSES + _user_responses


_EMOJI_TOKEN_RE = re.compile(r":([A-Za-z0-9_]+):")


def _resolve_emojis(text: str, guild: discord.Guild | None) -> str:
    if guild is None:
        return text
    def _sub(m: re.Match) -> str:
        emoji = discord.utils.get(guild.emojis, name=m.group(1))
        return str(emoji) if emoji else m.group(0)
    return _EMOJI_TOKEN_RE.sub(_sub, text)


_QUOTE_RE = re.compile(
    r'^"(?P<quote>[^"\n]+)"\s*\n-\s*(?P<author>[^\n]+)$', re.MULTILINE
)


async def _parse_quotes_channel():
    if not QUOTES_CHANNEL_ID:
        return
    channel = bot.get_channel(QUOTES_CHANNEL_ID)
    if channel is None:
        return
    added = 0
    async for msg in channel.history(limit=None, oldest_first=True):
        if msg.author.bot:
            continue
        for m in _QUOTE_RE.finditer(msg.content):
            quote = m.group("quote").strip()
            if quote and quote not in _user_responses and quote not in DEFAULT_RESPONSES:
                _user_responses.append(quote)
                added += 1
    if added:
        _save_user_responses()
        print(f"Parsed {added} quotes from channel {QUOTES_CHANNEL_ID} into response pool.")


async def _send_to_notify_channel(guild_id: int, content: str | None = None, embed: discord.Embed | None = None):
    chan_id = _notify_channels.get(str(guild_id))
    if not chan_id:
        return False
    channel = bot.get_channel(chan_id)
    if not isinstance(channel, discord.TextChannel):
        return False
    try:
        kwargs: dict = {}
        if content is not None:
            kwargs["content"] = content
        if embed is not None:
            kwargs["embed"] = embed
        await channel.send(**kwargs)
        return True
    except discord.HTTPException as e:
        print(f"Failed to send to notify channel {chan_id}: {e}")
        return False


#Servers command

async def _fetch_fleet():
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(f"{BACKEND_URL}/api/bot/status", headers=_auth_headers())
        resp.raise_for_status()
        return resp.json().get("servers", [])


@bot.slash_command(name="servers", description="Show live status of all managed game/media servers")
async def servers_cmd(ctx: discord.ApplicationContext):
    await ctx.defer()
    try:
        servers = await _fetch_fleet()
    except Exception as e:
        await ctx.followup.send(f"⚠️ Failed to reach dashboard backend: `{e}`")
        return

    if not servers:
        await ctx.followup.send("No servers found.")
        return

    running = sum(1 for s in servers if s["state"] == "running")
    total = len(servers)
    embed = discord.Embed(
        title="🖥️ Fleet Status",
        description=f"{running}/{total} containers running",
        color=0x2ECC71 if running > 0 else 0xE74C3C,
        timestamp=datetime.now(timezone.utc),
    )
    by_host: dict[str, list] = {}
    for s in servers:
        by_host.setdefault(s["host"], []).append(s)

    for host, entries in by_host.items():
        entries.sort(key=lambda s: (s["state"] != "running", s["name"].lower()))
        lines = []
        for s in entries:
            emoji = STATE_EMOJI.get(s["state"], "⚫")
            name = s["name"][:40]
            lines.append(f"{emoji} **{name}** — {s['state']}")
        embed.add_field(name=f"📍 {host}", value="\n".join(lines), inline=False)

    await ctx.followup.send(embed=embed)


#Ping command

@bot.slash_command(name="ping", description="Show bot gateway + REST API latency")
async def ping_cmd(ctx: discord.ApplicationContext):
    t0 = time.perf_counter()
    await ctx.respond("Pinging...")
    rest_ms = round((time.perf_counter() - t0) * 1000)
    gw_ms = round(bot.latency * 1000)

    embed = discord.Embed(title="Pong!", color=0x3498DB)
    embed.add_field(name="Gateway Heartbeat", value=f"`{gw_ms} ms`")
    embed.add_field(name="REST Round-Trip", value=f"`{rest_ms} ms`")
    await ctx.edit(embed=embed)


#Duration helper

def _fmt_duration(seconds: int) -> str:
    hours, remainder = divmod(int(seconds), 3600)
    minutes = remainder // 60
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


#Leaderboard command

@bot.slash_command(name="leaderboard", description="Top 10 Lunkflix users by total watch time")
async def leaderboard_cmd(ctx: discord.ApplicationContext):
    await ctx.defer()
    if not PLAYBACK_DB_PATH or not JELLYFIN_DB_PATH:
        await ctx.followup.send(
            "⚠️ Lunkflix database paths not configured (`PLAYBACK_DB_PATH` / `JELLYFIN_DB_PATH`)."
        )
        return

    board = await bot.loop.run_in_executor(
        None, jellyfin_db.get_leaderboard, PLAYBACK_DB_PATH, JELLYFIN_DB_PATH, 10
    )
    if not board:
        await ctx.followup.send(
            "📊 No playback data yet. Tell people to watch something on Lunkflix!"
        )
        return

    medals = ["🥇", "🥈", "🥉"] + [""] * 7
    lines = []
    for i, entry in enumerate(board):
        medal = medals[i] if i < len(medals) else ""
        dur = _fmt_duration(entry["total_seconds"])
        lines.append(f"{medal} **{entry['username']}** — {dur} ({entry['play_count']} plays)")

    embed = discord.Embed(
        title="📊 Lunkflix Watch Leaderboard",
        description="\n".join(lines),
        color=0xF1C40F,
        timestamp=datetime.now(timezone.utc),
    )
    await ctx.followup.send(embed=embed)


#System command

@bot.slash_command(name="system", description="Host system resources (CPU, RAM, disk)")
async def system_cmd(ctx: discord.ApplicationContext):
    await ctx.defer()
    try:
        data = await _fetch_json("/api/system")
    except Exception as e:
        await ctx.followup.send(f"⚠️ Failed to reach backend: `{e}`")
        return

    cpu = data.get("cpu_percent", 0)
    ram_pct = data.get("ram_percent", 0)
    ram_used = data.get("ram_used", "?")
    ram_total = data.get("ram_total", "?")
    disk_pct = data.get("storage_percent", 0)
    disk_used = data.get("storage_used", "?")
    disk_total = data.get("storage_total", "?")

    def _bar(pct):
        filled = int(pct / 10)
        return "█" * filled + "░" * (10 - filled)

    embed = discord.Embed(title="🖥️ System Resources", color=0x3498DB)
    embed.add_field(name=f"CPU `{cpu:.1f}%`", value=f"`{_bar(cpu)}`", inline=False)
    embed.add_field(name=f"RAM `{ram_pct:.1f}%`", value=f"`{_bar(ram_pct)}` ({ram_used}/{ram_total} GB)", inline=False)
    embed.add_field(name=f"Disk `{disk_pct:.1f}%`", value=f"`{_bar(disk_pct)}` ({disk_used}/{disk_total} GB)", inline=False)
    await ctx.followup.send(embed=embed)


#Server command

@bot.slash_command(name="server", description="Show details for a specific server")
@option("name", description="Server name (e.g. lunkflix, jellyfin, minecraft_01)")
async def server_cmd(ctx: discord.ApplicationContext, name: str):
    await ctx.defer()
    sid = SERVER_ALIASES.get(name.strip().lower(), name.strip().lower())
    try:
        data = await _fetch_json(f"/api/servers/{sid}")
    except Exception as e:
        await ctx.followup.send(f"⚠️ Could not fetch server `{sid}`: `{e}`")
        return

    status = data.get("status", "unknown")
    emoji = STATE_EMOJI.get(status, "⚫")
    embed = discord.Embed(
        title=f"{emoji} {data.get('name') or sid}",
        color=0x2ECC71 if status == "running" else 0xE74C3C,
    )
    embed.add_field(name="Status", value=f"`{status}`", inline=True)
    embed.add_field(name="Uptime", value=f"`{data.get('uptime', 'Offline')}`", inline=True)
    embed.add_field(name="Type", value=f"`{data.get('game_type', 'unknown')}`", inline=True)
    conn_url = _connection_url(sid, data.get("client_port"))
    embed.add_field(name="Connect", value=f"`{conn_url}`", inline=True)
    if status == "running":
        embed.add_field(name="CPU", value=f"`{data.get('cpu_load', 0):.1f}%`", inline=True)
        embed.add_field(name="RAM", value=f"`{data.get('ram_used', 0):.2f}` GB", inline=True)
    if data.get("description"):
        embed.add_field(name="", value=data["description"], inline=False)
    await ctx.followup.send(embed=embed)


#Start command

@bot.slash_command(name="start", description="Start a server container")
@option("name", description="Server name (e.g. lunkflix, jellyfin, minecraft_01)")
async def start_cmd(ctx: discord.ApplicationContext, name: str):
    await ctx.defer()
    sid = SERVER_ALIASES.get(name.strip().lower(), name.strip().lower())
    try:
        result = await _post_json(f"/api/servers/{sid}/start")
        msg = result.get("message", "Started")
        embed = discord.Embed(
            title=f"🟢 {sid} — {msg}",
            color=0x2ECC71,
            timestamp=datetime.now(timezone.utc),
        )
        try:
            data = await _fetch_json(f"/api/servers/{sid}")
            conn_url = _connection_url(sid, data.get("client_port"))
            embed.add_field(name="Connect", value=f"`{conn_url}`", inline=False)
        except Exception:
            pass
        await ctx.followup.send(embed=embed)
    except httpx.HTTPStatusError as e:
        await ctx.followup.send(f"⚠️ Backend rejected start: `{e.response.text[:200]}`")
    except Exception as e:
        await ctx.followup.send(f"⚠️ Could not start `{sid}`: `{e}`")


#Watched command

@bot.slash_command(name="watched", description="Recently watched items on Lunkflix")
async def watched_cmd(ctx: discord.ApplicationContext):
    await ctx.defer()
    if not PLAYBACK_DB_PATH or not JELLYFIN_DB_PATH:
        await ctx.followup.send("⚠️ Lunkflix database paths not configured.")
        return
    recent = await bot.loop.run_in_executor(
        None, jellyfin_db.get_recent_activity, PLAYBACK_DB_PATH, JELLYFIN_DB_PATH, 10
    )
    if not recent:
        await ctx.followup.send("📺 No recent playback activity.")
        return
    lines = []
    for entry in recent:
        dur = _fmt_duration(entry["duration"])
        lines.append(f"**{entry['username']}** watched *{entry['item_name'][:60]}* ({dur})")
    embed = discord.Embed(
        title="📺 Recently Watched",
        description="\n".join(lines),
        color=0x9B59B6,
    )
    await ctx.followup.send(embed=embed)


#Livetv command

async def _fetch_livetv_programs() -> list[dict]:
    if not JELLYFIN_URL or not JELLYFIN_TOKEN:
        return []
    headers = {"X-Emby-Token": JELLYFIN_TOKEN}
    async with httpx.AsyncClient(timeout=20) as client:
        ch_resp = await client.get(
            f"{JELLYFIN_URL}/LiveTv/Channels",
            headers=headers,
            params={"Limit": 3000},
        )
        ch_resp.raise_for_status()
        chmap = {
            ch["Id"]: ch.get("Name", "?")
            for ch in ch_resp.json().get("Items", [])
            if ch.get("Id")
        }

        prog_resp = await client.post(
            f"{JELLYFIN_URL}/LiveTv/Programs",
            headers=headers,
            json={"IsAiring": True, "Limit": 1000, "EnableImages": False},
        )
        prog_resp.raise_for_status()
        programs = prog_resp.json().get("Items", [])

    return [
        {
            "channel_name": chmap.get(p.get("ChannelId"), "?"),
            "program_title": p.get("Name", ""),
            "program_subtitle": p.get("Subtitle") or p.get("OriginalTitle") or "",
            "start": p.get("StartDate", ""),
            "end": p.get("EndDate", ""),
        }
        for p in programs
    ]


@bot.slash_command(name="livetv", description="Find what Live TV channel is playing something (e.g. /livetv spurs)")
@option("query", description="What to search for (team, show, movie name)")
async def livetv_cmd(ctx: discord.ApplicationContext, query: str):
    await ctx.defer()
    if not JELLYFIN_URL or not JELLYFIN_TOKEN:
        await ctx.followup.send(
            "⚠️ Live TV not configured (`JELLYFIN_URL` / `JELLYFIN_TOKEN` env vars not set)."
        )
        return

    try:
        programs = await _fetch_livetv_programs()
    except Exception as e:
        await ctx.followup.send(f"⚠️ Failed to reach Jellyfin Live TV: `{e}`")
        return

    if not programs:
        await ctx.followup.send("📺 No Live TV programs currently airing.")
        return

    q_lower = query.strip().lower()
    matches = [
        p for p in programs
        if q_lower in p["program_title"].lower()
        or q_lower in p["program_subtitle"].lower()
        or q_lower in p["channel_name"].lower()
    ]

    if not matches:
        await ctx.followup.send(
            f"🔍 No Live TV programs matching '**{query}**' found right now.\n"
            f"Checked {len(programs)} channels."
        )
        return

    lines = []
    for m in matches[:5]:
        title = m["program_title"] or "Unknown"
        sub = f" — {m['program_subtitle']}" if m["program_subtitle"] else ""
        lines.append(f"📡 **{m['channel_name']}** — *{title}*{sub}")

    embed = discord.Embed(
        title="📺 Live TV Results",
        description="\n".join(lines),
        color=0xE67E22,
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(
        name="Search",
        value=f"`{query}` — {len(matches)} match(es) out of {len(programs)} channels",
        inline=False,
    )
    await ctx.followup.send(embed=embed)


#Media command

@bot.slash_command(name="media", description="Top watched media by play count")
async def media_cmd(ctx: discord.ApplicationContext):
    await ctx.defer()
    if not PLAYBACK_DB_PATH:
        await ctx.followup.send("⚠️ Lunkflix database paths not configured.")
        return
    board = await bot.loop.run_in_executor(
        None, jellyfin_db.get_media_leaderboard, PLAYBACK_DB_PATH, 10
    )
    if not board:
        await ctx.followup.send("📊 No media playback data yet.")
        return
    medals = ["🥇", "🥈", "🥉"] + [""] * 7
    lines = []
    for i, entry in enumerate(board):
        medal = medals[i] if i < len(medals) else ""
        dur = _fmt_duration(entry["total_seconds"]) if entry["total_seconds"] else "N/A"
        lines.append(f"{medal} **{entry['name'][:50]}** — {entry['plays']} plays ({dur}) [{entry['item_type']}]")
    embed = discord.Embed(
        title="📊 Most Watched Media",
        description="\n".join(lines),
        color=0xE67E22,
        timestamp=datetime.now(timezone.utc),
    )
    await ctx.followup.send(embed=embed)


#Stats command

@bot.slash_command(name="stats", description="Your Lunkflix watch profile (total time, top media)")
@option("user", description="Lunkflix username (defaults to your Discord name)")
async def stats_cmd(ctx: discord.ApplicationContext, user: str = ""):
    await ctx.defer()
    if not PLAYBACK_DB_PATH or not JELLYFIN_DB_PATH:
        await ctx.followup.send("⚠️ Lunkflix database paths not configured.")
        return
    username = user.strip() or ctx.user.name
    stats = await bot.loop.run_in_executor(
        None, jellyfin_db.get_user_stats, username, PLAYBACK_DB_PATH, JELLYFIN_DB_PATH
    )
    if stats is None:
        await ctx.followup.send(f"❌ User `{username}` not found in Lunkflix.")
        return
    if stats["play_count"] == 0:
        await ctx.followup.send(f"📊 `{username}` has no real playback data yet.")
        return
    embed = discord.Embed(
        title=f"📊 {username}'s Watch Profile",
        color=0x9B59B6,
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Total Watch Time", value=f"`{_fmt_duration(stats['total_seconds'])}`", inline=True)
    embed.add_field(name="Total Plays", value=f"`{stats['play_count']}`", inline=True)
    if stats["top_media"]:
        top_lines = []
        for i, m in enumerate(stats["top_media"], 1):
            top_lines.append(f"{i}. **{m['name'][:40]}** — {m['plays']} plays")
        embed.add_field(name="Top Media", value="\n".join(top_lines), inline=False)
    if stats["last_watched"]:
        lw = stats["last_watched"]
        embed.add_field(
            name="Last Watched",
            value=f"*{lw['name'][:50]}* ({_fmt_duration(lw['duration'])})",
            inline=False,
        )
    await ctx.followup.send(embed=embed)


#Thischannel command

@bot.slash_command(name="thischannel", description="Set this text channel for bot notifications and announcements")
@commands.guild_only()
async def thischannel_cmd(ctx: discord.ApplicationContext):
    if ctx.guild is None or ctx.channel is None:
        await ctx.respond("⚠️ This command can only be used in a server text channel.")
        return
    gid = str(ctx.guild_id)
    _notify_channels[gid] = ctx.channel_id
    _save_notify_channels()
    chan_mention = ctx.channel.mention
    embed = discord.Embed(
        title="📢 Notification Channel Set",
        description=f"Bot announcements will be sent to {chan_mention}.\n"
                    f"Voice join alerts and other notifications will appear here.",
        color=0x2ECC71,
    )
    await ctx.respond(embed=embed)


#Addresponse command

@bot.slash_command(name="addresponse", description="Add a custom response to the bot's mention/reply pool")
@option("response", description="The response text to add")
async def addresponse_cmd(ctx: discord.ApplicationContext, response: str):
    resp = response.strip()
    if not resp:
        await ctx.respond("⚠️ Response can't be empty.")
        return
    if resp in DEFAULT_RESPONSES or resp in _user_responses:
        await ctx.respond("⚠️ That response is already in the pool.")
        return
    _user_responses.append(resp)
    _save_user_responses()
    await ctx.respond(
        f"✅ Added to pool (now {len(_full_pool())} responses):\n> {resp}"
    )


#Random command

@bot.slash_command(name="random", description="Toggle smart response mode on/off")
async def random_cmd(ctx: discord.ApplicationContext):
    global _smart_mode
    _smart_mode = not _smart_mode
    status = "ON (sentiment + keyword matching)" if _smart_mode else "OFF (pure random)"
    await ctx.respond(f"🎲 Smart mode: **{status}**")


#Help command

@bot.slash_command(name="help", description="List all bot commands")
async def help_cmd(ctx: discord.ApplicationContext):
    embed = discord.Embed(
        title="🤖 Lunkbot Commands",
        color=0x2ECC71,
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(
        name="🦍 About",
        value=(
            "Lunkbot is a Discord companion bot to the Lunkserver Software Suite\n"
            + (f"The full sized dashboard can be found at {DASHBOARD_URL}\n" if DASHBOARD_URL else "")
            + "If you have any suggestions or critiques, Lunkcorp is fairly receptive to them.\n"
        ),
        inline=False,
    )
    embed.add_field(
        name="🖥️ Servers",
        value=(
            "`/servers` — all containers grouped by host\n"
            "`/server <name>` — single container detail + IP\n"
            "`/start <name>` — start a container\n"
            "`/system` — host CPU/RAM/disk\n"
            "`/ping` — bot latency"
        ),
        inline=False,
    )
    embed.add_field(
        name="📊 Lunkflix",
        value=(
            "`/leaderboard` — top 10 by watch time\n"
            "`/media` — top watched media by plays\n"
            "`/stats [user]` — per-user watch profile\n"
            "`/watched` — recent playback activity\n"
            "`/livetv <query>` — find what Live TV channel is playing"
        ),
        inline=False,
    )
    embed.add_field(
        name="🔔 Notifications",
        value=(
            "`/thischannel` — set this channel for bot announcements\n"
            "(voice joins, lonely-GIF alerts, etc.)"
        ),
        inline=False,
    )
    embed.add_field(
        name="🎤 Voice",
        value=(
            "@lunkbot **join vc** — joins your voice channel\n"
            "@lunkbot **leave vc** — leaves the voice channel\n"
            'Say "**hey lunkbot**" in VC → bot speaks a random quote'
        ),
        inline=False,
    )
    embed.add_field(
        name="💬 Responses",
        value=(
            "`/addresponse <text>` — add a custom response to the pool\n"
            "`/random` — toggle smart matching (sentiment + keywords) on/off\n"
            "@mention or reply to the bot → it replies with a smart-matched response"
        ),
        inline=False,
    )
    embed.add_field(
        name="ℹ️",
        value="Bot presence shows live Lunkflix status (green = up, red = down).",
        inline=False,
    )
    await ctx.respond(embed=embed)


# ---------------------------------------------------------------------------
# Voice channel events for join alerts, lonely GIF, and voice commands
# ---------------------------------------------------------------------------

def _human_members(channel: discord.VoiceChannel | discord.StageChannel) -> list[discord.Member]:
    return [m for m in channel.members if not m.bot]


async def _solo_vigil(channel: discord.VoiceChannel | discord.StageChannel):
    try:
        await asyncio.sleep(SOLO_VC_TIMEOUT)
    except asyncio.CancelledError:
        return
    humans = _human_members(channel)
    if len(humans) == 1:
        lone_user = humans[0]
        content = f"🥺 {lone_user.mention} has been alone in **{channel.name}** for a while..."
        if LONELY_GIF:
            content += f"\n{LONELY_GIF}"
        sent = await _send_to_notify_channel(channel.guild.id, content=content)
        if not sent:
            print(f"No notify channel for guild {channel.guild.id}; couldn't post lonely GIF.")
    _solo_tasks.pop(channel.id, None)


async def _start_listening(vc):
    """Start the WakeupSink on a voice client."""
    global _voice_sink
    _voice_sink = WakeupSink(bot)
    try:
        vc.start_listening(_voice_sink)
        print(f"Started listening in VC with {len(WAKE_WORDS)} wake words.")
    except Exception as e:
        print(f"start_listening failed: {e}")


@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot:
        return

    joined = before.channel is None and after.channel is not None
    left = before.channel is not None and after.channel is None
    moved = (
        before.channel is not None
        and after.channel is not None
        and before.channel.id != after.channel.id
    )

    if (joined or moved) and after.channel is not None:
        if VC_JOIN_PHRASES:
            phrase = random.choice(VC_JOIN_PHRASES)
            await _send_to_notify_channel(
                member.guild.id,
                content=f"{member.display_name} joined **{after.channel.name}** — {phrase}",
            )

        humans = _human_members(after.channel)
        if len(humans) == 1:
            existing = _solo_tasks.pop(after.channel.id, None)
            if existing:
                existing.cancel()
            _solo_tasks[after.channel.id] = bot.loop.create_task(_solo_vigil(after.channel))
        else:
            existing = _solo_tasks.pop(after.channel.id, None)
            if existing:
                existing.cancel()

    if (left or moved) and before.channel is not None:
        old_channel = before.channel
        humans = _human_members(old_channel)
        if len(humans) == 1:
            existing = _solo_tasks.pop(old_channel.id, None)
            if existing:
                existing.cancel()
            _solo_tasks[old_channel.id] = bot.loop.create_task(_solo_vigil(old_channel))
        elif len(humans) == 0:
            existing = _solo_tasks.pop(old_channel.id, None)
            if existing:
                existing.cancel()

    #Bot auto-leaves if no humans remain in the VC
    vc = _get_voice_client()
    if vc and vc.channel:
        humans_in_vc = _human_members(vc.channel)
        if len(humans_in_vc) == 0:
            try:
                vc.stop_listening()
            except Exception:
                pass
            await vc.disconnect()
            _voice_sink = None
            print("Left VC: no humans remaining.")
            await _send_to_notify_channel(
                member.guild.id,
                content="👋 Left the voice channel (everyone left).",
            )


# ---------------------------------------------------------------------------
# On message: mentions, replies, quotes ingestion, voice join/leave triggers
# ---------------------------------------------------------------------------

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    #Live quotes ingestion
    if QUOTES_CHANNEL_ID and message.channel.id == QUOTES_CHANNEL_ID:
        for m in _QUOTE_RE.finditer(message.content):
            quote = m.group("quote").strip()
            if quote and quote not in _user_responses and quote not in DEFAULT_RESPONSES:
                _user_responses.append(quote)
                _save_user_responses()

    #Voice join/leave triggers via text message
    me = message.guild.me if message.guild else bot.user
    is_mention = me in message.mentions
    is_reply = (
        message.reference is not None
        and message.reference.resolved is not None
        and getattr(message.reference.resolved, "author", None) == me
    )

    if is_mention or is_reply:
        content_lower = message.content.lower()

        #Join VC trigger
        if "join vc" in content_lower or "join voice" in content_lower:
            voice = message.author.voice
            if voice is None or voice.channel is None:
                await message.reply("⚠️ You're not in a voice channel.")
                return
            existing_vc = _get_voice_client()
            if existing_vc and existing_vc.is_connected():
                await message.reply("I'm already in a voice channel.")
                return
            try:
                vc = await voice.channel.connect()
                await _start_listening(vc)
            except Exception as e:
                await message.reply(f"⚠️ Could not join VC: `{e}`")
            return

        #Leave VC trigger
        if "leave vc" in content_lower or "leave voice" in content_lower:
            vc = _get_voice_client()
            if vc is None or not vc.is_connected():
                await message.reply("⚠️ I'm not in a voice channel.")
                return
            try:
                vc.stop_listening()
            except Exception:
                pass
            await vc.disconnect()
            _voice_sink = None
            await message.reply("👋 Left the voice channel.")
            return

    #Smart response only when mentioned or replied to
    if not (is_mention or is_reply):
        return

    pool = _full_pool()
    if not pool:
        return
    if _smart_mode:
        text = _select_response(message.content, pool)
    else:
        text = random.choice(pool)
    text = _resolve_emojis(text, message.guild)
    await message.reply(text)


# ---------------------------------------------------------------------------
# Smart response selection (sentiment + keyword scoring)
# ---------------------------------------------------------------------------

_SENT_POS = frozenset({
    "love", "great", "awesome", "good", "best", "amazing", "happy", "nice",
    "cool", "funny", "perfect", "thanks", "thank", "hype", "hyped", "sick",
    "fire", "based", "win", "wins", "gg", "beautiful", "incredible",
    "goated", "goat", "slaps", "bussin", "peak", "slay", "pog", "epic",
    "clean", "cracked", "wild", "dub", "chad", "clutch", "unreal",
    "stellar", "masterpiece", "legend", "fantastic", "brilliant",
    "wholesome", "vibe", "w",
})
_SENT_NEG = frozenset({
    "hate", "trash", "bad", "worst", "terrible", "awful", "sad", "stupid",
    "lame", "boring", "broken", "sucks", "suck", "dumb", "cringe", "mid",
    "dead", "kill", "kys", "shit", "garbage", "loss", "rip", "dogshit",
    "cooked", "yikes", "cursed", "oof", "brutal", "painful", "rough",
    "embarrassing", "pathetic", "disaster", "tragic", "cancer", "lag",
    "crash", "stink", "smelly", "ratio", "booty", "bummer", "l",
})
_NEGATORS = frozenset({
    "not", "no", "never", "don't", "doesn't", "didn't", "won't", "can't",
    "isn't", "wasn't", "aren't", "wouldn't", "couldn't", "shouldn't",
    "ain't", "aint", "nor", "barely", "hardly", "rarely",
})
_INTENSIFIERS = frozenset({
    "very", "really", "so", "super", "extremely", "absolutely", "totally",
    "completely", "hella", "insanely", "utterly", "highly", "truly",
    "genuinely", "literally", "actually",
})
_STOPWORDS = frozenset({
    "the", "a", "an", "is", "are", "was", "were", "be", "to", "of", "in",
    "on", "at", "it", "this", "that", "and", "or", "but", "do", "does",
    "did", "you", "i", "me", "my", "we", "they", "he", "she", "for", "with",
    "so", "if", "can", "will", "would", "should", "have", "has", "not",
    "no", "yes", "just", "like", "get", "got", "what", "how", "about",
})

_WH_WORDS = frozenset({"who", "what", "when", "where", "why", "how", "which", "whose"})

_YN_ANSWERS = {
    "yes":     ["yes", "yeah", "yep", "obviously", "100%", "for sure"],
    "no":      ["no", "nope", "nah", "absolutely not", "hell no"],
    "maybe":   ["maybe", "perhaps", "possibly", "could go either way"],
    "idk":     ["i don't know", "idk", "no idea", "beats me", "who knows"],
}


def _sentiment(text: str) -> float:
    words = re.findall(r"[a-z']+", text.lower())
    if not words:
        return 0.0
    pos = 0.0
    neg = 0.0
    for i, w in enumerate(words):
        prev = words[i - 1] if i > 0 else ""
        prev2 = words[i - 2] if i > 1 else ""
        is_pos = any(w == p if len(p) <= 2 else w.startswith(p) for p in _SENT_POS)
        is_neg = any(w == p if len(p) <= 2 else w.startswith(p) for p in _SENT_NEG)
        if not (is_pos or is_neg):
            continue
        weight = 2.0 if prev in _INTENSIFIERS else 1.0
        negated = prev in _NEGATORS or prev2 in _NEGATORS
        if negated:
            is_pos, is_neg = is_neg, is_pos
        if is_pos:
            pos += weight
        elif is_neg:
            neg += weight
    return math.tanh(pos - neg)


def _stem(word: str) -> str:
    for suffix in ("ing", "ed", "es", "s"):
        if len(word) > len(suffix) + 2 and word.endswith(suffix):
            return word[: -len(suffix)]
    return word


def _keywords(text: str) -> set[str]:
    return {
        _stem(w)
        for w in re.findall(r"[a-z']+", text.lower())
        if len(w) > 1 and w not in _STOPWORDS
    }


def _is_yn_question(text: str) -> bool:
    stripped = text.strip()
    if not stripped.endswith("?"):
        return False
    first = re.findall(r"[a-z]+", stripped.lower())
    return not (first and first[0] in _WH_WORDS)


def _select_response(message_text: str, pool: list[str]) -> str:
    if not pool:
        return ""

    if _is_yn_question(message_text):
        mood = _sentiment(message_text)
        if mood > 0.3:
            bucket = random.choice(["yes", "yes", "maybe"])
        elif mood < -0.3:
            bucket = random.choice(["no", "no", "idk"])
        else:
            bucket = random.choice(["yes", "no", "maybe", "idk"])
        return random.choice(_YN_ANSWERS[bucket])

    msg_sent = _sentiment(message_text)
    msg_kw = _keywords(message_text)

    scored: list[tuple[str, float]] = []
    for resp in pool:
        r_sent = _sentiment(resp)
        r_kw = _keywords(resp)

        sent_dist = abs(msg_sent - r_sent)
        sent_score = (1.0 - sent_dist * sent_dist) * 0.5

        if msg_kw and r_kw:
            overlap = len(msg_kw & r_kw)
            kw_score = min(overlap / 3.0, 1.0) * 0.5
        else:
            kw_score = 0.0

        total = sent_score + kw_score
        scored.append((resp, total))

    TEMP = 12.0
    weights = [math.exp(s * TEMP) for _, s in scored]
    total_w = sum(weights)
    if total_w == 0:
        return random.choice(pool)
    r = random.random() * total_w
    cumulative = 0.0
    for (resp, _), w in zip(scored, weights):
        cumulative += w
        if r <= cumulative:
            return resp
    return scored[-1][0]


# ---------------------------------------------------------------------------
# Lunkflix status to bot rich presence
# ---------------------------------------------------------------------------

async def _lunkflix_presence():
    prev_state = None
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            data = await _fetch_json("/api/servers/jellyfin")
            cur_state = "running" if data.get("status") == "running" else "stopped"
        except Exception:
            cur_state = "unknown"
        if cur_state != prev_state:
            if cur_state == "running":
                await bot.change_presence(
                    status=discord.Status.online,
                    activity=discord.Activity(
                        type=discord.ActivityType.watching,
                        name="Lunkflix 🟢",
                    ),
                )
            else:
                await bot.change_presence(
                    status=discord.Status.do_not_disturb,
                    activity=discord.Activity(
                        type=discord.ActivityType.watching,
                        name=f"Lunkflix 🔴 ({cur_state})",
                    ),
                )
            prev_state = cur_state
        await asyncio.sleep(180)


def main():
    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()