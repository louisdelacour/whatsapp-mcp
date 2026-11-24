"""Local transcription of WhatsApp voice messages with whisper.cpp.

Nothing leaves the Mac: the bridge downloads the audio, ffmpeg turns it into 16 kHz mono WAV,
whisper-cli transcribes it with the local model, and the text is kept in store/transcripts.db
so the same voice message is never transcribed twice.
"""

import os
import shutil
import sqlite3
import subprocess
import tempfile
from typing import Any, Dict

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
TRANSCRIPTS_DB_PATH = os.path.join(ROOT, "whatsapp-bridge", "store", "transcripts.db")
MODEL_PATH = os.environ.get("WHISPER_MODEL", os.path.join(ROOT, "models", "ggml-large-v3-turbo.bin"))
LANGUAGE = os.environ.get("WHISPER_LANGUAGE", "auto")


def _tool(name: str) -> str:
    """Find a command even when the MCP client starts us without Homebrew on PATH."""
    found = shutil.which(name) or next(
        (p for p in (f"/opt/homebrew/bin/{name}", f"/usr/local/bin/{name}") if os.path.exists(p)), None
    )
    if not found:
        raise RuntimeError(f"{name} not found (brew install {'whisper-cpp' if name == 'whisper-cli' else name})")
    return found


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(TRANSCRIPTS_DB_PATH)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS transcripts (id TEXT, chat_jid TEXT, text TEXT, model TEXT, PRIMARY KEY (id, chat_jid))"
    )
    return conn


def cached_transcript(message_id: str, chat_jid: str) -> str | None:
    if not os.path.exists(TRANSCRIPTS_DB_PATH):
        return None
    with _db() as conn:
        row = conn.execute(
            "SELECT text FROM transcripts WHERE id = ? AND chat_jid = ?", (message_id, chat_jid)
        ).fetchone()
    return row[0] if row else None


def transcribe_file(audio_path: str) -> str:
    if not os.path.exists(MODEL_PATH):
        raise RuntimeError(f"Whisper model not found at {MODEL_PATH}")
    with tempfile.TemporaryDirectory() as tmp:
        wav = os.path.join(tmp, "audio.wav")
        subprocess.run(
            [_tool("ffmpeg"), "-nostdin", "-loglevel", "error", "-y", "-i", audio_path, "-ar", "16000", "-ac", "1", "-c:a", "pcm_s16le", wav],
            check=True,
            capture_output=True,
            timeout=300,
        )
        result = subprocess.run(
            [_tool("whisper-cli"), "-m", MODEL_PATH, "-f", wav, "-l", LANGUAGE, "-nt", "-np"],
            check=True,
            capture_output=True,
            text=True,
            timeout=900,
        )
    return " ".join(line.strip() for line in result.stdout.splitlines() if line.strip())


def transcribe_message(message_id: str, chat_jid: str, audio_path: str) -> Dict[str, Any]:
    cached = cached_transcript(message_id, chat_jid)
    if cached is not None:
        return {"success": True, "text": cached, "cached": True}
    text = transcribe_file(audio_path)
    with _db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO transcripts (id, chat_jid, text, model) VALUES (?, ?, ?, ?)",
            (message_id, chat_jid, text, os.path.basename(MODEL_PATH)),
        )
    return {"success": True, "text": text, "cached": False}
