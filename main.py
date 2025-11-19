#!/usr/bin/env python3
"""
main.py

Fetch YouTube transcripts + metadata, clean transcripts with NLP, and write to Google Sheet.

Configuration via environment variables:
- SPREADSHEET_ID (required)
- SERVICE_ACCOUNT_FILE (path to service account JSON in runner; required)
- YT_API_KEY (optional; for YouTube Data API metadata)
- COOKIES_FILE (optional; path to cookies.txt for signed-in access)
- PROXY (optional; http://user:pass@host:port)
- SOURCE_COLUMN (default A)
- TARGET_COLUMN (clean transcript) (default B)
- RAW_TRANSCRIPT_COLUMN (default C)
- DIAG_COLUMN (optional)
- HEADER_ROWS (default 1)
- INDIAN_LANG_CODES (comma-separated, default: en,hi,bn,te,mr,ta,gu,kn,ml,pa,or,as,ur)
- MAX_CHARS (default 30000)
- SLEEP_BETWEEN_CALLS (float, default 0.5)
- CHUNK_UPDATE_SIZE (int, default 800) -> how many cells per update batch
"""

import os
import time
import re
import json
import glob
import html
import shlex
import logging
import subprocess
from typing import List, Optional, Tuple, Dict, Any

# Third-party imports (installed via requirements.txt)
try:
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread.utils import a1_to_rowcol
    from youtube_transcript_api import (
        YouTubeTranscriptApi,
        NoTranscriptFound,
        TranscriptsDisabled,
        CouldNotRetrieveTranscript,
    )
except Exception as e:
    raise SystemExit(f"Missing required packages or failed import: {e}")

# Try NLP models; fall back gracefully if not present
PUNCT_MODEL = None
SPACY_NLP = None
PUNCT_AVAILABLE = False
SPACY_AVAILABLE = False
try:
    from deepmultilingualpunctuation import PunctuationModel

    try:
        PUNCT_MODEL = PunctuationModel()
        PUNCT_AVAILABLE = True
        logging.info("PunctuationModel loaded.")
    except Exception as e:
        logging.warning("PunctuationModel failed to load: %s", e)
except Exception:
    logging.info("deepmultilingualpunctuation not installed; punctuation restoration disabled.")

try:
    import spacy

    try:
        SPACY_NLP = spacy.load("en_core_web_sm")
        SPACY_AVAILABLE = True
        # Optionally disable heavy pipes for speed
        for pipe in list(SPACY_NLP.pipe_names):
            if pipe not in ("senter", "sentencizer", "parser", "tagger"):
                try:
                    SPACY_NLP.disable_pipe(pipe)
                except Exception:
                    pass
        logging.info("spaCy loaded.")
    except Exception as e:
        logging.warning("spaCy load failed: %s", e)
except Exception:
    logging.info("spaCy not available.")


# -------------------------
# Configuration from env
# -------------------------
def getenv(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    if v is None:
        return default
    return v


SPREADSHEET_ID = getenv("SPREADSHEET_ID")
SERVICE_ACCOUNT_FILE = getenv("SERVICE_ACCOUNT_FILE", "service_account.json")
YT_API_KEY = getenv("YT_API_KEY")
COOKIES_FILE = getenv("COOKIES_FILE")  # path to cookies.txt if provided
PROXY = getenv("PROXY") or getenv("HTTP_PROXY") or getenv("HTTPS_PROXY")

SOURCE_COLUMN = getenv("SOURCE_COLUMN", "A")
TARGET_COLUMN = getenv("TARGET_COLUMN", "B")
RAW_TRANSCRIPT_COLUMN = getenv("RAW_TRANSCRIPT_COLUMN", "C")
DIAG_COLUMN = getenv("DIAG_COLUMN")  # optional
HEADER_ROWS = int(getenv("HEADER_ROWS", "1"))
INDIAN_LANG_CODES = getenv("INDIAN_LANG_CODES", "en,hi,bn,te,mr,ta,gu,kn,ml,pa,or,as,ur").split(",")
MAX_CHARS = int(getenv("MAX_CHARS", "30000"))
SLEEP_BETWEEN_CALLS = float(getenv("SLEEP_BETWEEN_CALLS", "0.5"))
CHUNK_UPDATE_SIZE = int(getenv("CHUNK_UPDATE_SIZE", "800"))

YT_DLP_OUTDIR = getenv("YT_DLP_OUTDIR", "/tmp/yt_dlp_subs")
os.makedirs(YT_DLP_OUTDIR, exist_ok=True)

# Logging config
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("yt_transcript_worker")

# Basic validation
if not SPREADSHEET_ID:
    raise SystemExit("SPREADSHEET_ID environment variable is required.")


# -------------------------
# Utilities
# -------------------------
def _ensure_text(x: Any) -> str:
    """Convert many types to a safe string (handles nested lists/dicts)."""
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    if isinstance(x, (list, tuple)):
        return " ".join(_ensure_text(e) for e in x)
    if isinstance(x, dict):
        if "text" in x:
            return _ensure_text(x["text"])
        # fallback: stringify dict
        return json.dumps(x, ensure_ascii=False)
    return str(x)


def safe_join_segments(segs: List[Any]) -> str:
    """Robustly join segments that may be dicts, strings, lists."""
    if not segs:
        return ""
    parts = []
    for s in segs:
        if isinstance(s, dict):
            t = s.get("text", "")
        else:
            t = s
        t = _ensure_text(t).strip()
        if t:
            parts.append(t.replace("\n", " "))
    return " ".join(parts)


def chunked(iterable: List[Any], size: int):
    """Yield successive chunks from iterable."""
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


# -------------------------
# NLP cleaning pipeline
# -------------------------
_SENTENCE_END = re.compile(r"[.!?]$")


def _strip_vtt_srt_artifacts(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"^\s*WEBVTT[^\n]*\n", "", text, flags=re.IGNORECASE)
    # remove common timestamp patterns
    text = re.sub(r"\d{1,2}:\d{2}:\d{2}\.\d{3}\s*-->\s*\d{1,2}:\d{2}:\d{2}\.\d{3}", " ", text)
    text = re.sub(r"\d{1,2}:\d{2}\.\d{3}\s*-->\s*\d{1,2}:\d{2}\.\d{3}", " ", text)
    text = re.sub(r"^\s*\d+\s*$", "", text, flags=re.MULTILINE)
    text = html.unescape(text)
    return text


def _join_segments_for_nlp(segments: List[dict]) -> str:
    parts = []
    for seg in segments:
        if not seg:
            continue
        t = seg.get("text") if isinstance(seg, dict) else str(seg)
        if t:
            t = t.replace("\n", " ").strip()
            parts.append(t)
    return " ".join(parts)


def restore_punctuation_and_case(raw_text: str) -> str:
    """Use punctuation model if available; otherwise return raw_text."""
    if not raw_text:
        return ""
    if not PUNCT_AVAILABLE:
        return raw_text
    # chunk to avoid OOM / long processing
    max_chunk = 2000
    overlap = 50
    chunks = []
    i = 0
    n = len(raw_text)
    while i < n:
        end = min(i + max_chunk, n)
        chunk = raw_text[i:end]
        # add overlap for context
        if i != 0:
            start = max(0, i - overlap)
            chunk = raw_text[start:end]
        if end < n:
            chunk = raw_text[i : end + overlap]
        chunks.append(chunk)
        i = end
    restored_chunks = []
    for c in chunks:
        try:
            out = PUNCT_MODEL.restore_punctuation(c)
            restored_chunks.append(out)
        except Exception as e:
            logger.debug("punctuator chunk failed: %s", e)
            restored_chunks.append(c)
    restored = " ".join(restored_chunks)
    restored = re.sub(r"\s+", " ", restored).strip()
    return restored


def sentence_segment_and_capitalize(text: str) -> str:
    if not text:
        return ""
    if SPACY_AVAILABLE and SPACY_NLP:
        doc = SPACY_NLP(text)
        sents = [s.text.strip() for s in doc.sents if s.text.strip()]
    else:
        sents = re.split(r"(?<=[\.\?\!])\s+", text)
        sents = [s.strip() for s in sents if s.strip()]
    cap_sents = []
    for s in sents:
        idx = re.search(r"[A-Za-z0-9]", s)
        if idx:
            i = idx.start()
            s = s[:i] + s[i].upper() + s[i + 1 :]
        cap_sents.append(s)
    return " ".join(cap_sents)


def clean_transcript_nlp(source: Any) -> str:
    """
    Full cleaning:
      - accepts list of segments or raw string
      - strips vtt/srt artifacts
      - restores punctuation (if model available)
      - sentence segmentation and capitalization via spaCy
    """
    if isinstance(source, list):
        raw = _join_segments_for_nlp(source)
    else:
        raw = _ensure_text(source)
    raw = _strip_vtt_srt_artifacts(raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    try:
        puncted = restore_punctuation_and_case(raw)
    except Exception:
        puncted = raw
    final = sentence_segment_and_capitalize(puncted)
    final = re.sub(r"\s+([,.;:!?])", r"\1", final)
    final = re.sub(r"([,.;:!?])([^\s])", r"\1 \2", final)
    final = re.sub(r"\s+", " ", final).strip()
    return final


# -------------------------
# yt-dlp helper (subprocess)
# -------------------------
def yt_dlp_fetch_subtitles(
    video_url: str,
    lang: str = "en",
    use_auto: bool = True,
    cookies_file: Optional[str] = None,
    proxy: Optional[str] = None,
    out_dir: str = YT_DLP_OUTDIR,
    timeout: int = 120,
) -> Tuple[Optional[str], str]:
    """
    Returns (subtitle_text or None, diagnostics stdout+stderr)
    """
    os.makedirs(out_dir, exist_ok=True)
    sub_flags = ["--skip-download", "--output", f"{out_dir}/%(id)s.%(ext)s"]
    if use_auto:
        sub_flags += ["--write-auto-sub"]
    else:
        sub_flags += ["--write-sub"]
    sub_flags += ["--sub-lang", lang]
    if cookies_file:
        sub_flags += ["--cookies", cookies_file]
    if proxy:
        sub_flags += ["--proxy", proxy]
    # quiet but capture output
    cmd = ["yt-dlp"] + sub_flags + [video_url]
    try:
        logger.debug("Running yt-dlp: %s", shlex.join(cmd))
    except Exception:
        logger.debug("Running yt-dlp: %s", " ".join(cmd))
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)
        diag = proc.stdout + "\n" + proc.stderr
    except Exception as e:
        return None, f"yt-dlp-run-exception:{e}"
    # determine id
    vid = None
    if "watch?v=" in video_url:
        vid = video_url.split("watch?v=")[-1].split("&")[0]
    else:
        vid = video_url.rstrip("/").split("/")[-1]
    candidates = []
    if vid:
        candidates = glob.glob(os.path.join(out_dir, f"{vid}.*"))
    if not candidates:
        candidates = sorted(glob.glob(os.path.join(out_dir, "*.*")), key=os.path.getmtime, reverse=True)[:6]
    txt = None
    for c in candidates:
        if c.lower().endswith((".vtt", ".srt", ".txt", ".webvtt")):
            try:
                with open(c, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
            except Exception:
                continue
            if c.lower().endswith(".vtt") or content.strip().upper().startswith("WEBVTT"):
                lines = []
                for line in content.splitlines():
                    if line.strip().upper().startswith("WEBVTT"):
                        continue
                    if "-->" in line:
                        continue
                    if line.strip().isdigit():
                        continue
                    lines.append(line)
                txt = "\n".join(lines).strip()
            else:
                txt = content.strip()
            if txt:
                txt = html.unescape(txt)
                break
    return txt, diag


# -------------------------
# YouTube Data API metadata fetch
# -------------------------
YOUTUBE_CLIENT = None
if YT_API_KEY:
    try:
        from googleapiclient.discovery import build

        YOUTUBE_CLIENT = build("youtube", "v3", developerKey=YT_API_KEY)
    except Exception as e:
        logger.warning("Could not create YouTube Data API client: %s", e)
        YOUTUBE_CLIENT = None


def parse_iso8601_duration(iso_duration: str) -> str:
    if not iso_duration:
        return ""
    m = re.match(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$", iso_duration)
    if not m:
        return iso_duration
    hours = int(m.group(1) or 0)
    minutes = int(m.group(2) or 0)
    seconds = int(m.group(3) or 0)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def fetch_video_metadata(video_id: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "author": "",
        "duration": "",
        "description": "",
        "likes": "",
        "comment_count": "",
        "top_comment": "",
    }
    if not YOUTUBE_CLIENT:
        return result
    try:
        resp = YOUTUBE_CLIENT.videos().list(part="snippet,contentDetails,statistics", id=video_id).execute()
        items = resp.get("items", [])
        if not items:
            return result
        item = items[0]
        snippet = item.get("snippet", {})
        content = item.get("contentDetails", {})
        stats = item.get("statistics", {})
        result["author"] = snippet.get("channelTitle", "") or ""
        iso_dur = content.get("duration")
        result["duration"] = pars
