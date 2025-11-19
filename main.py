#!/usr/bin/env python3
import os
import time
import json
import logging
from gspread.utils import a1_to_rowcol
import gspread
from google.oauth2.service_account import Credentials
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled, CouldNotRetrieveTranscript
import subprocess, glob, html, shlex

# ---------- Configuration (from env variables)
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "service_account.json")
YT_API_KEY = os.environ.get("YT_API_KEY")  # optional - used for metadata via Data API
COOKIES_FILE = os.environ.get("COOKIES_FILE")  # optional path to cookies.txt if provided
PROXY = os.environ.get("HTTP_PROXY") or os.environ.get("HTTPS_PROXY")  # optional proxy string (http://user:pass@host:port)
SOURCE_COLUMN = os.environ.get("SOURCE_COLUMN", "A")
TARGET_COLUMN = os.environ.get("TARGET_COLUMN", "B")
RAW_TRANSCRIPT_COLUMN = os.environ.get("RAW_TRANSCRIPT_COLUMN", "C")
DIAG_COLUMN = os.environ.get("DIAG_COLUMN", None)  # e.g. "Z" or None
HEADER_ROWS = int(os.environ.get("HEADER_ROWS", "1"))
INDIAN_LANG_CODES = os.environ.get("INDIAN_LANG_CODES", "en,hi,bn,te,mr,ta,gu,kn,ml,pa,or,as,ur").split(",")
MAX_CHARS = int(os.environ.get("MAX_CHARS", "30000"))
SLEEP_BETWEEN_CALLS = float(os.environ.get("SLEEP_BETWEEN_CALLS", "0.5"))

# ---------- Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------- Helper functions (safe join + simple yt-dlp wrapper + minimal metadata via YouTube Data API)
def _ensure_text(x):
    if x is None: return ""
    if isinstance(x, str): return x
    if isinstance(x, (list, tuple)): return " ".join(_ensure_text(e) for e in x)
    if isinstance(x, dict):
        if "text" in x: return _ensure_text(x["text"])
        return str(x)
    return str(x)

def safe_join_segments(segs):
    if not segs: return ""
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

# You should paste your clean_transcript_nlp implementation here or import from helpers
# For brevity, here's a minimal fallback:
def clean_transcript_nlp(source):
    # If you added the full NLP in helpers, import and call that here.
    # Minimal fallback: join segments or return text with small fixes.
    if isinstance(source, list):
        raw = safe_join_segments(source)
    else:
        raw = _ensure_text(source)
    raw = raw.replace("\n", " ").strip()
    # simple sentence separation: put periods between segments if missing
    raw = raw.replace("  ", " ")
    return raw

# yt-dlp helper (calls system yt-dlp)
YT_DLP_OUTDIR = "/tmp/yt_dlp_subs"
os.makedirs(YT_DLP_OUTDIR, exist_ok=True)

def yt_dlp_fetch_subtitles(video_url, lang="en", use_auto=True, cookies_file=None, proxy=None, out_dir=YT_DLP_OUTDIR, timeout=120):
    sub_flags = ["--skip-download", "--output", out_dir + "/%(id)s.%(ext)s"]
    sub_flags += ["--sub-lang", lang]
    sub_flags += ["--no-progress"]
    sub_flags += ["--no-warnings"]
    if use_auto:
        sub_flags += ["--write-auto-sub"]
    else:
        sub_flags += ["--write-sub"]
    if cookies_file:
        sub_flags += ["--cookies", cookies_file]
    if proxy:
        sub_flags += ["--proxy", proxy]
    cmd = ["yt-dlp"] + sub_flags + [video_url]
    try:
        logger.info("Running yt-dlp: %s", shlex.join(cmd))
    except Exception:
        logger.info("Running yt-dlp: %s", " ".join(cmd))
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout)
    diag = proc.stdout + "\n" + proc.stderr
    # find file
    vid = video_url.split("watch?v=")[-1].split("&")[0] if "watch?v=" in video_url else video_url.rsplit("/", 1)[-1]
    candidates = glob.glob(out_dir + "/" + vid + ".*") + glob.glob(out_dir + "/*.*")
    for c in candidates:
        if c.lower().endswith((".vtt", ".srt", ".txt")):
            with open(c, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            # basic cleanup for vtt
            if c.lower().endswith(".vtt") or content.strip().upper().startswith("WEBVTT"):
                lines = []
                for line in content.splitlines():
                    if "-->" in line or line.strip().upper().startswith("WEBVTT") or line.strip().isdigit():
                        continue
                    lines.append(line)
                return ("\n".join(lines)).strip(), diag
            return content.strip(), diag
    return None, diag

# minimal metadata fetch using googleapiclient if YT_API_KEY provided
youtube = None
if YT_API_KEY:
    try:
        from googleapiclient.discovery import build
        youtube = build("youtube", "v3", developerKey=YT_API_KEY)
    except Exception as e:
        logger.warning("Failed to create YouTube Data client: %s", e)
def fetch_metadata(video_id):
    if not youtube:
        return {}
    try:
        resp = youtube.videos().list(part="snippet,contentDetails,statistics", id=video_id).execute()
        items = resp.get("items", [])
        if not items:
            return {}
        i = items[0]
        snippet = i.get("snippet", {})
        cd = i.get("contentDetails", {})
        stats = i.get("statistics", {})
        return {
            "author": snippet.get("channelTitle", ""),
            "duration": cd.get("duration", ""),
            "description": snippet.get("description", "")[:5000],
            "likes": stats.get("likeCount", "N/A"),
            "comment_count": stats.get("commentCount", "0")
        }
    except Exception as e:
        logger.warning("metadata error for %s: %s", video_id, e)
        return {}

# ---------- Connect to Google Sheets
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID).get_worksheet(0)
logger.info("Connected to spreadsheet: %s, worksheet: %s", SPREADSHEET_ID, sh.title)

# ---------- Main loop: read rows, process only if target empty
src_idx = a1_to_rowcol(f"{SOURCE_COLUMN}1")[1]
trg_idx = a1_to_rowcol(f"{TARGET_COLUMN}1")[1]
raw_idx = a1_to_rowcol(f"{RAW_TRANSCRIPT_COLUMN}1")[1]
if DIAG_COLUMN:
    diag_idx = a1_to_rowcol(f"{DIAG_COLUMN}1")[1]
else:
    diag_idx = None

rows = sh.get_all_values()
nrows = len(rows)
logger.info("Found %d rows", nrows)
cells_to_write = []
for r in range(HEADER_ROWS, nrows):
    rownum = r + 1
    row = rows[r]
    url = row[src_idx-1].strip() if len(row) >= src_idx and row[src_idx-1] else ""
    existing_target = row[trg_idx-1].strip() if len(row) >= trg_idx and row[trg_idx-1] else ""
    if not url: continue
    if existing_target:
        logger.info("Row %d: already has transcript, skipping", rownum)
        continue

    # extract video id (reuse your extractor; minimal here: last 11 char or watch?v=)
    vid = None
    if "watch?v=" in url:
        vid = url.split("watch?v=")[-1].split("&")[0]
    else:
        vid = url.rstrip("/").split("/")[-1]
    diag_notes = ""
    transcript_raw = ""
    transcript_clean = ""

    # try youtube-transcript-api
    try:
        tl = YouTubeTranscriptApi.list_transcripts(vid)
        try:
            track = tl.find_transcript(INDIAN_LANG_CODES)
        except Exception:
            track = next(iter(tl))
        segs = track.fetch()
        transcript_raw = safe_join_segments(segs)
        # clean with NLP (if you included the heavy pipeline, call it here)
        transcript_clean = clean_transcript_nlp(segs)
    except TranscriptsDisabled:
        transcript_clean = "Transcripts disabled"
        diag_notes += "transcripts_disabled; "
    except (NoTranscriptFound, CouldNotRetrieveTranscript) as e:
        diag_notes += f"ytapi_err:{e}; "
    except Exception as e:
        diag_notes += f"ytapi_unexp:{e}; "

    # fallback to yt-dlp if needed
    if not transcript_clean or "No transcript" in diag_notes or "disabled" in transcript_clean.lower():
        try:
            url_watch = f"https://www.youtube.com/watch?v={vid}"
            subs, diag = yt_dlp_fetch_subtitles(url_watch, lang="en", use_auto=True, cookies_file=COOKIES_FILE, proxy=PROXY)
            diag_notes += "yt-dlp-auto: " + (diag[:400] if diag else "") + "; "
            if subs:
                transcript_raw = subs
                transcript_clean = clean_transcript_nlp(subs)
            else:
                subs2, diag2 = yt_dlp_fetch_subtitles(url_watch, lang="en", use_auto=False, cookies_file=COOKIES_FILE, proxy=PROXY)
                diag_notes += "yt-dlp-uploaded: " + (diag2[:400] if diag2 else "") + "; "
                if subs2:
                    transcript_raw = subs2
                    transcript_clean = clean_transcript_nlp(subs2)
        except Exception as e:
            diag_notes += f"yt-dlp-exc:{e}; "

    if not transcript_clean:
        transcript_clean = "Could not retrieve transcript. See diag."

    # fetch metadata
    meta = fetch_metadata(vid)
    # Build cells
    cells_to_write.append(gspread.models.Cell(rownum, trg_idx, transcript_clean[:MAX_CHARS]))
    cells_to_write.append(gspread.models.Cell(rownum, raw_idx, transcript_raw[:MAX_CHARS]))
    # write metadata if needed (you can add more columns)
    if meta:
        # add more cell appends mapping your configured columns
        pass
    if diag_idx:
        cells_to_write.append(gspread.models.Cell(rownum, diag_idx, diag_notes[:4000]))

    time.sleep(SLEEP_BETWEEN_CALLS)

# Batch write
if cells_to_write:
    sh.update_cells(cells_to_write)
    logger.info("Wrote %d cells", len(cells_to_write))
else:
    logger.info("No updates to write")
