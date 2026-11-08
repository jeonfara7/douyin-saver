"""
DigitCuan Downloader — TikTok, Douyin, YouTube, Instagram & Facebook (v3).

Fitur:
- TikTok: video tanpa watermark, video watermark, MP3, foto slideshow.
  Resolver via API publik tikwm.com (terverifikasi 22 Okt 2026).
- Douyin: video tanpa watermark (resolver _ROUTER_DATA + ttwid, sama seperti
  v1), foto slideshow (postingan /note/), MP3 diekstrak dari video pakai
  ffmpeg (imageio-ffmpeg) karena data share Douyin tidak menyediakan
  tautan audio terpisah.
- YouTube, Instagram, Facebook: resolver via yt-dlp (tanpa login, konten
  publik saja). Video MP4 gabungan (YouTube: 720p bila tersedia, kalau tidak
  360p) + MP3 (audio-only bila ada, kalau tidak diekstrak dari video via
  ffmpeg). Catatan jujur: YouTube/Instagram kadang membatasi akses tanpa
  login (429 / SABR / API tidak memberi akses) — saat itu server mengembalikan
  pesan gagal yang jelas, bukan file kosong.

Unduhan file dialirkan lewat /download (proxy) agar browser menyimpan
sebagai file. MP3 lewat /mp3 (ekstraksi server-side, batas sumber 70 MB).
"""
import os

# Env sandbox tertentu punya no_proxy berisi IPv6 bracket yang membuat
# parser URL httpx crash saat client dibuat. Sanitasi lebih dulu.
os.environ["no_proxy"] = "localhost,127.0.0.1"
os.environ["NO_PROXY"] = "localhost,127.0.0.1"

import asyncio
import json
import re
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.parse import quote, urlparse

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from starlette.background import BackgroundTask

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="DigitCuan Downloader — TikTok, Douyin, YouTube, Instagram & Facebook", version="3.0.0")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

SITE_URL = "https://douyin.digitcuan.site"

UA_MOBILE = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"
)
UA_PC = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
TIMEOUT = httpx.Timeout(connect=10.0, read=45.0, write=10.0, pool=10.0)
DOWNLOAD_TIMEOUT = httpx.Timeout(connect=10.0, read=120.0, write=10.0, pool=10.0)

# Host CDN yang boleh di-proxy untuk unduhan (video/audio/gambar).
DOWNLOAD_HOST_SUFFIXES = (
    # Douyin / ByteDance
    "snssdk.com", "douyinvod.com", "douyin.com", "amemv.com", "bytecdn.cn",
    "douyinpic.com", "ibyteimg.com", "bytedance.com",
    # TikTok
    "tiktokcdn.com", "tiktokcdn-us.com", "tiktok.com", "musical.ly",
    # YouTube (yt-dlp)
    "googlevideo.com",
    # Instagram / Facebook (yt-dlp)
    "fbcdn.net", "cdninstagram.com",
)

MAX_MP3_SOURCE_BYTES = 70 * 1024 * 1024  # 70 MB — batas sumber ekstraksi MP3


class URLRequest(BaseModel):
    url: str


# ---------------------------------------------------------------- ttwid ----
_ttwid_cache: dict = {"value": None, "ts": 0.0}
TTWID_TTL = 24 * 3600


async def get_ttwid(client: httpx.AsyncClient, force: bool = False) -> str | None:
    """Cookie ttwid = 'tiket tamu' agar Douyin mau menyajikan data video."""
    now = time.time()
    if not force and _ttwid_cache["value"] and now - _ttwid_cache["ts"] < TTWID_TTL:
        return _ttwid_cache["value"]
    payload = {
        "region": "cn", "aid": 6383, "needFid": False, "service": "www.douyin.com",
        "migrate_info": {"ticket": "", "source": "node"},
        "cbUrlProtocol": "https", "union": True,
    }
    for _ in range(2):
        try:
            r = await client.post(
                "https://ttwid.bytedance.com/ttwid/union/register/",
                json=payload,
                headers={"User-Agent": UA_PC, "Content-Type": "application/json"},
            )
            ttwid = r.cookies.get("ttwid")
            if ttwid:
                _ttwid_cache.update(value=ttwid, ts=now)
                return ttwid
        except httpx.HTTPError:
            continue
    return _ttwid_cache["value"]


# -------------------------------------------------------------- resolver ----
def extract_url(text: str) -> str | None:
    m = re.search(r"https?://[^\s\"'<>]+", text.strip())
    if not m:
        return None
    return m.group(0).rstrip(").,，。)】")


def detect_platform(url: str) -> str | None:
    host = urlparse(url).netloc.lower()
    if "douyin" in host:
        return "douyin"
    if "tiktok" in host:
        return "tiktok"
    if "youtube" in host or "youtu.be" in host:
        return "youtube"
    if "instagram" in host or "instagr.am" in host:
        return "instagram"
    if "facebook" in host or "fb.watch" in host or host == "fb.com":
        return "facebook"
    return None


def extract_aweme_id(text: str) -> str | None:
    for pat in (r"/video/(\d+)", r"/note/(\d+)", r"modal_id=(\d+)", r"/share/(?:video|note)/(\d+)"):
        m = re.search(pat, text)
        if m:
            return m.group(1)
    return None


def parse_router_data(html: str) -> dict | None:
    idx = html.find("_ROUTER_DATA")
    if idx < 0:
        return None
    start = html.find("{", idx)
    if start < 0:
        return None
    try:
        obj, _ = json.JSONDecoder().raw_decode(html[start:])
        return obj
    except Exception:
        return None


def find_item(obj) -> dict | None:
    if isinstance(obj, dict):
        vir = obj.get("videoInfoRes")
        if isinstance(vir, dict):
            items = vir.get("item_list") or []
            if items:
                return items[0]
        for v in obj.values():
            r = find_item(v)
            if r:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = find_item(v)
            if r:
                return r
    return None


def _first_url(node) -> str:
    if isinstance(node, dict):
        lst = node.get("url_list") or []
        return lst[0] if lst else ""
    return ""


def douyin_item_to_result(item: dict, aweme_id: str) -> dict:
    video = item.get("video") or {}
    author = item.get("author") or {}
    music = item.get("music") or {}
    play_list = (video.get("play_addr") or {}).get("url_list") or []
    playwm = play_list[0] if play_list else ""
    images = []
    for img in item.get("images") or []:
        u = _first_url(img)
        if u:
            images.append(u)
    return {
        "success": True,
        "platform": "douyin",
        "type": "image" if images and not playwm else "video",
        "title": item.get("desc") or "Postingan Douyin",
        "author": author.get("nickname") or "Unknown Author",
        "avatar": _first_url(author.get("avatar_larger")),
        "cover": _first_url(video.get("cover")) or (images[0] if images else ""),
        "video_url": playwm.replace("playwm", "play") if playwm else "",
        "video_url_watermark": playwm,
        "music_url": "",  # Douyin tidak menyediakan audio terpisah — MP3 via /mp3
        "music_title": music.get("title") or "",
        "images": images,
        "aweme_id": aweme_id,
        "duration_ms": video.get("duration"),
    }


async def extract_douyin(raw_input: str) -> dict:
    share_url = extract_url(raw_input)
    if not share_url:
        raise HTTPException(status_code=400, detail="Tidak ada URL di teks yang ditempel.")
    if "douyin" not in urlparse(share_url).netloc:
        raise HTTPException(status_code=400, detail="URL ini sepertinya bukan link Douyin.")

    aweme_id = extract_aweme_id(share_url)
    is_note = "/note/" in share_url
    async with httpx.AsyncClient(follow_redirects=True, timeout=TIMEOUT) as client:
        ttwid = await get_ttwid(client)
        cookies = {"ttwid": ttwid} if ttwid else {}

        if not aweme_id:
            try:
                r = await client.get(share_url, headers={"User-Agent": UA_MOBILE}, cookies=cookies)
            except httpx.HTTPError:
                raise HTTPException(status_code=502, detail="Gagal menghubungi Douyin. Coba lagi sebentar.")
            aweme_id = extract_aweme_id(str(r.url)) or extract_aweme_id(r.text)
            if "/note/" in str(r.url):
                is_note = True
            if aweme_id:
                item = find_item(parse_router_data(r.text) or {})
                if item:
                    return douyin_item_to_result(item, aweme_id)
        if not aweme_id:
            raise HTTPException(status_code=400, detail="Tidak bisa menemukan ID postingan dari link ini. Pastikan link-nya benar.")

        paths = ["note", "video"] if is_note else ["video", "note"]
        for kind in paths:
            for attempt in range(3):
                if attempt:
                    await asyncio.sleep(2)
                try:
                    r = await client.get(
                        f"https://www.iesdouyin.com/share/{kind}/{aweme_id}/",
                        headers={"User-Agent": UA_MOBILE},
                        cookies=cookies,
                    )
                except httpx.HTTPError:
                    raise HTTPException(status_code=502, detail="Douyin tidak merespons. Coba lagi sebentar.")
                item = find_item(parse_router_data(r.text) or {})
                if item:
                    return douyin_item_to_result(item, aweme_id)
                ttwid = await get_ttwid(client, force=True)
                cookies = {"ttwid": ttwid} if ttwid else {}

        try:
            r = await client.get(
                f"https://www.iesdouyin.com/web/api/v2/aweme/iteminfo/?item_ids={aweme_id}",
                headers={"User-Agent": UA_MOBILE},
            )
            if r.status_code == 200 and r.content:
                data = r.json()
                items = data.get("item_list") or []
                if items:
                    return douyin_item_to_result(items[0], aweme_id)
        except Exception:
            pass

    raise HTTPException(
        status_code=502,
        detail="Douyin tidak memberikan data untuk link ini (mungkin postingan privat/dihapus, atau Douyin sedang membatasi akses).",
    )


async def extract_tiktok(raw_input: str) -> dict:
    share_url = extract_url(raw_input)
    if not share_url:
        raise HTTPException(status_code=400, detail="Tidak ada URL di teks yang ditempel.")
    if "tiktok" not in urlparse(share_url).netloc:
        raise HTTPException(status_code=400, detail="URL ini sepertinya bukan link TikTok.")

    api = "https://www.tikwm.com/api/?url=" + quote(share_url, safe="")
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=TIMEOUT) as client:
            r = await client.get(api, headers={"User-Agent": UA_PC})
            data = r.json()
    except Exception:
        raise HTTPException(status_code=502, detail="Layanan pengambil data TikTok sedang tidak merespons. Coba lagi sebentar.")
    if not isinstance(data, dict) or data.get("code") != 0 or not data.get("data"):
        msg = (data.get("msg") if isinstance(data, dict) else None) or "Video tidak ditemukan."
        raise HTTPException(status_code=400, detail=f"Tidak bisa memproses link TikTok ini: {msg}")

    d = data["data"]
    author = d.get("author") or {}
    music_info = d.get("music_info") or {}
    images = d.get("images") or []
    video_url = d.get("hdplay") or d.get("play") or ""
    return {
        "success": True,
        "platform": "tiktok",
        "type": "image" if images and not d.get("play") else "video",
        "title": d.get("title") or "Video TikTok",
        "author": author.get("nickname") or author.get("unique_id") or "Unknown Author",
        "avatar": author.get("avatar") or "",
        "cover": d.get("cover") or (images[0] if images else ""),
        "video_url": video_url,
        "video_url_watermark": d.get("wmplay") or "",
        "music_url": d.get("music") or "",
        "music_title": music_info.get("title") or "",
        "images": images,
        "aweme_id": str(d.get("id") or ""),
        "duration_ms": (d.get("duration") or 0) * 1000,
    }


# ------------------------------------------------- yt-dlp (YT/IG/FB) ----
YTDLP_TIMEOUT = 90.0  # detik — ekstraksi IG kadang lambat


def _ytdlp_opts(platform: str) -> dict:
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "socket_timeout": 30,
        # Pakai CA sistem, bukan bundle certifi — agar jalan di lingkungan
        # yang memakai CA egress sendiri (sandbox ini), tanpa mematikan
        # verifikasi TLS. Di Render ini = CA sistem normal.
        # (yt-dlp: prefer_system_certs hanya aktif lewat compat_opt 'no-certifi'.)
        "compat_opts": {"no-certifi"},
    }
    if platform == "youtube":
          # Client Android + Android VR terbukti mengembalikan format 18
      # (video+audio 360p) dari IP bersih — terverifikasi 8 Nov 2026
      # dengan yt-dlp 2026.10.26, TANPA PO token dan TANPA JS runtime.
      # JANGAN pakai tv_downgraded/web_embedded: tv_downgraded kena
      # bot-check, web_embedded hanya mengembalikan storyboard (tanpa
      # format video/audio). ios/tv/web/web_safari/tv_samsung juga
      # kena bot-check dari IP ini. (android_vr di tabel policy yt-dlp
      # memang “GVS PO token required”, tapi enforcement-nya selektif
      # — dari IP bersih ia jalan tanpa token, sama seperti android.)
      # Paksa IPv4 (setara CLI -4; di API Python tidak ada param
      # force_ipv4 — caranya source_address "0.0.0.0", terverifikasi
      # di yt_dlp/options.py + YoutubeDL.py:441): egress IPv6 Render
      # bisa satu blok /64 yang di-flag YouTube.
      opts["source_address"] = "0.0.0.0"
      opts["extractor_args"] = {"youtube": {"player_client": ["android", "android_vr"]}}
    return opts
      
def _pick_combined_video(formats: list) -> dict | None:
    """Pilih format video gabungan (ada audio).
    Tingkat 1: format dengan vcodec+acodec eksplisit (YouTube 22/18).
    Tingkat 2: format progresif IG/FB (vcodec/acodec None, ext mp4) — ini
    URL MP4 langsung dari API platform, audio sudah menyatu.
    """
    cands = [
        f for f in formats
        if f.get("vcodec") not in (None, "none")
        and f.get("acodec") not in (None, "none")
        and f.get("url")
    ]

    def score(f):
        fid = str(f.get("format_id") or "")
        h = f.get("height") or 0
        if fid == "22":
            return (3, h)
        if fid == "18":
            return (2, h)
        if f.get("ext") == "mp4" and h <= 720:
            return (1, h)
        return (0, h)

    cands.sort(key=score, reverse=True)
    if cands:
        return cands[0]

    prog = [
        f for f in formats
        if f.get("vcodec") in (None, "none")
        and f.get("acodec") in (None, "none")
        and f.get("ext") == "mp4"
        and f.get("url")
    ]
    if not prog:
        return None

    def prog_score(f):
        fid = str(f.get("format_id") or "")
        return {"hd": 3, "3": 2, "2": 1, "sd": 0}.get(fid, 0)

    prog.sort(key=prog_score, reverse=True)
    return prog[0]


def _pick_audio_only(formats: list) -> dict | None:
    """Pilih format audio-only — utamakan 140 (m4a AAC)."""
    cands = [
        f for f in formats
        if f.get("vcodec") in (None, "none")
        and f.get("acodec") not in (None, "none")
        and f.get("url")
    ]

    def score(f):
        fid = str(f.get("format_id") or "")
        if fid == "140":
            return 3
        if f.get("ext") == "m4a":
            return 2
        return 1

    cands.sort(key=score, reverse=True)
    return cands[0] if cands else None


def _ytdlp_extract(url: str, platform: str) -> dict:
    """Ekstraksi sinkron (dijalankan di thread) — mengembalikan dict hasil standar."""
    from yt_dlp import YoutubeDL

    with YoutubeDL(_ytdlp_opts(platform)) as ydl:
        info = ydl.extract_info(url, download=False)
    if not info:
        raise HTTPException(status_code=502, detail="Platform tidak memberikan data untuk link ini.")
    if info.get("_type") == "playlist":
        entries = [e for e in (info.get("entries") or []) if e]
        if not entries:
            raise HTTPException(status_code=400, detail="Tidak ada video di link ini.")
        info = entries[0]

    formats = info.get("formats") or []
    video = _pick_combined_video(formats)
    audio = _pick_audio_only(formats)
    ext = (info.get("ext") or "").lower()

    images: list = []
    is_image = False
    if not video and not audio and ext in ("jpg", "jpeg", "png", "webp") and info.get("url"):
        images = [info["url"]]
        is_image = True

    if not video and not audio and not is_image:
        raise HTTPException(
            status_code=502,
            detail="Tidak ditemukan file video/audio untuk link ini (mungkin konten privat/dihapus, atau platform sedang membatasi akses tanpa login).",
        )

    return {
        "success": True,
        "platform": platform,
        "type": "image" if is_image else "video",
        "title": info.get("title") or f"Video {platform.capitalize()}",
        "author": info.get("uploader") or info.get("uploader_id") or info.get("channel") or "Unknown Author",
        "avatar": "",
        "cover": info.get("thumbnail") or "",
        "video_url": (video or {}).get("url") or "",
        "video_url_watermark": "",
        "music_url": (audio or {}).get("url") or "",
        "music_title": info.get("title") or "",
        "images": images,
        "aweme_id": str(info.get("id") or ""),
        "duration_ms": int((info.get("duration") or 0) * 1000),
    }


async def extract_ytdlp(raw_input: str, platform: str) -> dict:
    share_url = extract_url(raw_input)
    if not share_url:
        raise HTTPException(status_code=400, detail="Tidak ada URL di teks yang ditempel.")
    host = urlparse(share_url).netloc.lower()
    need = {
        "youtube": ("youtube", "youtu.be"),
        "instagram": ("instagram", "instagr.am"),
        "facebook": ("facebook", "fb.watch", "fb.com"),
    }[platform]
    if not any(n in host for n in need):
        raise HTTPException(status_code=400, detail=f"URL ini sepertinya bukan link {platform.capitalize()}.")
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_ytdlp_extract, share_url, platform), timeout=YTDLP_TIMEOUT
        )
    except asyncio.TimeoutError:
        raise HTTPException(status_code=502, detail=f"{platform.capitalize()} lambat merespons. Coba lagi sebentar.")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(
            status_code=502,
            detail=f"Gagal mengambil data dari {platform.capitalize()} (mungkin konten privat/dihapus, atau platform sedang membatasi akses tanpa login).",
        )


# ----------------------------------------------------------------- pages ----
def page_ctx(request: Request, **kw):
    ctx = {"request": request, "site_url": SITE_URL}
    ctx.update(kw)
    return ctx


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(request, "index.html", page_ctx(request))


@app.get("/tiktok-downloader", response_class=HTMLResponse)
async def tiktok_page(request: Request):
    return templates.TemplateResponse(request, "tool.html", page_ctx(request, tool="tiktok"))


@app.get("/douyin-downloader", response_class=HTMLResponse)
async def douyin_page(request: Request):
    return templates.TemplateResponse(request, "tool.html", page_ctx(request, tool="douyin"))


@app.get("/youtube-downloader", response_class=HTMLResponse)
async def youtube_page(request: Request):
    return templates.TemplateResponse(request, "tool.html", page_ctx(request, tool="youtube"))


@app.get("/instagram-downloader", response_class=HTMLResponse)
async def instagram_page(request: Request):
    return templates.TemplateResponse(request, "tool.html", page_ctx(request, tool="instagram"))


@app.get("/facebook-downloader", response_class=HTMLResponse)
async def facebook_page(request: Request):
    return templates.TemplateResponse(request, "tool.html", page_ctx(request, tool="facebook"))


@app.get("/cara-penggunaan", response_class=HTMLResponse)
async def cara_page(request: Request):
    return templates.TemplateResponse(request, "cara.html", page_ctx(request))


@app.get("/faq", response_class=HTMLResponse)
async def faq_page(request: Request):
    return templates.TemplateResponse(request, "faq.html", page_ctx(request))


@app.get("/kontak", response_class=HTMLResponse)
async def kontak_page(request: Request):
    return templates.TemplateResponse(request, "kontak.html", page_ctx(request))


@app.get("/robots.txt", response_class=PlainTextResponse)
async def robots():
    return f"User-agent: *\nAllow: /\nSitemap: {SITE_URL}/sitemap.xml\n"


@app.get("/sitemap.xml")
async def sitemap():
    paths = ["/", "/tiktok-downloader", "/douyin-downloader", "/youtube-downloader",
             "/instagram-downloader", "/facebook-downloader",
             "/cara-penggunaan", "/faq", "/kontak"]
    urls = "".join(f"<url><loc>{SITE_URL}{p}</loc></url>" for p in paths)
    xml = f'<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{urls}</urlset>'
    return HTMLResponse(content=xml, media_type="application/xml")


@app.get("/.well-known/assetlinks.json")
async def assetlinks():
    """Digital Asset Links untuk verifikasi TWA Android (APK DigitCuan Downloader)."""
    f = BASE_DIR / "static" / "well-known" / "assetlinks.json"
    if f.exists():
        return JSONResponse(content=json.loads(f.read_text()))
    return JSONResponse(content=[])


# ------------------------------------------------------------------- api ----
@app.post("/api/download")
async def api_download(payload: URLRequest):
    if not payload.url or not payload.url.strip():
        raise HTTPException(status_code=400, detail="URL tidak boleh kosong.")
    url = extract_url(payload.url)
    if not url:
        raise HTTPException(status_code=400, detail="Tidak ada URL di teks yang ditempel. Tempel link TikTok, Douyin, YouTube, Instagram, atau Facebook.")
    platform = detect_platform(url)
    if platform == "tiktok":
        result = await extract_tiktok(payload.url)
    elif platform == "douyin":
        result = await extract_douyin(payload.url)
    elif platform in ("youtube", "instagram", "facebook"):
        result = await extract_ytdlp(payload.url, platform)
    else:
        raise HTTPException(status_code=400, detail="Link tidak dikenali. Tool ini mendukung TikTok, Douyin, YouTube, Instagram, dan Facebook.")
    return JSONResponse(result)


def _check_host(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if not any(host == s or host.endswith("." + s) for s in DOWNLOAD_HOST_SUFFIXES):
        raise HTTPException(status_code=400, detail="Host unduhan tidak diizinkan.")
    return host


def _referer_for_host(host: str) -> str:
    if "googlevideo" in host:
        return "https://www.youtube.com/"
    if "cdninstagram" in host:
        return "https://www.instagram.com/"
    if "fbcdn" in host:
        return "https://www.facebook.com/"
    if "tiktok" in host:
        return "https://www.tiktok.com/"
    return "https://www.douyin.com/"


@app.get("/download")
async def download_proxy(url: str, name: str = "video.mp4"):
    """Stream file lewat server agar browser menyimpan sebagai unduhan."""
    host = _check_host(url)
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)[:80] or "video.mp4"
    referer = _referer_for_host(host)
    client = httpx.AsyncClient(follow_redirects=True, timeout=DOWNLOAD_TIMEOUT)
    try:
        req = client.build_request("GET", url, headers={"User-Agent": UA_MOBILE, "Referer": referer})
        resp = await client.send(req, stream=True)
    except httpx.HTTPError:
        await client.aclose()
        raise HTTPException(status_code=502, detail="Gagal mengambil file dari server sumber.")
    if resp.status_code != 200:
        await resp.aclose()
        await client.aclose()
        raise HTTPException(status_code=502, detail=f"Server sumber menolak unduhan (HTTP {resp.status_code}).")

    async def body_iter():
        try:
            async for chunk in resp.aiter_bytes(65536):
                yield chunk
        finally:
            await resp.aclose()
            await client.aclose()

    headers = {"Content-Disposition": f'attachment; filename="{safe_name}"'}
    if resp.headers.get("content-length"):
        headers["Content-Length"] = resp.headers["content-length"]
    return StreamingResponse(
        body_iter(),
        media_type=resp.headers.get("content-type", "application/octet-stream"),
        headers=headers,
    )


def _ffmpeg_exe() -> str | None:
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return None


@app.get("/mp3")
async def mp3_extract(url: str, name: str = "douyin-audio.mp3"):
    """Ekstrak audio MP3 dari video (Douyin/YouTube/Instagram/Facebook)."""
    host = _check_host(url)
    ff = _ffmpeg_exe()
    if not ff:
        raise HTTPException(status_code=501, detail="Fitur MP3 belum tersedia di server.")
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)[:80] or "douyin-audio.mp3"
    if not safe_name.endswith(".mp3"):
        safe_name += ".mp3"
    referer = _referer_for_host(host)

    tmpdir = tempfile.mkdtemp(prefix="dc-mp3-")
    src = os.path.join(tmpdir, "src.mp4")
    out = os.path.join(tmpdir, "out.mp3")

    def cleanup():
        try:
            for f in (src, out):
                if os.path.exists(f):
                    os.remove(f)
            os.rmdir(tmpdir)
        except OSError:
            pass

    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=DOWNLOAD_TIMEOUT) as client:
            async with client.stream("GET", url, headers={"User-Agent": UA_MOBILE, "Referer": referer}) as resp:
                if resp.status_code != 200:
                    raise HTTPException(status_code=502, detail="Gagal mengambil video untuk diekstrak audionya.")
                total = 0
                with open(src, "wb") as fh:
                    async for chunk in resp.aiter_bytes(65536):
                        total += len(chunk)
                        if total > MAX_MP3_SOURCE_BYTES:
                            raise HTTPException(status_code=413, detail="Video terlalu besar untuk ekstraksi MP3 (maks 70 MB).")
                        fh.write(chunk)
    except HTTPException:
        cleanup()
        raise
    except httpx.HTTPError:
        cleanup()
        raise HTTPException(status_code=502, detail="Gagal mengambil video dari sumber.")

    def run_ffmpeg():
        return subprocess.run(
            [ff, "-y", "-v", "error", "-i", src, "-vn", "-codec:a", "libmp3lame", "-q:a", "4", out],
            capture_output=True, timeout=180,
        )

    try:
        proc = await asyncio.to_thread(run_ffmpeg)
    except Exception:
        cleanup()
        raise HTTPException(status_code=500, detail="Proses ekstraksi audio gagal.")
    if proc.returncode != 0 or not os.path.exists(out):
        cleanup()
        raise HTTPException(status_code=500, detail="Proses ekstraksi audio gagal.")
    return FileResponse(out, media_type="audio/mpeg", filename=safe_name, background=BackgroundTask(cleanup))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000)
