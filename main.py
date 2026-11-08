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
  ffmpeg). YouTube: bila tunnel WARP hidup, ekstraksi lewat SOCKS5
  127.0.0.1:1080 (bootstrap wgcf+wireproxy otomatis saat start, fallback
  direct bila gagal). Catatan jujur: YouTube/Instagram kadang membatasi akses tanpa
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
import socket
import ssl
import subprocess
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.request
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote, urlparse

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from starlette.background import BackgroundTask

# ------------------------------------------------- WARP (Cloudflare) ----
# YouTube dari Render diblokir karena IP datacenter di-flag (v3.1 client
# android/android_vr + paksa IPv4 tetap 502). Solusi: egress YouTube lewat
# Cloudflare WARP gratis — wgcf mendaftarkan identitas WARP gratis, lalu
# wireproxy menjalankan tunnel WireGuard userspace (tanpa root) dan
# mengeksposnya sebagai proxy lokal SOCKS5 127.0.0.1:1080 (+ HTTP 1081
# sebagai cadangan/debug). HANYA YouTube yang memakai proxy; TikTok,
# Douyin, IG, FB tidak berubah. Semua best-effort: bila bootstrap gagal
# di langkah mana pun, aplikasi tetap jalan dan YouTube fallback ke direct
# (perilaku lama, 502). Start command tetap `uvicorn main:app` — bootstrap
# jalan dari dalam proses aplikasi (thread background, tidak memblokir
# startup; Render filesystem ephemeral, jadi daftar tiap boot itu normal).
# Wiring terverifikasi dari README resmi: wgcf `register --accept-tos` +
# `generate [--keepalive]` (akun: wgcf-account.toml, profil:
# wgcf-profile.conf); wireproxy `wireproxy -c <conf>` dengan [Socks5] /
# [http] ditempel ke profil wgcf (README wireproxy, pufferffish/windtf).
# Verifikasi tunnel: probe SOCKS5 end-to-end (CONNECT ke
# www.cloudflare.com:443 + TLS + GET /cdn-cgi/trace, harus 'warp=on').

WARP_DIR = Path(tempfile.gettempdir()) / "dc-warp"
WARP_SOCKS_HOST = "127.0.0.1"
WARP_SOCKS_PORT = 1080
WARP_SOCKS_URL = f"socks5://{WARP_SOCKS_HOST}:{WARP_SOCKS_PORT}"
WARP_HTTP_PORT = 1081
WARP_GITHUB_API = "https://api.github.com/repos/{repo}/releases/latest"
WGCF_FALLBACK_URL = "https://github.com/ViRb3/wgcf/releases/download/v2.3.1/wgcf_2.3.1_linux_amd64"
WIREPROXY_FALLBACK_URL = "https://github.com/windtf/wireproxy/releases/download/v1.1.2/wireproxy_linux_amd64.tar.gz"

_WARP = {
    "up": False,          # True bila tunnel terverifikasi warp=on
    "healthy": False,     # hasil cek port terakhir
    "last_check": 0.0,    # monotonic detik cek terakhir
    "proc": None,         # subprocess.Popen wireproxy
    "started": False,     # guard agar bootstrap thread hanya sekali
    "logged_use": False,  # log pemakaian proxy sekali per proses
}


def _warp_log(msg: str) -> None:
    print(f"[warp] {msg}", flush=True)


def _is_elf(path: Path) -> bool:
    try:
        with open(path, "rb") as fh:
            return fh.read(4) == b"\x7fELF"
    except OSError:
        return False


def _download_file(url: str, dest: Path, min_size: int, magic: bytes, timeout: int = 90) -> bool:
    """Unduh URL ke dest; verifikasi ukuran + magic bytes (tolak halaman
    error HTML — pelajaran insiden download Caddy corrupt)."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "DigitCuan-Downloader"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
    except Exception as e:
        _warp_log(f"download gagal {url}: {e}")
        return False
    if len(data) < min_size or not data.startswith(magic):
        head = data[:120].decode("utf-8", "replace").replace("\n", " ")
        _warp_log(f"download {url} tidak valid (ukuran {len(data)}, awal: {head!r})")
        return False
    dest.write_bytes(data)
    os.chmod(dest, 0o755)
    return True


def _github_asset_url(repo: str, match) -> str | None:
    """Resolve asset release terbaru via GitHub API (tanpa auth)."""
    try:
        req = urllib.request.Request(
            WARP_GITHUB_API.format(repo=repo),
            headers={"User-Agent": "DigitCuan-Downloader", "Accept": "application/vnd.github+json"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        for asset in data.get("assets") or []:
            name = asset.get("name") or ""
            if match(name):
                return asset.get("browser_download_url")
    except Exception as e:
        _warp_log(f"GitHub API {repo} gagal: {e}")
    return None


def _ensure_warp_binaries() -> tuple[Path, Path] | None:
    """Pastikan biner wgcf + wireproxy ada & valid (ELF). Unduh bila perlu."""
    WARP_DIR.mkdir(parents=True, exist_ok=True)
    wgcf_bin = WARP_DIR / "wgcf"
    wire_bin = WARP_DIR / "wireproxy"

    if not _is_elf(wgcf_bin):
        url = _github_asset_url(
            "ViRb3/wgcf",
            lambda n: n.startswith("wgcf_") and "linux_amd64" in n and not n.endswith((".sig", ".sha256")),
        ) or WGCF_FALLBACK_URL
        _warp_log(f"unduh wgcf dari {url}")
        if not _download_file(url, wgcf_bin, 5_000_000, b"\x7fELF"):
            return None
        _warp_log(f"wgcf OK ({wgcf_bin.stat().st_size} byte)")

    if not _is_elf(wire_bin):
        url = _github_asset_url(
            "windtf/wireproxy",
            lambda n: n.endswith("linux_amd64.tar.gz"),
        ) or WIREPROXY_FALLBACK_URL
        _warp_log(f"unduh wireproxy dari {url}")
        tgz = WARP_DIR / "wireproxy.tar.gz"
        if not _download_file(url, tgz, 1_000_000, b"\x1f\x8b"):
            return None
        try:
            with tarfile.open(tgz, "r:gz") as tf:
                member = next((m for m in tf.getmembers() if m.isfile() and Path(m.name).name == "wireproxy"), None)
                if member is None:
                    _warp_log("wireproxy.tar.gz tidak berisi biner 'wireproxy'")
                    return None
                tf.extract(member, WARP_DIR, filter="data")
                src = WARP_DIR / member.name
                if src != wire_bin:
                    src.replace(wire_bin)
                os.chmod(wire_bin, 0o755)
        except Exception as e:
            _warp_log(f"ekstrak wireproxy gagal: {e}")
            return None
        if not _is_elf(wire_bin):
            _warp_log("wireproxy hasil ekstrak bukan ELF")
            return None
        _warp_log(f"wireproxy OK ({wire_bin.stat().st_size} byte)")
    return wgcf_bin, wire_bin


def _run_wgcf(args: list[str], timeout: int) -> subprocess.CompletedProcess | None:
    try:
        return subprocess.run(
            args, cwd=str(WARP_DIR), capture_output=True, text=True,
            input="y\n", timeout=timeout,
        )
    except Exception as e:
        _warp_log(f"wgcf {' '.join(args[1:])} error: {e}")
        return None


def _socks5_probe_warp(timeout: float = 20.0) -> bool:
    """Verifikasi end-to-end: CONNECT SOCKS5 ke www.cloudflare.com:443,
    TLS, GET /cdn-cgi/trace — sukses hanya bila respons berisi 'warp=on'."""

    def recv_exact(sock, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                break
            buf += chunk
        return buf

    try:
        sock = socket.create_connection((WARP_SOCKS_HOST, WARP_SOCKS_PORT), timeout=timeout)
        sock.settimeout(timeout)
        sock.sendall(b"\x05\x01\x00")
        if recv_exact(sock, 2) != b"\x05\x00":
            sock.close()
            return False
        host = b"www.cloudflare.com"
        sock.sendall(b"\x05\x01\x00\x03" + bytes([len(host)]) + host + (443).to_bytes(2, "big"))
        rep = recv_exact(sock, 4)
        if len(rep) < 4 or rep[0] != 0x05 or rep[1] != 0x00:
            sock.close()
            return False
        atyp = rep[3]
        if atyp == 0x01:        # IPv4
            recv_exact(sock, 6)
        elif atyp == 0x03:      # domain
            ln = recv_exact(sock, 1)
            if not ln:
                sock.close()
                return False
            recv_exact(sock, ln[0] + 2)
        elif atyp == 0x04:      # IPv6
            recv_exact(sock, 18)
        else:
            sock.close()
            return False
        ctx = ssl.create_default_context()
        tls = ctx.wrap_socket(sock, server_hostname="www.cloudflare.com")
        tls.sendall(
            b"GET /cdn-cgi/trace HTTP/1.1\r\nHost: www.cloudflare.com\r\n"
            b"Connection: close\r\nUser-Agent: DigitCuan-Downloader\r\n\r\n"
        )
        data = b""
        while True:
            chunk = tls.recv(4096)
            if not chunk:
                break
            data += chunk
            if len(data) > 65536:
                break
        tls.close()
        return "warp=on" in data.decode("utf-8", "replace")
    except Exception as e:
        _warp_log(f"probe warp gagal: {e}")
        return False


def _warp_bootstrap() -> None:
    """Thread background: siapkan WARP. TIDAK PERNAH raise — semua kegagalan
    dicatat dan YouTube fallback ke direct."""
    try:
        _warp_log("bootstrap mulai (background, non-blocking)")
        bins = _ensure_warp_binaries()
        if bins is None:
            _warp_log("bootstrap GAGAL: biner tidak tersedia — YouTube fallback direct")
            return
        wgcf_bin, wire_bin = bins

        account = WARP_DIR / "wgcf-account.toml"
        if not (account.exists() and account.stat().st_size > 0):
            proc = _run_wgcf([str(wgcf_bin), "register", "--accept-tos"], timeout=60)
            if proc is not None and proc.returncode != 0 and "unknown flag" in (proc.stderr or ""):
                _warp_log("register --accept-tos tidak didukung, coba tanpa flag")
                proc = _run_wgcf([str(wgcf_bin), "register"], timeout=60)
            if proc is not None:
                tail = ((proc.stdout or "") + (proc.stderr or ""))[-300:].replace("\n", " ")
                _warp_log(f"register rc={proc.returncode}: {tail}")
            # Issue wgcf #515: register kadang rc!=0 tapi account tetap dibuat.
            if not (account.exists() and account.stat().st_size > 0):
                _warp_log("bootstrap GAGAL: wgcf-account.toml tidak terbentuk — YouTube fallback direct")
                return
        else:
            _warp_log("wgcf-account.toml sudah ada — lewati register")

        profile = WARP_DIR / "wgcf-profile.conf"
        proc = _run_wgcf([str(wgcf_bin), "generate", "--keepalive"], timeout=30)
        if proc is not None and proc.returncode != 0 and "unknown flag" in (proc.stderr or ""):
            proc = _run_wgcf([str(wgcf_bin), "generate"], timeout=30)
        if proc is None or proc.returncode != 0 or not (profile.exists() and profile.stat().st_size > 0):
            _warp_log("bootstrap GAGAL: wgcf generate gagal — YouTube fallback direct")
            return
        _warp_log("wgcf-profile.conf OK")

        conf_text = profile.read_text()
        conf_text += (
            "\n[Socks5]\nBindAddress = 127.0.0.1:1080\n"
            "\n[http]\nBindAddress = 127.0.0.1:1081\n"
        )
        conf = WARP_DIR / "wireproxy.conf"
        conf.write_text(conf_text)

        logf = open(WARP_DIR / "wireproxy.log", "ab")
        proc = subprocess.Popen(
            [str(wire_bin), "-c", str(conf)],
            cwd=str(WARP_DIR), stdout=logf, stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        _WARP["proc"] = proc
        _warp_log(f"wireproxy start (pid {proc.pid})")

        # Tunggu listener SOCKS naik (maks 15 dtk).
        deadline = time.monotonic() + 15
        socks_up = False
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                _warp_log(f"wireproxy mati mendadak rc={proc.returncode} — YouTube fallback direct")
                _stop_warp()
                return
            try:
                with socket.create_connection((WARP_SOCKS_HOST, WARP_SOCKS_PORT), timeout=1):
                    socks_up = True
                    break
            except OSError:
                time.sleep(0.5)
        if not socks_up:
            _warp_log("bootstrap GAGAL: SOCKS 1080 tidak naik dalam 15 dtk — YouTube fallback direct")
            _stop_warp()
            return

        # Verifikasi end-to-end: lalu lintas benar-benar keluar via WARP.
        if _socks5_probe_warp(timeout=20.0):
            _WARP["up"] = True
            _WARP["healthy"] = True
            _WARP["last_check"] = time.monotonic()
            _warp_log("WARP AKTIF — YouTube akan lewat socks5://127.0.0.1:1080 (probe warp=on)")
        else:
            _warp_log("bootstrap GAGAL: probe warp=on tidak lolos (kemungkinan UDP diblokir) — YouTube fallback direct")
            _stop_warp()
    except Exception as e:
        _warp_log(f"bootstrap GAGAL tak terduga: {e!r} — YouTube fallback direct")
        _stop_warp()


def _start_warp_bootstrap() -> None:
    if _WARP["started"]:
        return
    _WARP["started"] = True
    threading.Thread(target=_warp_bootstrap, name="warp-bootstrap", daemon=True).start()


def _stop_warp() -> None:
    proc = _WARP.get("proc")
    _WARP["proc"] = None
    _WARP["up"] = False
    _WARP["healthy"] = False
    if proc is not None and proc.poll() is None:
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass


def _warp_healthy() -> bool:
    """Cek murah (cache 10 dtk): apakah SOCKS WARP masih hidup."""
    if not _WARP["up"]:
        return False
    now = time.monotonic()
    if now - _WARP["last_check"] < 10.0:
        return _WARP["healthy"]
    ok = False
    try:
        with socket.create_connection((WARP_SOCKS_HOST, WARP_SOCKS_PORT), timeout=1.0):
            ok = True
    except OSError:
        ok = False
    if not ok and _WARP["healthy"]:
        _warp_log("SOCKS 127.0.0.1:1080 mati — WARP dimatikan, YouTube fallback direct")
    _WARP["healthy"] = ok
    _WARP["last_check"] = now
    return ok


@asynccontextmanager
async def lifespan(app: FastAPI):
    _start_warp_bootstrap()
    yield
    _stop_warp()


BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="DigitCuan Downloader — TikTok, Douyin, YouTube, Instagram & Facebook", version="3.0.0", lifespan=lifespan)
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
        # memang "GVS PO token required", tapi enforcement-nya selektif
        # — dari IP bersih ia jalan tanpa token, sama seperti android.)
        # Paksa IPv4 (setara CLI -4; di API Python tidak ada param
        # force_ipv4 — caranya source_address "0.0.0.0", terverifikasi
        # di yt_dlp/options.py + YoutubeDL.py:441): egress IPv6 Render
        # bisa satu blok /64 yang di-flag YouTube.
        opts["source_address"] = "0.0.0.0"
        opts["extractor_args"] = {"youtube": {"player_client": ["android", "android_vr"]}}
        # Egress via WARP bila tunnel hidup — HANYA YouTube. yt-dlp memakai
        # implementasi SOCKS-nya sendiri (yt_dlp/socks.py, stdlib saja),
        # jadi tidak butuh PySocks / paket tambahan.
        if _warp_healthy():
            opts["proxy"] = WARP_SOCKS_URL
            if not _WARP["logged_use"]:
                _WARP["logged_use"] = True
                _warp_log("YouTube memakai proxy WARP socks5://127.0.0.1:1080")
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
