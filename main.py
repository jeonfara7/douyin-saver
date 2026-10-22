"""
DouyinSaver — Douyin No-Watermark Downloader (fixed build).

Cara kerja resolver (terverifikasi 21 Okt 2026):
1. Ambil URL dari teks yang ditempel (boleh teks share penuh).
2. Resolve link pendek v.douyin.com -> aweme_id.
3. Minta cookie pengunjung "ttwid" dari endpoint register ByteDance
   (tanpa cookie ini Douyin menyajikan halaman share TANPA data video).
4. Ambil halaman share iesdouyin.com dengan UA mobile + cookie ttwid,
   parse JSON _ROUTER_DATA -> videoInfoRes.item_list[0].
5. URL video tanpa watermark = play_addr dengan "playwm" -> "play".
"""
import os

# Env sandbox tertentu punya no_proxy berisi IPv6 bracket yang membuat
# parser URL httpx crash saat client dibuat. Sanitasi lebih dulu.
os.environ["no_proxy"] = "localhost,127.0.0.1"
os.environ["NO_PROXY"] = "localhost,127.0.0.1"

import asyncio
import json
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="Douyin No-Watermark Downloader", version="1.1.0")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

UA_MOBILE = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"
)
UA_PC = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
TIMEOUT = httpx.Timeout(connect=10.0, read=45.0, write=10.0, pool=10.0)
# Unduhan file bisa besar & koneksi lambat — beri napas lebih panjang.
DOWNLOAD_TIMEOUT = httpx.Timeout(connect=10.0, read=120.0, write=10.0, pool=10.0)

DOWNLOAD_HOST_SUFFIXES = (
    "snssdk.com", "douyinvod.com", "douyin.com", "amemv.com", "bytecdn.cn",
)


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


def extract_aweme_id(text: str) -> str | None:
    for pat in (r"/video/(\d+)", r"modal_id=(\d+)", r"/share/video/(\d+)"):
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


def item_to_result(item: dict, aweme_id: str) -> dict:
    video = item.get("video") or {}
    author = item.get("author") or {}
    play_list = (video.get("play_addr") or {}).get("url_list") or []
    playwm = play_list[0] if play_list else ""
    cover = ((video.get("cover") or {}).get("url_list") or [""])[0]
    avatar = ((author.get("avatar_larger") or {}).get("url_list") or [""])[0]
    return {
        "success": True,
        "title": item.get("desc") or "Douyin Video",
        "author": author.get("nickname") or "Unknown Author",
        "avatar": avatar,
        "cover": cover,
        "video_url": playwm.replace("playwm", "play"),
        "video_url_watermark": playwm,
        "aweme_id": aweme_id,
        "duration_ms": video.get("duration"),
    }


async def extract_douyin_video(raw_input: str) -> dict:
    share_url = extract_url(raw_input)
    if not share_url:
        raise HTTPException(status_code=400, detail="Tidak ada URL di teks yang ditempel.")
    host = urlparse(share_url).netloc
    if "douyin" not in host:
        raise HTTPException(status_code=400, detail="URL ini sepertinya bukan link Douyin.")

    aweme_id = extract_aweme_id(share_url)
    async with httpx.AsyncClient(follow_redirects=True, timeout=TIMEOUT) as client:
        ttwid = await get_ttwid(client)
        cookies = {"ttwid": ttwid} if ttwid else {}

        # Link pendek perlu di-resolve dulu untuk dapat aweme_id.
        if not aweme_id:
            try:
                r = await client.get(share_url, headers={"User-Agent": UA_MOBILE}, cookies=cookies)
            except httpx.HTTPError:
                raise HTTPException(status_code=502, detail="Gagal menghubungi Douyin. Coba lagi sebentar.")
            aweme_id = extract_aweme_id(str(r.url)) or extract_aweme_id(r.text)
            if aweme_id:
                item = find_item(parse_router_data(r.text) or {})
                if item:
                    return item_to_result(item, aweme_id)
        if not aweme_id:
            raise HTTPException(status_code=400, detail="Tidak bisa menemukan ID video dari link ini. Pastikan link-nya benar.")

        # Jalur utama: halaman share + ttwid + UA mobile.
        # Douyin kadang menyajikan halaman tanpa data secara sesaat
        # (rate limit) — ulangi sampai 3x dengan ttwid segar + jeda.
        for attempt in range(3):
            if attempt:
                await asyncio.sleep(2)
            try:
                r = await client.get(
                    f"https://www.iesdouyin.com/share/video/{aweme_id}/",
                    headers={"User-Agent": UA_MOBILE},
                    cookies=cookies,
                )
            except httpx.HTTPError:
                raise HTTPException(status_code=502, detail="Douyin tidak merespons. Coba lagi sebentar.")
            item = find_item(parse_router_data(r.text) or {})
            if item:
                return item_to_result(item, aweme_id)
            ttwid = await get_ttwid(client, force=True)
            cookies = {"ttwid": ttwid} if ttwid else {}

        # Jalur cadangan terakhir: API iteminfo lama (jarang berhasil).
        try:
            r = await client.get(
                f"https://www.iesdouyin.com/web/api/v2/aweme/iteminfo/?item_ids={aweme_id}",
                headers={"User-Agent": UA_MOBILE},
            )
            if r.status_code == 200 and r.content:
                data = r.json()
                items = data.get("item_list") or []
                if items:
                    return item_to_result(items[0], aweme_id)
        except Exception:
            pass

    raise HTTPException(
        status_code=502,
        detail="Douyin tidak memberikan data video untuk link ini (mungkin video privat/dihapus, atau Douyin sedang membatasi akses).",
    )


# --------------------------------------------------------------- routes ----
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/api/download")
async def api_download(payload: URLRequest):
    if not payload.url or not payload.url.strip():
        raise HTTPException(status_code=400, detail="URL cannot be empty")
    result = await extract_douyin_video(payload.url)
    return JSONResponse(result)


@app.get("/download")
async def download_proxy(url: str, name: str = "douyin-video.mp4"):
    """Stream file video lewat server agar browser menyimpan sebagai unduhan."""
    host = urlparse(url).netloc.lower()
    if not any(host == s or host.endswith("." + s) for s in DOWNLOAD_HOST_SUFFIXES):
        raise HTTPException(status_code=400, detail="Host unduhan tidak diizinkan.")
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)[:80] or "douyin-video.mp4"
    client = httpx.AsyncClient(follow_redirects=True, timeout=DOWNLOAD_TIMEOUT)
    try:
        req = client.build_request("GET", url, headers={"User-Agent": UA_MOBILE, "Referer": "https://www.douyin.com/"})
        resp = await client.send(req, stream=True)
    except httpx.HTTPError:
        await client.aclose()
        raise HTTPException(status_code=502, detail="Gagal mengambil file video dari Douyin.")
    if resp.status_code != 200:
        await resp.aclose()
        await client.aclose()
        raise HTTPException(status_code=502, detail=f"Douyin menolak unduhan (HTTP {resp.status_code}).")

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
        media_type=resp.headers.get("content-type", "video/mp4"),
        headers=headers,
    )


if __name__ == "__main__":
    import uvicorn

    # Di Hugging Face Spaces port-nya 7860; lokal default 8000.
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
