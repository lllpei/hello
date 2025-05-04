from typing import Dict, Any, Optional
import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
import httpx
from mcp.server.fastmcp import FastMCP
from fastapi import FastAPI
from starlette.routing import Mount
from asgiref.wsgi import WsgiToAsgi
from ofac_api import app as flask_app  # ← 既存 Flask API をインポート

"""
OFAC MCP (SSE 版)
=================
* FastMCP 高レベル API + Server‑Sent Events トランスポート
* 既存 ofac_api (Flask) を ASGI ラッパー経由で同ポート配下にマウント
* Hypercorn 1 プロセスで `/ofacParty/...` と `/mcp/...` を同時提供

エンドポイント構成
------------------
- **/ofacParty**                 : Flask REST API (既存)
- **/ofacParty/search**          : 同上 (検索拡張)
- **/mcp/sse** (GET)            : text/event‑stream (MCP)
- **/mcp/messages** (POST)      : JSON‑RPC (MCP)
"""

# ──────────────────────────────────────────────
# ロギング設定
# ──────────────────────────────────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "ofac_mcp_sse.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stderr)
    ]
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# 環境変数ロード
# ──────────────────────────────────────────────
load_dotenv()
BASE_URL = os.getenv("BASE_URL", "https://hello-render-rbg8.onrender.com")
API_ENDPOINT    = f"{BASE_URL}/ofacParty"
SEARCH_ENDPOINT = f"{API_ENDPOINT}/search"

logger.info("API_ENDPOINT = %s", API_ENDPOINT)
logger.info("SEARCH_ENDPOINT = %s", SEARCH_ENDPOINT)

# ──────────────────────────────────────────────
# MCP サーバー (FastMCP)
# ──────────────────────────────────────────────
MCP_NAME = "ofac_party_service"
mcp = FastMCP(MCP_NAME)

ALLOWED_SCOPES = {"all", "name", "alias", "address"}


def _extract_error_message(resp: httpx.Response) -> str:
    try:
        data = resp.json()
        return data.get("message", f"API Error: {resp.status_code}")
    except Exception:
        return f"API Error: {resp.status_code} - {resp.text}"[:300]


# ──────────────────────────────────────────────
# ツール: 個別パーティ取得
# ──────────────────────────────────────────────

@mcp.tool()
async def get_ofac_party_info(party_id: int) -> Dict[str, Any]:
    """party_id を指定して個別パーティ情報を取得します"""
    logger.info("get_ofac_party_info start: party_id=%s", party_id)

    params = {"partyId": str(party_id)}

    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(API_ENDPOINT, params=params, timeout=10.0)
            r.raise_for_status()
            j = r.json()
            if j.get("resultCd") is True:
                logger.info("取得成功 party_id=%s", party_id)
                return {"status": "success", "data": j.get("data", {})}
            else:
                msg = j.get("message", "API returned error")
                logger.warning("取得失敗 party_id=%s msg=%s", party_id, msg)
                return {"status": "error", "message": msg}
        except httpx.HTTPStatusError as e:
            msg = _extract_error_message(e.response)
            logger.error("HTTPStatusError: %s", msg)
            return {"status": "error", "message": msg}
        except httpx.RequestError as e:
            logger.error("RequestError: %s", str(e))
            return {"status": "error", "message": str(e)}
        except Exception as e:
            logger.exception("Unexpected error: %s", str(e))
            return {"status": "error", "message": str(e)}


# ──────────────────────────────────────────────
# ツール: 統合検索
# ──────────────────────────────────────────────

@mcp.tool()
async def search_party(
    q: str,
    scope: str = "all",
    country: Optional[str] = None,
    city: Optional[str] = None,
    limit: int = 100,
    fuzzy: bool = False
) -> Dict[str, Any]:
    """名前・別名・住所を含む統合検索を実行します"""
    logger.info("search_party start: q=%s, scope=%s", q, scope)

    q = (q or "").strip()
    if len(q) < 2:
        return {"status": "error", "message": "q must be at least 2 characters"}

    scope = (scope or "all").lower()
    if scope not in ALLOWED_SCOPES:
        return {"status": "error", "message": f"scope must be one of {', '.join(ALLOWED_SCOPES)}"}

    limit = max(1, min(int(limit or 100), 1000))

    params: Dict[str, Any] = {"q": q, "scope": scope, "limit": str(limit)}
    if country:
        params["country"] = country
    if city:
        params["city"] = city
    if fuzzy:
        params["fuzzy"] = "true"

    async with httpx.AsyncClient() as client:
        try:
            logger.debug("search request => %s params=%s", SEARCH_ENDPOINT, params)
            r = await client.get(SEARCH_ENDPOINT, params=params, timeout=10.0)
            r.raise_for_status()
            j = r.json()
            if j.get("resultCd") is True:
                data = j.get("data", [])
                logger.info("search success: hits=%s", len(data))
                return {"status": "success", "data": data}
            else:
                msg = j.get("message", "API returned error")
                logger.warning("search biz error: %s", msg)
                return {"status": "error", "message": msg}
        except httpx.HTTPStatusError as e:
            msg = _extract_error_message(e.response)
            logger.error("HTTPStatusError: %s", msg)
            return {"status": "error", "message": msg}
        except httpx.RequestError as e:
            logger.error("RequestError: %s", str(e))
            return {"status": "error", "message": str(e)}
        except Exception as e:
            logger.exception("Unexpected error: %s", str(e))
            return {"status": "error", "message": str(e)}


# ──────────────────────────────────────────────
# ASGI アプリ (Flask + MCP) を統合
# ──────────────────────────────────────────────

flask_asgi = WsgiToAsgi(flask_app)

combined_app = FastAPI(title="OFAC API + MCP (SSE)")
combined_app.router.routes.append(Mount("/", app=flask_asgi, name="ofac_api"))
combined_app.router.routes.append(Mount("/mcp", app=mcp.sse_app(), name="mcp"))


# ──────────────────────────────────────────────
# エントリポイント
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import hypercorn.asyncio as hyper_asyncio
    from hypercorn.config import Config
    import asyncio

    cfg = Config()
    cfg.bind = ["0.0.0.0:10000"]  # ofac_api と同じポート
    cfg.workers = 1
    cfg.keep_alive_timeout = 65

    logger.info("Starting combined OFAC API + MCP (SSE) server on %s", cfg.bind)
    try:
        asyncio.run(hyper_asyncio.serve(combined_app, cfg))
    except Exception as exc:
        logger.exception("Server startup failed: %s", str(exc))
        sys.exit(1)
