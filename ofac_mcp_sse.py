from typing import Dict, Any, Optional
import os
import sys
import httpx
import logging
from pathlib import Path
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

"""
OFAC MCP Tool (extended)
------------------------
2025-05-04
 - /ofacParty/search の統合検索 API に合わせて検索ツールを拡張
 - search_party(q, scope, country, city, limit, fuzzy)
 - 既存 get_ofac_party_info(party_id) は変更なし
"""

# ──────────────────────────────────────────────
# ロギング設定
# ──────────────────────────────────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "ofac_mcp.log"

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

# Flask API エンドポイント (env で上書き可)
DEFAULT_BASE = "https://hello-render-rbg8.onrender.com/ofacParty"
API_ENDPOINT = os.getenv("API_ENDPOINT", DEFAULT_BASE)
SEARCH_ENDPOINT = os.getenv("SEARCH_ENDPOINT", f"{DEFAULT_BASE}/search")

logger.info("API_ENDPOINT = %s", API_ENDPOINT)
logger.info("SEARCH_ENDPOINT = %s", SEARCH_ENDPOINT)

# ──────────────────────────────────────────────
# MCP サーバ初期化
# ──────────────────────────────────────────────
MCP_NAME = "ofac_party_service"
mcp = FastMCP(MCP_NAME)

# ──────────────────────────────────────────────
# 共通 util
# ──────────────────────────────────────────────
ALLOWED_SCOPES = {"all", "name", "alias", "address"}


def _extract_error_message(resp: httpx.Response) -> str:
    """API 返却 JSON から message を抽出 (fallback は status/text)"""
    try:
        data = resp.json()
        return data.get("message", f"API Error: {resp.status_code}")
    except Exception:
        return f"API Error: {resp.status_code} - {resp.text}"[:300]


# ──────────────────────────────────────────────
# ツール: 個別取得
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
    """名前・別名・住所を含む統合検索を実行します

    Args:
        q (str): 検索語 (2 文字以上)
        scope (str, optional): "all" | "name" | "alias" | "address". デフォルト "all".
        country (str, optional): 国コードまたは国名
        city (str, optional): 都市名
        limit (int, optional): 最大取得件数 (1–1000)
        fuzzy (bool, optional): 類似度検索 (true=有効)

    Returns:
        Dict[str, Any]:
            - status: success / error
            - data : 検索結果 (list) ※ success 時
            - message: エラー詳細 (error 時)
    """
    logger.info("search_party start: q=%s, scope=%s", q, scope)

    # 入力バリデーション
    q = (q or "").strip()
    if len(q) < 2:
        return {"status": "error", "message": "q must be at least 2 characters"}

    scope = (scope or "all").lower()
    if scope not in ALLOWED_SCOPES:
        return {"status": "error", "message": f"scope must be one of {', '.join(ALLOWED_SCOPES)}"}

    limit = max(1, min(int(limit or 100), 1000))

    params: Dict[str, Any] = {
        "q": q,
        "scope": scope,
        "limit": str(limit),
    }
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
# ENTRY POINT
# ──────────────────────────────────────────────

if __name__ == "__main__":
    try:
        logger.info("Starting OFAC MCP server (SSE transport)…")
        mcp.run(transport="sse", host="0.0.0.0", port=10001)
    except Exception as exc:
        logger.exception("MCP server startup failed: %s", str(exc))
        sys.exit(1)
