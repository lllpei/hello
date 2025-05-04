from flask import Flask, request, jsonify
import sqlite3
import pandas as pd
from pathlib import Path
import logging
from typing import List, Dict, Any, Optional

"""
OFAC API (extended)
-------------------
2025-05-04
 - 統合検索エンドポイント `/ofacParty/search` を拡張し、別名(alias)・住所(address) 検索を含むフルテキスト検索を実装
 - クエリパラメータ
      q       : 検索語 (必須 / name でも可 · 2〜100文字)
      scope   : all | name | alias | address   [default: all]
      country : 国コード or 国名 (任意)
      city    : 都市名 (任意)
      limit   : 1‒1000  [default: 100]
      fuzzy   : true/false 類似度検索 (未実装 placeholder)
 - 既存 `partyId` 取得エンドポイントは変更なし
 - 後方互換: `name=` パラメータを許容 (scope=name)
"""

# ──────────────────────────────────────────────
# ロギング設定
# ──────────────────────────────────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "ofac_api.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Flask アプリ & DB ファイル
# ──────────────────────────────────────────────
app = Flask(__name__)
DB_FILE = Path("ofac_demo.db")

# ──────────────────────────────────────────────
# 共通ユーティリティ
# ──────────────────────────────────────────────

def query_to_df(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> pd.DataFrame:
    """SQL を実行し DataFrame で返す (0 行なら empty DataFrame)"""
    return pd.read_sql_query(sql, conn, params=params)

# ──────────────────────────────────────────────
# 個別パーティ詳細取得
# ──────────────────────────────────────────────

def get_party_data(party_id: int) -> Optional[Dict[str, Any]]:
    if not DB_FILE.exists():
        logger.error("DBファイルが見つかりません: %s", DB_FILE)
        return {"error": f"DB ファイル {DB_FILE} が見つかりません。"}

    logger.info("パーティデータの取得: party_id=%s", party_id)
    conn = sqlite3.connect(DB_FILE)

    details_sql = """
    SELECT 
        p.party_type_cd             AS "Type",
        n.name_text                 AS "Entity Name",
        cm_list.code_value          AS "List",
        GROUP_CONCAT(cm_prog.code_value, '; ') AS "Program",
        COALESCE(p.remarks,'')      AS "Remarks"
    FROM ofac_sanctioned_party p
    JOIN ofac_party_name n
          ON p.party_id = n.party_id
         AND n.is_primary_flg = 1
         AND n.name_type_cd = 'FORMAL'
    JOIN ofac_party_list_link ll    ON p.party_id = ll.party_id
    JOIN ofac_code_master cm_list   ON ll.list_cd   = cm_list.code_id
    LEFT JOIN ofac_party_program_link pl ON p.party_id = pl.party_id
    LEFT JOIN ofac_code_master cm_prog   ON pl.program_cd = cm_prog.code_id
    WHERE p.party_id = ?
    GROUP BY p.party_id;
    """
    details_df = query_to_df(conn, details_sql, (party_id,))
    if details_df.empty:
        conn.close()
        return None

    ident_df = query_to_df(conn, """
    SELECT attribute_type_cd AS "Type", attribute_value AS "ID / Information"
      FROM ofac_party_attribute
     WHERE party_id = ?
       AND attribute_type_cd IN ('Website', 'Additional Sanctions Information -')
     ORDER BY attribute_type_cd, attribute_value;
    """, (party_id,))

    alias_df = query_to_df(conn, """
    SELECT 'a.k.a.' AS "Type", 'weak' AS "Category", name_text AS "Name"
      FROM ofac_party_name
     WHERE party_id = ?
       AND REPLACE(LOWER(name_type_cd),'.','') = 'aka'
     ORDER BY name_text;
    """, (party_id,))

    addr_df = query_to_df(conn, """
    SELECT address_line AS "Address", city AS "City", '' AS "State / Province",
           postal_code AS "Postal Code", cm.code_value AS "Country"
      FROM ofac_party_address ad
      LEFT JOIN ofac_code_master cm ON ad.country_cd = cm.code_id
     WHERE ad.party_id = ?;
    """, (party_id,))

    conn.close()
    return {
        "details": details_df.to_dict(orient="records")[0],
        "identifications": ident_df.to_dict(orient="records"),
        "aliases": alias_df.to_dict(orient="records"),
        "addresses": addr_df.to_dict(orient="records")
    }

# ──────────────────────────────────────────────
# 検索ロジック (統合)
# ──────────────────────────────────────────────

ALLOWED_SCOPES = {"all", "name", "alias", "address"}


def search_party_advanced(
    q: str,
    scope: str = "all",
    country: Optional[str] = None,
    city: Optional[str] = None,
    limit: int = 100,
    fuzzy: bool = False
) -> List[Dict[str, Any]]:
    """alias / address を含む統合検索 (LIKE ベース)"""
    if not DB_FILE.exists():
        logger.error("DBファイルが見つかりません: %s", DB_FILE)
        return {"error": f"DB ファイル {DB_FILE} が見つかりません。"}

    # パラメータ前処理
    scope = scope.lower()
    if scope not in ALLOWED_SCOPES:
        raise ValueError(f"scope は {', '.join(ALLOWED_SCOPES)} のいずれかで指定してください")
    limit = max(1, min(limit, 1000))  # clamp to 1–1000

    pattern = f"%{q}%"  # LIKE パターン

    conn = sqlite3.connect(DB_FILE)
    conn.create_function("lower", 1, lambda s: s.lower() if s else None)

    # 動的 WHERE 条件
    where_clauses = ["lower(s.match_value) LIKE lower(?)"]
    params: List[Any] = [pattern]

    if scope != "all":
        where_clauses.append("s.match_field = ?")
        params.append(scope)

    if country:
        where_clauses.append("(lower(ad.country_cd) = lower(?) OR lower(cm_country.code_value) = lower(?))")
        params.extend([country, country])

    if city:
        where_clauses.append("lower(ad.city) = lower(?)")
        params.append(city)

    where_sql = " AND ".join(where_clauses)

    # SQL 組み立て
    sql = f"""
    WITH union_search AS (
        -- name
        SELECT n.party_id, 'name' AS match_field, n.name_text AS match_value
          FROM ofac_party_name n
         WHERE n.is_primary_flg = 1 AND n.name_type_cd = 'FORMAL'
        UNION ALL
        -- alias
        SELECT n2.party_id, 'alias', n2.name_text
          FROM ofac_party_name n2
         WHERE REPLACE(LOWER(n2.name_type_cd),'.','') = 'aka'
        UNION ALL
        -- address
        SELECT ad.party_id, 'address',
               COALESCE(ad.address_line,'') || ' ' || COALESCE(ad.city,'') || ' ' || COALESCE(ad.country_cd,'')
          FROM ofac_party_address ad
    )
    SELECT DISTINCT p.party_id,
           pn.name_text              AS "Entity Name",
           p.party_type_cd           AS "Type",
           cm_list.code_value        AS "List",
           GROUP_CONCAT(cm_prog.code_value,'; ') AS "Program",
           s.match_field             AS matchField,
           s.match_value             AS matchValue
      FROM union_search s
      JOIN ofac_sanctioned_party p       ON p.party_id = s.party_id
      JOIN ofac_party_name pn            ON pn.party_id = p.party_id
                                        AND pn.is_primary_flg = 1
                                        AND pn.name_type_cd = 'FORMAL'
      JOIN ofac_party_list_link ll       ON p.party_id = ll.party_id
      JOIN ofac_code_master cm_list      ON ll.list_cd  = cm_list.code_id
      LEFT JOIN ofac_party_program_link pl  ON p.party_id = pl.party_id
      LEFT JOIN ofac_code_master cm_prog    ON pl.program_cd = cm_prog.code_id
      LEFT JOIN ofac_party_address ad       ON ad.party_id = p.party_id
      LEFT JOIN ofac_code_master cm_country ON ad.country_cd = cm_country.code_id
     WHERE {where_sql}
     GROUP BY p.party_id, pn.name_text, p.party_type_cd, cm_list.code_value, s.match_field, s.match_value
     ORDER BY pn.name_text
     LIMIT ?;
    """

    params.append(limit)

    logger.debug("統合検索 SQL: %s", sql)
    logger.debug("params: %s", params)

    df = query_to_df(conn, sql, tuple(params))
    conn.close()

    # 空なら []
    return df.to_dict(orient="records") if not df.empty else []

# ──────────────────────────────────────────────
# Flask ルーティング
# ──────────────────────────────────────────────

@app.route("/ofacParty", methods=["GET"])
def ofac_party():
    party_id_param = request.args.get("partyId")
    logger.info("/ofacParty request: partyId=%s", party_id_param)

    if not party_id_param or not party_id_param.isdigit():
        return jsonify({"resultCd": False, "message": "partyId を数値で指定してください"}), 400

    party_id = int(party_id_param)
    result = get_party_data(party_id)

    if isinstance(result, dict) and result.get("error"):
        return jsonify({"resultCd": False, "message": result["error"]}), 500
    if result is None:
        return jsonify({"resultCd": False, "message": f"party_id={party_id} のデータが見つかりません"}), 404

    return jsonify({"resultCd": True, "data": result})

@app.route("/ofacParty/search", methods=["GET"])
def search_party():
    # q (新) or name (旧) のいずれか必須
    q_param = request.args.get("q") or request.args.get("name")
    if not q_param or len(q_param.strip()) < 2:
        return jsonify({"resultCd": False, "message": "q または name パラメータを2文字以上で指定してください"}), 400

    scope_param = (request.args.get("scope") or "all").lower()
    country_param = request.args.get("country")
    city_param = request.args.get("city")

    # limit
    try:
        limit_param = int(request.args.get("limit", 100))
    except ValueError:
        return jsonify({"resultCd": False, "message": "limit パラメータは整数で指定してください"}), 400

    fuzzy_param = request.args.get("fuzzy", "false").lower() == "true"

    # 検索実行
    try:
        result = search_party_advanced(
            q=q_param.strip(),
            scope=scope_param,
            country=country_param,
            city=city_param,
            limit=limit_param,
            fuzzy=fuzzy_param
        )
    except ValueError as e:
        return jsonify({"resultCd": False, "message": str(e)}), 400

    if isinstance(result, dict) and "error" in result:
        return jsonify({"resultCd": False, "message": result["error"]}), 500

    return jsonify({"resultCd": True, "data": result})

# ──────────────────────────────────────────────
# エントリポイント
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import hypercorn.asyncio
    import asyncio
    config = hypercorn.Config()
    config.bind = ["0.0.0.0:10000"]
    config.workers = 1
    config.keep_alive_timeout = 65
    logger.info("OFAC API サーバー起動: %s", config.bind)
    asyncio.run(hypercorn.asyncio.serve(app, config))
