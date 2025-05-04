from flask import Flask, request, jsonify
import sqlite3
import pandas as pd
from pathlib import Path
import logging

# ログファイルのパスを設定
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "ofac_api.log"

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()  # 標準出力にも出力
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

DB_FILE = Path("ofac_demo.db")

def query_to_df(conn, sql: str, params=()):
    """Execute SQL and return a pandas DataFrame (empty DataFrame if no rows)."""
    return pd.read_sql_query(sql, conn, params=params)

def get_party_data(party_id: int):
    """Return sanction party data broken into sections. Return None if party_id not found."""
    if not DB_FILE.exists():
        logger.error(f"DBファイルが見つかりません: {DB_FILE}")
        return {"error": f"DB ファイル {DB_FILE} が見つかりません。"}

    logger.info(f"パーティデータの取得を開始: party_id={party_id}")
    conn = sqlite3.connect(DB_FILE)

    # Details
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
    JOIN ofac_party_list_link ll
          ON p.party_id = ll.party_id
    JOIN ofac_code_master cm_list
          ON ll.list_cd = cm_list.code_id
    LEFT JOIN ofac_party_program_link pl
          ON p.party_id = pl.party_id
    LEFT JOIN ofac_code_master cm_prog
          ON pl.program_cd = cm_prog.code_id
    WHERE p.party_id = ?
    GROUP BY p.party_id;
    """
    details_df = query_to_df(conn, details_sql, (party_id,))

    if details_df.empty:
        logger.warning(f"パーティデータが見つかりません: party_id={party_id}")
        conn.close()
        return None

    # Identifications
    ident_sql = """
    SELECT 
        attribute_type_cd AS "Type",
        attribute_value   AS "ID / Information"
    FROM ofac_party_attribute
    WHERE party_id = ?
      AND attribute_type_cd IN ('Website', 'Additional Sanctions Information -')
    ORDER BY attribute_type_cd, attribute_value;
    """
    ident_df = query_to_df(conn, ident_sql, (party_id,))

    # Aliases
    alias_sql = """
    SELECT 
        'a.k.a.'        AS "Type",
        'weak'          AS "Category",
        name_text       AS "Name"
    FROM ofac_party_name
    WHERE party_id = ?
      AND REPLACE(LOWER(name_type_cd), '.', '') = 'aka'
    ORDER BY name_text;
    """
    alias_df = query_to_df(conn, alias_sql, (party_id,))

    # Addresses
    addr_sql = """
    SELECT 
        address_line           AS "Address",
        city                   AS "City",
        ''                     AS "State / Province",
        postal_code            AS "Postal Code",
        cm.code_value          AS "Country"
    FROM ofac_party_address ad
    LEFT JOIN ofac_code_master cm ON ad.country_cd = cm.code_id
    WHERE ad.party_id = ?;
    """
    addr_df = query_to_df(conn, addr_sql, (party_id,))

    conn.close()
    logger.info(f"パーティデータの取得に成功: party_id={party_id}")
    return {
        "details": details_df.to_dict(orient="records")[0],
        "identifications": ident_df.to_dict(orient="records"),
        "aliases": alias_df.to_dict(orient="records"),
        "addresses": addr_df.to_dict(orient="records")
    }

def search_party_by_name(name: str):
    """Return sanction party data by name search. Return empty list if no matches found."""
    if not DB_FILE.exists():
        logger.error(f"DBファイルが見つかりません: {DB_FILE}")
        return {"error": f"DB ファイル {DB_FILE} が見つかりません。"}

    logger.info(f"パーティ名による検索を開始: name={name}")
    conn = sqlite3.connect(DB_FILE)

    # 名前による検索SQL
    search_sql = """
    SELECT DISTINCT
        p.party_id,
        n.name_text                 AS "Entity Name",
        p.party_type_cd             AS "Type",
        cm_list.code_value          AS "List",
        GROUP_CONCAT(cm_prog.code_value, '; ') AS "Program"
    FROM ofac_sanctioned_party p
    JOIN ofac_party_name n
          ON p.party_id = n.party_id
         AND n.is_primary_flg = 1
         AND n.name_type_cd = 'FORMAL'
    JOIN ofac_party_list_link ll
          ON p.party_id = ll.party_id
    JOIN ofac_code_master cm_list
          ON ll.list_cd = cm_list.code_id
    LEFT JOIN ofac_party_program_link pl
          ON p.party_id = pl.party_id
    LEFT JOIN ofac_code_master cm_prog
          ON pl.program_cd = cm_prog.code_id
    WHERE LOWER(n.name_text) LIKE LOWER(?)
    GROUP BY p.party_id, n.name_text, p.party_type_cd, cm_list.code_value
    ORDER BY n.name_text;
    """
    search_df = query_to_df(conn, search_sql, (f'%{name}%',))
    conn.close()

    if search_df.empty:
        logger.info(f"検索結果が見つかりません: name={name}")
        return []

    logger.info(f"検索結果を返却: 件数={len(search_df)}")
    return search_df.to_dict(orient="records")

@app.route('/ofacParty', methods=['GET'])
def ofac_party():
    party_id_param = request.args.get('partyId')
    logger.info(f"OFACパーティAPIリクエスト受信: partyId={party_id_param}")

    # Validate partyId
    if not party_id_param or not party_id_param.isdigit():
        logger.warning(f"無効なpartyIdパラメータ: {party_id_param}")
        return jsonify({
            "resultCd": False,
            "message": "partyId を数値で指定してください"
        }), 400

    party_id = int(party_id_param)

    # Fetch data
    result = get_party_data(party_id)

    # Database file missing
    if isinstance(result, dict) and "error" in result:
        logger.error(f"DBファイルエラー: {result['error']}")
        return jsonify({"resultCd": False, "message": result["error"]}), 500

    # Not found
    if result is None:
        logger.warning(f"パーティデータが見つかりません: party_id={party_id}")
        return jsonify({
            "resultCd": False,
            "message": f"party_id={party_id} のデータが見つかりません"
        }), 404

    # Success
    logger.info(f"OFACパーティAPIレスポンス送信: party_id={party_id}")
    return jsonify({"resultCd": True, "data": result})

@app.route('/ofacParty/search', methods=['GET'])
def search_party():
    name_param = request.args.get('name')
    logger.info(f"OFACパーティ検索APIリクエスト受信: name={name_param}")

    # Validate name parameter
    if not name_param or len(name_param.strip()) < 2:
        logger.warning(f"無効なnameパラメータ: {name_param}")
        return jsonify({
            "resultCd": False,
            "message": "name パラメータを2文字以上で指定してください"
        }), 400

    # Search data
    result = search_party_by_name(name_param)

    # Database file missing
    if isinstance(result, dict) and "error" in result:
        logger.error(f"DBファイルエラー: {result['error']}")
        return jsonify({"resultCd": False, "message": result["error"]}), 500

    # Success
    logger.info(f"OFACパーティ検索APIレスポンス送信: 結果件数={len(result)}")
    return jsonify({"resultCd": True, "data": result})

if __name__ == '__main__':
    import hypercorn.asyncio
    import asyncio
    logger.info("OFAC APIサーバーを起動しています…")
    config = hypercorn.Config()
    config.bind = ["0.0.0.0:10000"]
    config.workers = 1
    config.keep_alive_timeout = 65
    logger.info(f"サーバー設定: bind={config.bind}, workers={config.workers}")
    asyncio.run(hypercorn.asyncio.serve(app, config))
