from flask import Flask, request, jsonify
import sqlite3
import pandas as pd
from pathlib import Path

app = Flask(__name__)

DB_FILE = Path("ofac_demo.db")

def query_to_df(conn, sql: str, params=()):
    """Execute SQL and return a pandas DataFrame (empty DataFrame if no rows)."""
    return pd.read_sql_query(sql, conn, params=params)

def get_party_data(party_id: int):
    """Return sanction party data broken into sections. Return None if party_id not found."""
    if not DB_FILE.exists():
        return {"error": f"DB ファイル {DB_FILE} が見つかりません。"}

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

    return {
        "details": details_df.to_dict(orient="records")[0],
        "identifications": ident_df.to_dict(orient="records"),
        "aliases": alias_df.to_dict(orient="records"),
        "addresses": addr_df.to_dict(orient="records")
    }

@app.route('/ofacParty', methods=['GET'])
def ofac_party():
    party_id_param = request.args.get('partyId')

    # Validate partyId
    if not party_id_param or not party_id_param.isdigit():
        return jsonify({
            "resultCd": False,
            "message": "partyId を数値で指定してください"
        }), 400

    party_id = int(party_id_param)

    # Fetch data
    result = get_party_data(party_id)

    # Database file missing
    if isinstance(result, dict) and "error" in result:
        return jsonify({"resultCd": False, "message": result["error"]}), 500

    # Not found
    if result is None:
        return jsonify({
            "resultCd": False,
            "message": f"party_id={party_id} のデータが見つかりません"
        }), 404

    # Success
    return jsonify({"resultCd": True, "data": result})

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
