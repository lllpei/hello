#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OFAC サンプル DB から 1 つの制裁対象（party_id）を取得して
・概要 (Details)
・識別情報 (Identifications)
・別名 (Aliases)
・住所 (Addresses)
をJSON形式で返却するAPI。

使い方:
    python show_ofac_party.py 4639
"""
from fastapi import FastAPI, HTTPException
import sys
import sqlite3
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from pydantic import BaseModel

app = FastAPI()
DB_FILE = Path("ofac_demo.db")

class Details(BaseModel):
    Type: str
    Entity_Name: str
    List: str
    Program: str
    Remarks: str

class Identification(BaseModel):
    Type: str
    ID_Information: str

class Alias(BaseModel):
    Type: str
    Category: str
    Name: str

class Address(BaseModel):
    Address: str
    City: str
    State_Province: str
    Postal_Code: str
    Country: str

class PartyResponse(BaseModel):
    details: List[Details]
    identifications: List[Identification]
    aliases: List[Alias]
    addresses: List[Address]

def query_to_df(conn, sql, params=()):
    """SQL を実行して pandas.DataFrame を返す（空なら列だけ）"""
    return pd.read_sql_query(sql, conn, params=params)

@app.get("/party/{party_id}", response_model=PartyResponse)
async def get_party(party_id: int):
    if not DB_FILE.exists():
        raise HTTPException(status_code=404, detail=f"DB ファイル {DB_FILE} が見つかりません。")

    conn = sqlite3.connect(DB_FILE)

    try:
        # 1. Details ─────────────────────────────────────────
        details_sql = """
        SELECT 
            p.party_type_cd             AS "Type",
            n.name_text                 AS "Entity_Name",
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
            raise HTTPException(status_code=404, detail=f"指定された party_id {party_id} は見つかりません。")

        # 2. Identifications ────────────────────────────────
        ident_sql = """
        SELECT 
            attribute_type_cd AS "Type",
            attribute_value   AS "ID_Information"
        FROM ofac_party_attribute
        WHERE party_id = ?
          AND attribute_type_cd IN ('Website', 'Additional Sanctions Information -')
        ORDER BY attribute_type_cd, attribute_value;
        """
        ident_df = query_to_df(conn, ident_sql, (party_id,))

        # 3. Aliases ────────────────────────────────────────
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

        # 4. Addresses ──────────────────────────────────────
        addr_sql = """
        SELECT 
            address_line           AS "Address",
            city                   AS "City",
            ''                     AS "State_Province",
            postal_code            AS "Postal_Code",
            cm.code_value          AS "Country"
        FROM ofac_party_address ad
        LEFT JOIN ofac_code_master cm ON ad.country_cd = cm.code_id
        WHERE ad.party_id = ?;
        """
        addr_df = query_to_df(conn, addr_sql, (party_id,))

        return PartyResponse(
            details=details_df.to_dict('records'),
            identifications=ident_df.to_dict('records'),
            aliases=alias_df.to_dict('records'),
            addresses=addr_df.to_dict('records')
        )

    finally:
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
