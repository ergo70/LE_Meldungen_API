#!/usr/bin/env python3

import sqlite3
import uvicorn
from typing import List, Union
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from slowapi.errors import RateLimitExceeded
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from datetime import datetime

con = sqlite3.connect('file:./LEMeldungen.db?mode=ro', uri=True)
select_part = """pzn, enr, meldungsart, beginn, ende, datum_der_letzten_meldung, art_des_grundes, arzneimittelbezeichnung, atc_code, wirkstoffe, krankenhausrelevant, zulassungsinhaber, telefon, email, grund, anmerkung_zum_grund, alternativpraeparat, datum_der_erstmeldung, info_an_fachkreise, created"""

limiter = Limiter(key_func=get_remote_address)
app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


class LEMeldung(BaseModel):
    PZN: Union[str, None] = None
    ENR: Union[str, None] = None
    Meldungsart: Union[str, None] = None
    Beginn: str
    Ende: str
    Datum_der_letzen_Meldung: Union[str, None] = None
    Art_des_Grundes: str
    Arzneimittelbezeichnung: str
    ATC_Code: str
    Wirkstoffe: str
    Krankenhausrelevant: bool
    Zulassungsinhaber: str
    Telefon: Union[str, None] = None
    EMail: Union[str, None] = None
    Grund: str
    Anmerkung_zum_Grund: Union[str, None] = None
    Alternativpraeparat: Union[str, None] = None
    Datum_der_Erstmeldung: str
    Info_an_Fachkreise: str
    Erzeugt_am: str


class LEMeldungen(BaseModel):
    le_meldungen: List[LEMeldung] = []


@app.get("/auswahl/", response_model=LEMeldungen)
@limiter.limit("6/minute")
async def filter(request: Request, erzeugt_von: Union[str, None] = None, erzeugt_bis: Union[str, None] = None, pzn: Union[str, None] = None, enr: Union[str, None] = None, meldungsart: Union[str, None] = None, beginn_von: Union[str, None] = None, beginn_bis: Union[str, None] = None, ende_von: Union[str, None] = None, ende_bis: Union[str, None] = None, letzte_meldung_von: Union[str, None] = None, letzte_meldung_bis: Union[str, None] = None, arzneimittel: Union[str, None] = None, atc_code: Union[str, None] = None, wirkstoffe: Union[str, None] = None, krankenhausrelevant: Union[bool, None] = None) -> LEMeldungen:
    and_part = []
    params = []

    if erzeugt_von:
        and_part.append("created >= ?")
        params.append(erzeugt_von)

    if erzeugt_bis:
        and_part.append("created <= ?")
        params.append(erzeugt_bis)

    if pzn:
        and_part.append("pzn LIKE ?")
        params.append('%'+pzn+'%')

    if enr:
        and_part.append("enr LIKE ?")
        params.append('%'+enr+'%')

    if meldungsart:
        and_part.append("meldungsart LIKE ?")
        params.append('%'+meldungsart+'%')

    if beginn_von:
        and_part.append("beginn >= ?")
        params.append(beginn_von)

    if beginn_bis:
        and_part.append("beginn <= ?")
        params.append(beginn_bis)

    if ende_von:
        and_part.append("ende >= ?")
        params.append(ende_von)

    if ende_bis:
        and_part.append("ende <= ?")
        params.append(ende_bis)

    if letzte_meldung_von:
        and_part.append("datum_der_letzten_meldung >= ?")
        params.append(letzte_meldung_von)

    if letzte_meldung_bis:
        and_part.append("datum_der_letzten_meldung <= ?")
        params.append(letzte_meldung_bis)

    if arzneimittel:
        and_part.append("arzneimittelbezeichnung LIKE ?")
        params.append('%'+arzneimittel+'%')

    if atc_code:
        and_part.append("atc_code LIKE ?")
        params.append('%'+atc_code+'%')

    if wirkstoffe:
        and_part.append("wirkstoffe LIKE ?")
        params.append('%'+wirkstoffe+'%')

    if krankenhausrelevant is not None:
        and_part.append("krankenhausrelevant = ?")
        params.append(1 if krankenhausrelevant else 0)

    if not and_part:
        raise HTTPException(
            status_code=400, detail="No query parameters given.")

    cur = con.cursor()

    sql = """SELECT {} FROM le_meldungen WHERE {};""".format(
        select_part, ' AND '.join(and_part))

    cur.execute(sql,
                params)

    result = LEMeldungen(le_meldungen=[LEMeldung(PZN=r[0], ENR=r[1], Meldungsart=r[2], Beginn=r[3], Ende=r[4], Datum_der_letzten_Meldung=r[5], Art_des_Grundes=r[6], Arzneimittelbezeichnung=r[7], ATC_Code=r[8], Wirkstoffe=r[9],
                                                 Krankenhausrelevant=(r[10] == 1), Zulassungsinhaber=r[11], Telefon=r[12], EMail=r[13], Grund=r[14], Anmerkung_zum_Grund=r[15], Alternativpraeparat=r[16], Datum_der_Erstmeldung=r[17], Info_an_Fachkreise=r[18], Erzeugt_am=r[19]) for r in cur])

    return result


@app.get("/heute/", response_model=LEMeldungen)
@limiter.limit("3/minute")
async def today(request: Request) -> LEMeldungen:
    now = datetime.now()
    today = now.strftime('%Y-%m-%d')

    cur = con.cursor()

    sql = """SELECT {} FROM le_meldungen WHERE created = ?;""".format(
        select_part)

    cur.execute(sql,
                [today])

    result = LEMeldungen(le_meldungen=[LEMeldung(PZN=r[0], ENR=r[1], Meldungsart=r[2], Beginn=r[3], Ende=r[4], Datum_der_letzten_Meldung=r[5], Art_des_Grundes=r[6], Arzneimittelbezeichnung=r[7], ATC_Code=r[8], Wirkstoffe=r[9],
                                                 Krankenhausrelevant=(r[10] == 1), Zulassungsinhaber=r[11], Telefon=r[12], EMail=r[13], Grund=r[14], Anmerkung_zum_Grund=r[15], Alternativpraeparat=r[16], Datum_der_Erstmeldung=r[17], Info_an_Fachkreise=r[18], Erzeugt_am=r[19]) for r in cur])

    return result


@app.get("/alle/", response_model=LEMeldungen)
@limiter.limit("1/minute")
async def all(request: Request) -> LEMeldungen:
    cur = con.cursor()

    cur.execute(
        """SELECT {} FROM le_meldungen;""".format(select_part))

    result = LEMeldungen(le_meldungen=[LEMeldung(PZN=r[0], ENR=r[1], Meldungsart=r[2], Beginn=r[3], Ende=r[4], Datum_der_letzten_Meldung=r[5], Art_des_Grundes=r[6], Arzneimittelbezeichnung=r[7], ATC_Code=r[8], Wirkstoffe=r[9],
                                                 Krankenhausrelevant=(r[10] == 1), Zulassungsinhaber=r[11], Telefon=r[12], EMail=r[13], Grund=r[14], Anmerkung_zum_Grund=r[15], Alternativpraeparat=r[16], Datum_der_Erstmeldung=r[17], Info_an_Fachkreise=r[18], Erzeugt_am=r[19]) for r in cur])

    return result

if __name__ == "__main__":
    uvicorn.run("le_meldungen_API:app", host='0.0.0.0', port=443, reload=False,
                ssl_keyfile="", ssl_certfile="")
