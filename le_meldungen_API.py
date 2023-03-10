#!/usr/bin/env python3

import sqlite3
import uvicorn
import logging
from typing import List, Union
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel
from slowapi.errors import RateLimitExceeded
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
#from datetime import datetime

tags_metadata = [
    {
        "name": "all",
        "description": "Alle Lieferengpassmeldungen.",
    },
    {
        "name": "active",
        "description": "Nur aktive Lieferengpassmeldungen.",
    },
    {
        "name": "deleted",
        "description": "Nur gelöschte Lieferengpassmeldungen.",
    },
    {
        "name": "filter",
        "description": "Individuell gefilterte Lieferengpassmeldungen.",
    },
]


select_part = """pzn, enr, meldungsart, beginn, ende, datum_der_letzten_meldung, art_des_grundes, arzneimittelbezeichnung, atc_code, wirkstoffe, krankenhausrelevant, zulassungsinhaber, telefon, email, grund, anmerkung_zum_grund, alternativpraeparat, datum_der_erstmeldung, info_an_fachkreise, created"""

logger = logging.getLogger(__name__)

con = sqlite3.connect('file:./LEMeldungen.db?mode=ro', uri=True)

limiter = Limiter(key_func=get_remote_address)

app = FastAPI(title="BfArM Lieferengpass-Datenbank Demo API",
              description="FastAPI based API for the BfArM Lieferengpass Database at https://anwendungen.pharmnet-bund.de/lieferengpassmeldungen/faces/public/meldungen.xhtml", version="1.0.0", openapi_tags=tags_metadata)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(GZipMiddleware, minimum_size=1024)


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
    #Telefon: Union[str, None] = None
    #EMail: Union[str, None] = None
    Grund: str
    Anmerkung_zum_Grund: Union[str, None] = None
    Alternativpraeparat: Union[str, None] = None
    Datum_der_Erstmeldung: str
    Info_an_Fachkreise: str
    #Erzeugt_am: str


class LEMeldungen(BaseModel):
    Daten_aktualisiert_am: str
    Anzahl_Datensaetze: int
    LE_Meldungen: List[LEMeldung] = []


@app.get("/v1/le_meldungen/alle/", response_model=LEMeldungen, tags=['all'])
@limiter.limit("1/minute")
async def all(request: Request) -> LEMeldungen:
    daa = None
    result = []
    cur = con.cursor()

    cur.execute(
        """SELECT {} FROM le_meldungen;""".format(select_part))

    for r in cur:
        daa = r[19]
        result += (LEMeldung(PZN=r[0], ENR=r[1], Meldungsart=r[2], Beginn=r[3], Ende=r[4], Datum_der_letzten_Meldung=r[5], Art_des_Grundes=r[6], Arzneimittelbezeichnung=r[7], ATC_Code=r[8], Wirkstoffe=r[9],
                             Krankenhausrelevant=(r[10] == 1), Zulassungsinhaber=r[11], Grund=r[14], Anmerkung_zum_Grund=r[15], Alternativpraeparat=r[16], Datum_der_Erstmeldung=r[17], Info_an_Fachkreise=r[18]),)

    result = LEMeldungen(Daten_aktualisiert_am=daa,
                         Anzahl_Datensaetze=len(result), LE_Meldungen=result)

    logger.info("all()")

    return result


@app.get("/v1/le_meldungen/aktiv/", response_model=LEMeldungen, tags=['active'])
@limiter.limit("1/minute")
async def all_active(request: Request) -> LEMeldungen:
    daa = None
    result = []
    cur = con.cursor()

    cur.execute(
        """SELECT {} FROM le_meldungen WHERE meldungsart != 'Löschmeldung';""".format(select_part))

    for r in cur:
        daa = r[19]
        result += (LEMeldung(PZN=r[0], ENR=r[1], Meldungsart=r[2], Beginn=r[3], Ende=r[4], Datum_der_letzten_Meldung=r[5], Art_des_Grundes=r[6], Arzneimittelbezeichnung=r[7], ATC_Code=r[8], Wirkstoffe=r[9],
                             Krankenhausrelevant=(r[10] == 1), Zulassungsinhaber=r[11], Grund=r[14], Anmerkung_zum_Grund=r[15], Alternativpraeparat=r[16], Datum_der_Erstmeldung=r[17], Info_an_Fachkreise=r[18]),)

    result = LEMeldungen(Daten_aktualisiert_am=daa,
                         Anzahl_Datensaetze=len(result), LE_Meldungen=result)

    logger.info("all_active()")

    return result


@app.get("/v1/le_meldungen/geloescht/", response_model=LEMeldungen, tags=['deleted'])
@limiter.limit("1/minute")
async def all_deleted(request: Request) -> LEMeldungen:
    daa = None
    result = []
    cur = con.cursor()

    cur.execute(
        """SELECT {} FROM le_meldungen WHERE meldungsart = 'Löschmeldung';""".format(select_part))

    for r in cur:
        daa = r[19]
        result += (LEMeldung(PZN=r[0], ENR=r[1], Meldungsart=r[2], Beginn=r[3], Ende=r[4], Datum_der_letzten_Meldung=r[5], Art_des_Grundes=r[6], Arzneimittelbezeichnung=r[7], ATC_Code=r[8], Wirkstoffe=r[9],
                             Krankenhausrelevant=(r[10] == 1), Zulassungsinhaber=r[11], Grund=r[14], Anmerkung_zum_Grund=r[15], Alternativpraeparat=r[16], Datum_der_Erstmeldung=r[17], Info_an_Fachkreise=r[18]),)

    result = LEMeldungen(Daten_aktualisiert_am=daa,
                         Anzahl_Datensaetze=len(result), LE_Meldungen=result)

    logger.info("all_deleted()")

    return result


@app.get("/v1/le_meldungen/auswahl/", response_model=LEMeldungen, tags=['filter'])
@limiter.limit("6/minute")
async def filter(request: Request, pzn: Union[str, None] = None, enr: Union[str, None] = None, meldungsart: Union[str, None] = None, beginn_von: Union[str, None] = None, beginn_bis: Union[str, None] = None, ende_von: Union[str, None] = None, ende_bis: Union[str, None] = None, letzte_meldung_von: Union[str, None] = None, letzte_meldung_bis: Union[str, None] = None, arzneimittel: Union[str, None] = None, atc_code: Union[str, None] = None, wirkstoffe: Union[str, None] = None, krankenhausrelevant: Union[bool, None] = None) -> LEMeldungen:
    daa = None
    and_part = []
    params = []
    result = []

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

    for r in cur:
        daa = r[19]
        result += (LEMeldung(PZN=r[0], ENR=r[1], Meldungsart=r[2], Beginn=r[3], Ende=r[4], Datum_der_letzten_Meldung=r[5], Art_des_Grundes=r[6], Arzneimittelbezeichnung=r[7], ATC_Code=r[8], Wirkstoffe=r[9],
                             Krankenhausrelevant=(r[10] == 1), Zulassungsinhaber=r[11], Grund=r[14], Anmerkung_zum_Grund=r[15], Alternativpraeparat=r[16], Datum_der_Erstmeldung=r[17], Info_an_Fachkreise=r[18]),)

    result = LEMeldungen(Daten_aktualisiert_am=daa,
                         Anzahl_Datensaetze=len(result), LE_Meldungen=result)

    logger.info("filter()")

    return result

if __name__ == "__main__":
    uvicorn.run("le_meldungen_API:app", host='0.0.0.0', port=8443, reload=False,
                ssl_keyfile="", ssl_certfile="")
