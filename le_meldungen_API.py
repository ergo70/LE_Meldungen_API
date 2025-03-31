#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This is the missing API for the BfArM Lieferengpass Database: https://anwendungen.pharmnet-bund.de/lieferengpassmeldungen/faces/public/meldungen.xhtml

Usage: uvicorn le_meldungen_API:app
"""

import duckdb
import uvicorn
import logging
from datetime import date
from typing import List, Optional
from fastapi import FastAPI, Request
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel
from slowapi.errors import RateLimitExceeded
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address


__author__ = "Ernst-Georg Schmid"
__copyright__ = "Copyright 2023, 2025 Ernst-Georg Schmid"
__license__ = "MIT"
__version__ = "2.0.0"
__maintainer__ = "Ernst-Georg Schmid"
__status__ = "Demo"


tags_metadata = [
    {
        "name": "filter",
        "description": "Individuell gefilterte Lieferengpassmeldungen.",
    }
]

# Arzneimittlbezeichnung is actually a typo in the CSV file
SELECT_PART = """"PZN","ENR","Bearbeitungsnummer","Referenzierte Erstmeldung","Meldungsart","Beginn","Ende","Datum der letzten Meldung","Art des Grundes","Arzneimittlbezeichnung" AS "AM-Bezeichnung","Atc Code" AS "ATC","Wirkstoffe","Krankenhausrelevant" AS "KKH-relevant","Zulassungsinhaber","Telefon","E-Mail","Grund","Anm. zum Grund","Alternativpräparat","Datum der Erstmeldung","Info an Fachkreise","Darreichungsform\""""

logger = logging.getLogger(__name__)

con = duckdb.connect(':memory:')
le_meldungen_rel = con.read_csv("""le_meldungen.csv""", sep=';', header=True)

limiter = Limiter(key_func=get_remote_address)

app = FastAPI(title="BfArM Lieferengpass-Datenbank Demo API",
              description="FastAPI based API for the BfArM Lieferengpass Database at https://anwendungen.pharmnet-bund.de/lieferengpassmeldungen/faces/public/meldungen.xhtml", version=__version__, openapi_tags=tags_metadata)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(GZipMiddleware, minimum_size=1024)


class LEMeldung(BaseModel):
    PZN: str = None
    ENR: List[str] = None
    Bearbeitungsnummer: str = None
    Referenzierte_Erstmeldung: str = None
    Meldungsart: str = None
    Beginn: date = None
    Ende: date = None
    Datum_der_letzten_Meldung: date = None
    Art_des_Grundes: str = None
    AM_Bezeichnung: str = None
    ATC: str = None
    Wirkstoffe: str = None
    KKH_relevant: str = None
    Zulassungsinhaber: str = None
    Telefon: str = None
    EMail: str = None
    Grund: str = None
    Anmerkung_zum_Grund: str = None
    Alternativpraeparat: str = None
    Datum_der_Erstmeldung: date = None
    Info_an_Fachkreise: str = None
    Darreichungsform: List[str] = None


class LEMeldungen(BaseModel):
    Anzahl_Datensaetze: int
    LE_Meldungen: List[LEMeldung] = []


@app.get("/api/v1/le_meldungen/filter", response_model=LEMeldungen, tags=['filter'])
@limiter.limit("100/minute")
async def filter(request: Request, pzn: Optional[str] = None, enr: Optional[str] = None, meldungsart: Optional[str] = None, beginn_ab: Optional[date] = None, beginn_bis: Optional[date] = None, ende_ab: Optional[date] = None, ende_bis: Optional[date] = None, letzte_meldung_ab: Optional[date] = None, letzte_meldung_bis: Optional[date] = None, am_bezeichnung: Optional[str] = None, atc: Optional[str] = None, wirkstoffe: Optional[str] = None, kkh_relevant: Optional[str] = None) -> LEMeldungen:
    and_part = []
    params = []
    result = []

    if pzn:
        and_part.append(""""PZN" ILIKE ?""")
        params.append('%'+pzn+'%')

    if enr:
        and_part.append(""""ENR" ILIKE ?""")
        params.append('%'+enr+'%')

    if meldungsart:
        and_part.append(""""Meldungsart" ILIKE ?""")
        params.append('%'+meldungsart+'%')

    if beginn_ab:
        and_part.append(""""Beginn" >= ?""")
        params.append(beginn_ab)

    if beginn_bis:
        and_part.append(""""Beginn" <= ?""")
        params.append(beginn_bis)

    if ende_ab:
        and_part.append(""""Ende" >= ?""")
        params.append(ende_ab)

    if ende_bis:
        and_part.append(""""Ende" <= ?""")
        params.append(ende_bis)

    if letzte_meldung_ab:
        and_part.append(""""Datum der letzten Meldung" >= ?""")
        params.append(letzte_meldung_ab)

    if letzte_meldung_bis:
        and_part.append(""""Datum der letzten Meldung" <= ?""")
        params.append(letzte_meldung_bis)

    if am_bezeichnung:
        and_part.append(""""AM-Bezeichnung" ILIKE ?""")
        params.append('%'+am_bezeichnung+'%')

    if atc:
        and_part.append(""""ATC" ILIKE ?""")
        params.append('%'+atc+'%')

    if wirkstoffe:
        and_part.append(""""Wirkstoffe" ILIKE ?""")
        params.append('%'+wirkstoffe+'%')

    if kkh_relevant:
        and_part.append(""""KKH-relevant" ILIKE ?""")
        params.append('%'+kkh_relevant+'%')

    if not and_part:
        and_part.append("TRUE")

    for r in con.execute("""SELECT {} FROM le_meldungen_rel WHERE {};""".format(
            SELECT_PART, ' AND '.join(and_part)), params).fetchall():

        result += (LEMeldung(PZN=r[0], ENR=r[1].replace(' ', '').split(',') if r[1] else None, Bearbeitungsnummer=r[2], Referenzierte_Erstmeldung=r[3], Meldungsart=r[4], Beginn=r[5], Ende=r[6], Datum_der_letzten_Meldung=r[7], Art_des_Grundes=r[8], AM_Bezeichnung=r[9], ATC=r[10], Wirkstoffe=r[11],
                             KKH_relevant=r[12].capitalize(), Zulassungsinhaber=r[13], Telefon=r[14], EMail=r[15], Grund=r[16], Anmerkung_zum_Grund=r[17], Alternativpraeparat=r[18], Datum_der_Erstmeldung=r[19], Info_an_Fachkreise=r[20], Darreichungsform=r[21].replace(' ', '').split(',') if r[21] else None),)

    result = LEMeldungen(
        Anzahl_Datensaetze=len(result), LE_Meldungen=result)

    logger.info("filter()")

    return result

if __name__ == "__main__":
    uvicorn.run("le_meldungen_API:app", host='127.0.0.1',
                port=8080, reload=True)
