import sqlite3
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

con = sqlite3.connect('file:./LEMeldungen.db?mode=ro', uri=True)
select_part = """pzn, enr, meldungsart, beginn, ende, datum_der_letzten_meldung, art_des_grundes, arzneimittelbezeichnung, atc_code, wirkstoffe, krankenhausrelevant, zulassungsinhaber, telefon, email, grund, anmerkung_zum_grund, alternativpraeparat, datum_der_erstmeldung, info_an_fachkreise, created"""


class LEMeldung(BaseModel):
    PZN: str | None = None
    ENR: str | None = None
    Meldungsart: str | None = None
    Beginn: str
    Ende: str
    Datum_der_letzen_Meldung: str | None = None
    Art_des_Grundes: str
    Arzneimittelbezeichnung: str
    ATC_Code: str
    Wirkstoffe: str
    Krankenhausrelevant: bool
    Zulassungsinhaber: str
    Telefon: str | None = None
    EMail: str | None = None
    Grund: str
    Anmerkung_zum_Grund: str | None = None
    Alternativpraeparat: str | None = None
    Datum_der_Erstmeldung: str
    Info_an_Fachkreise: str
    Erzeugt_am: str


@app.get("/all/")
async def find() -> list[LEMeldung]:
    cur = con.cursor()

    cur.execute(
        """SELECT {} FROM le_meldungen;""".format(select_part))

    result = [LEMeldung(PZN=r[0], ENR=r[1], Meldungsart=r[2], Beginn=r[3], Ende=r[4], Datum_der_letzten_Meldung=r[5], Art_des_Grundes=r[6], Arzneimittelbezeichnung=r[7], ATC_Code=r[8], Wirkstoffe=r[9],
                        Krankenhausrelevant=(r[10] == 1), Zulassungsinhaber=r[11], Telefon=r[12], EMail=r[13], Grund=r[14], Anmerkung_zum_Grund=r[15], Alternativpraeparat=r[16], Datum_der_Erstmeldung=r[17], Info_an_Fachkreise=r[18], Erzeugt_am=r[19]) for r in cur]

    return result


@app.get("/find/")
async def find(erzeugt_von: str | None = None, erzeugt_bis: str | None = None, pzn: str | None = None, enr: str | None = None, meldungsart: str | None = None, beginn_von: str | None = None, beginn_bis: str | None = None, ende_von: str | None = None, ende_bis: str | None = None, letzte_meldung_von: str | None = None, letzte_meldung_bis: str | None = None, arzneimittel: str | None = None, atc_code: str | None = None, wirkstoffe: str | None = None, krankenhausrelevant: bool | None = None) -> list[LEMeldung]:
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

    result = [LEMeldung(PZN=r[0], ENR=r[1], Meldungsart=r[2], Beginn=r[3], Ende=r[4], Datum_der_letzten_Meldung=r[5], Art_des_Grundes=r[6], Arzneimittelbezeichnung=r[7], ATC_Code=r[8], Wirkstoffe=r[9],
                        Krankenhausrelevant=(r[10] == 1), Zulassungsinhaber=r[11], Telefon=r[12], EMail=r[13], Grund=r[14], Anmerkung_zum_Grund=r[15], Alternativpraeparat=r[16], Datum_der_Erstmeldung=r[17], Info_an_Fachkreise=r[18], Erzeugt_am=r[19]) for r in cur]

    return result
