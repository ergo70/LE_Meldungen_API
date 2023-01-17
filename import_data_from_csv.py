#!/usr/bin/env python3

import csv
import sqlite3
from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from glob import glob
from os import remove

options = Options()
options.headless = True
driver = webdriver.Firefox(options=options, executable_path=r'./geckodriver')

driver.get(
    "https://anwendungen.pharmnet-bund.de/lieferengpassmeldungen/faces/public/meldungen.xhtml")
assert "Lieferengpassmeldungen" in driver.title
elem = driver.find_element(By.NAME, "meldungenForm:j_idt175")
elem.click()
sleep(30)
driver.close()

SOURCE = glob("../Downloads/*.csv")[0]

with open('{}'.format(SOURCE), 'r', encoding="ISO-8859-1") as fin:
    to_db = []
    # csv.DictReader uses first line in file for column headings by default
    dr = csv.DictReader(fin, delimiter=';')
    for r in dr:
        row = [SOURCE]+[v for v in r.values()]
        if row[1] == '*':
            row[1] = 'Altdatenübernahme war nicht möglich'
        for i in [13, 14, 16, 17]:
            if row[i] in ("N/A", "-", "n.a.", "'-"):
                row[i] = None
        for i in [4, 5, 6, 18]:
            d = row[i].split('.')
            row[i] = d[2] + '-' + d[1] + '-' + d[0]

        to_db.append(tuple(row))

remove(SOURCE)

con = sqlite3.connect(
    'file:./LEMeldungen.db?mode=rw', uri=True)
cur = con.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS le_import (col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, co13,col14,col15,col16,col17,col18,col19, col20);""")
cur.execute("""CREATE TABLE IF NOT EXISTS le_meldungen (source TEXT NOT NULL,pzn TEXT NULL,enr TEXT NULL,meldungsart TEXT NULL,beginn TEXT NOT NULL,ende TEXT NOT NULL,datum_der_letzten_meldung TEXT NULL,art_des_grundes TEXT NOT NULL,arzneimittelbezeichnung TEXT NOT NULL,atc_code TEXT NOT NULL,wirkstoffe TEXT NOT NULL,krankenhausrelevant INTEGER NOT NULL,zulassungsinhaber TEXT NOT NULL,telefon TEXT NULL,email TEXT NULL,grund TEXT NOT NULL,anmerkung_zum_grund TEXT NULL,alternativpraeparat TEXT NULL,datum_der_erstmeldung TEXT NOT NULL,info_an_fachkreise TEXT NOT NULL,created TEXT NOT NULL);""")
cur.execute(
    """CREATE INDEX IF NOT EXISTS le_meldungen_created_IDX ON le_meldungen (created DESC);""")

cur.executemany("""INSERT INTO le_import (col0, col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, co13,col14,col15,col16,col17,col18,col19, col20) VALUES (?,?, ?, ?,?,?,?,?,?,?,?,CASE WHEN ? = 'ja' THEN 1 ELSE 0 END,?,?,?,?,?,?,?,?,date('now'));""", to_db)
con.commit()
cur.execute("""INSERT INTO le_meldungen SELECT * FROM le_import;""")
cur.execute("""DROP TABLE IF EXISTS le_import;""")

con.commit()
con.close()
