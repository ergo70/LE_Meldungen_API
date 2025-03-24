#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
This pulls the current CSV file from https://anwendungen.pharmnet-bund.de/lieferengpassmeldungen/faces/public/meldungen.xhtml

Usage: python3 get_current_csv.py
"""

import logging
from requests import get

__author__ = "Ernst-Georg Schmid"
__copyright__ = "Copyright 2023, 2025 Ernst-Georg Schmid"
__license__ = "MIT"
__version__ = "1.0.0"
__maintainer__ = "Ernst-Georg Schmid"
__status__ = "Demo"

logger = logging.getLogger(__name__)


def get_csv():
    try:
        csv = get(
            """https://anwendungen.pharmnet-bund.de/lieferengpassmeldungen/public/csv""")

        with open("""le_meldungen.csv""", 'w', encoding='utf-8') as fout:
            fout.write(csv.content.decode('iso-8859-1'))
    except Exception as e:
        logger.error(f"Error while downloading CSV: {e}")


if __name__ == "__main__":
    get_csv()
