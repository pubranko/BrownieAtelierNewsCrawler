import os
import sys
from collections.abc import Iterable
from typing import Final

sys.path.append(os.getcwd())
from shared.settings import DATA__DEBUG_FILE_DIR

path = os.getcwd()

LASTMOD: Final[str] = "lastmod"
LOC: Final[str] = "loc"


def start_request_debug_file_generate(spider_name: str, start_url: str, entries: Iterable, debug: bool):
    """
    サイトマップ、各カテゴリーの一覧ページ、XMLページなどの情報をデバック用に出力する。
    """
    if debug:
        path: str = os.path.join(DATA__DEBUG_FILE_DIR, "start_urls(" + spider_name + ").txt")
        with open(path, "a") as file:
            for entry in entries:
                entry: dict
                file.write(start_url + "," + str(entry[LOC]) + "," + str(entry[LASTMOD]) + "\n")
