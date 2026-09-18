import os
import sys

from scrapy.spiders import Spider

sys.path.append(os.getcwd())
from shared.settings import DATA__DEBUG_FILE_DIR

path = os.getcwd()


def start_request_debug_file_init(spider: Spider, debug: bool):
    """
    サイトマップ、各カテゴリーの一覧ページ、XMLページなどの情報をデバック用に初期化（空ファイル化）する。
    """
    if debug:
        spider.logger.info(f"=== debugモード ON: {spider.name}")
        # デバック用のファイルを初期化
        path = os.path.join(DATA__DEBUG_FILE_DIR, "start_urls(" + spider.name + ").txt")
        with open(path, "w"):
            pass
