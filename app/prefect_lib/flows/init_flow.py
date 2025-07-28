import logging
import os
from logging import Logger
from typing import Any

from prefect import get_run_logger
from prefect_lib.flows import LOG_FILE_PATH
from shared.settings import LOG_DATEFORMAT, LOG_FORMAT


def init_flow():
    ####################################################
    # 初期処理                                         #
    ####################################################
    file_handler = logging.FileHandler(LOG_FILE_PATH)
    file_handler.setFormatter(logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_DATEFORMAT))
    prefect_logger: Logger = logging.getLogger("prefect")
    prefect_logger.addHandler(file_handler)
    prefect_logger.setLevel(logging.DEBUG)

    # DEBUGレベルの場合、余計な"aiosqlite","httpcore"ロガーのログ出力を抑制する。
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    # 不要なhttpxのログを抑制
    logging.getLogger("httpx").setLevel(logging.WARNING)
    # 不要なcharset_normalizerのログを抑制
    logging.getLogger("charset_normalizer").setLevel(logging.WARNING)
    # 不要なwebsockets.clientのログを抑制
    logging.getLogger("websockets.client").setLevel(logging.WARNING)
    # 不要なprefect.events.clientsのログを抑制
    logging.getLogger("prefect.events.clients").setLevel(logging.WARNING)
    logger = get_run_logger()  # PrefectLogAdapter
    logger.info(f'=== 保存用ログファイル: {os.environ.get("SCRAPY__LOG_FILE")}')
