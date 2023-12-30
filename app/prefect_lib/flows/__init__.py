import os
import logging
import tempfile
from typing import Any
from logging import Logger
from datetime import datetime
# from prefect import get_run_logger

from shared.settings import TIMEZONE, DATA__LOGS
from shared.settings import LOG_FORMAT, LOG_DATEFORMAT
from prefect_lib.flows import *

# from prefect_lib.flows import LOG_FILE_PATH

logging.basicConfig(level=logging.INFO)

# 開始時間
START_TIME = datetime.now().astimezone(TIMEZONE)

# prefectのlogger本体にファイルハンドラーを付与する。※flow_logger/task_loggerの内容をログファイルに保存させる。
LOG_FILE_PATH = tempfile.NamedTemporaryFile(
    prefix=f'prefect_log_{START_TIME.strftime("%Y-%m-%d %H-%M-%S")}_',
    dir=DATA__LOGS,
    ).name

# scrapy側のロガーへ上記の添付ファイルパス環境変数を通して連携する。
os.environ['SCRAPY__LOG_FILE'] = LOG_FILE_PATH
# file_handler = logging.FileHandler(LOG_FILE_PATH)
# file_handler.setFormatter(logging.Formatter(
#     fmt=LOG_FORMAT, datefmt=LOG_DATEFORMAT))

# prefect_logger: Logger = logging.getLogger('prefect')
# prefect_logger.addHandler(file_handler)
# prefect_logger.setLevel(logging.DEBUG)

# logger = get_run_logger()   # PrefectLogAdapter
# prefect_logger.info(f'=== 保存用ログファイル: {os.environ.get("SCRAPY__LOG_FILE")}')
