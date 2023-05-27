import os
import logging
import tempfile
from logging import Logger
from datetime import datetime
from shared.settings import TIMEZONE, DATA_DIR__LOGS

# 開始時間
START_TIME = datetime.now().astimezone(TIMEZONE)

# prefectのlogger本体にファイルハンドラーを付与する。※flow_logger/task_loggerの内容をログファイルに保存させる。
LOG_FILE_PATH = tempfile.NamedTemporaryFile(
    prefix=f'prefect_log_{START_TIME.strftime("%Y-%m-%d %H-%M-%S")}_',
    dir=DATA_DIR__LOGS,
    ).name

# scrapy側のロガーへ上記の添付ファイルパス環境変数を通して連携する。
os.environ['SCRAPY__LOG_FILE'] = LOG_FILE_PATH
