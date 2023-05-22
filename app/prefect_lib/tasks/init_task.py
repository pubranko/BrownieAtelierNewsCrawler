import logging
import tempfile
from logging import Logger
from datetime import datetime
from prefect import task
from prefect import get_run_logger
from prefect.states import Crashed, Pending, exception_to_failed_state, Failed, Running, Completed
from prefect.exceptions import FailedRun
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from shared.settings import TIMEZONE, LOG_FORMAT, LOG_DATEFORMAT


'''
mongoDBのインポートを行う。
・pythonのlistをpickle.loadsで復元しインポートする。
・対象のコレクションを選択できる。
・対象の年月を指定できる。範囲を指定した場合、月ごとにエクスポートを行う。
'''


@task
def init_task():
    '''prefectの初期処理専用タスク'''
    # 開始時間
    start_time = datetime.now().astimezone(TIMEZONE)

    # prefectのlogger本体にファイルハンドラーを付与する。※flow_logger/task_loggerの内容をログファイルに保存させる。
    log_file_path = tempfile.NamedTemporaryFile(prefix='prefect_log_'+start_time.strftime('%Y-%m-%d %H:%M:%S')+'_').name
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(logging.Formatter(
        fmt=LOG_FORMAT, datefmt=LOG_DATEFORMAT))
    prefect_logger: Logger = logging.getLogger('prefect')
    prefect_logger.addHandler(file_handler)
    prefect_logger.setLevel(logging.DEBUG)

    logger = get_run_logger()
    logger.info(f'=== start_time : {start_time.isoformat()}')

    # mongoDB接続
    mongo = MongoModel(prefect_logger)

    return start_time, log_file_path, mongo

