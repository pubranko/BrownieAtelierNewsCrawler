from prefect import task
from prefect import get_run_logger
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from prefect_lib.flows import START_TIME

"""
mongoDBのインポートを行う。
・pythonのlistをpickle.loadsで復元しインポートする。
・対象のコレクションを選択できる。
・対象の年月を指定できる。範囲を指定した場合、月ごとにエクスポートを行う。
"""


@task
def init_task():
    """prefectの初期処理専用タスク"""

    logger = get_run_logger()
    logger.info(f"=== start_time : {START_TIME.isoformat()}")

    # mongoDB接続
    mongo = MongoModel(logger)

    return mongo
