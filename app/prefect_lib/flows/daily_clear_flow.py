from datetime import datetime
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect.task_runners import SequentialTaskRunner
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.end_task import end_task
from prefect_lib.flows.common_flow import common_flow
from prefect_lib.tasks.mongo_delete_task import mongo_delete_task
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.scraped_from_response_model import ScrapedFromResponseModel


@flow(
    flow_run_name='[MONGO_001] Daily clear flow',
    task_runner=SequentialTaskRunner())
@common_flow
def daily_clear_flow():

    # ロガー取得
    logger = get_run_logger()   # PrefectLogAdapter
    # 初期処理
    init_task_result: PrefectFuture = init_task.submit()

    if init_task_result.get_state().is_completed():
        mongo: MongoModel = init_task_result.result()

        try:
            # ScrapedFromResponseModelのデータを一括削除
            mongo_delete_task(mongo, datetime.min, datetime.max, [ScrapedFromResponseModel.COLLECTION_NAME], True)

        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f'=== {e}')
        finally:
            # 後続の処理を実行する
            end_task(mongo)

    else:
        logger.error(f'=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。')
