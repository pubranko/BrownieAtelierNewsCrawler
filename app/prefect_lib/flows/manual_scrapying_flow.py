from typing import Optional
from datetime import datetime
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect.task_runners import SequentialTaskRunner
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.end_task import end_task
from prefect_lib.flows.common_flow import common_flow
from prefect_lib.tasks.scrapying_task import scrapying_task
from prefect_lib.tasks.news_clip_master_save_task import news_clip_master_save_task
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel


@flow(
    name='Manual scrapying flow',
    task_runner=SequentialTaskRunner())
@common_flow
def manual_scrapying_flow(
    domain: Optional[str],
    target_start_time_from: Optional[datetime],
    target_start_time_to: Optional[datetime],
    urls: Optional[list],
    following_processing_execution: bool,
):

    # ロガー取得
    logger = get_run_logger()   # PrefectLogAdapter
    # 初期処理
    init_task_result: PrefectFuture = init_task.submit()

    if init_task_result.get_state().is_completed():
        mongo: MongoModel = init_task_result.result()

        try:
            # 引数で指定されたクロール結果のスクレイピングを実施
            scrapying_task(mongo, domain, urls, target_start_time_from, target_start_time_to)

            if following_processing_execution:
                # 後続処理実施指定がある場合、スクレイピング結果をニュースクリップマスターへ保存
                news_clip_master_save_task(mongo, domain, target_start_time_from, target_start_time_to)

        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f'=== {e}')
        finally:
            # 後続の処理を実行する
            end_task(mongo)

    else:
        logger.error(f'=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。')
