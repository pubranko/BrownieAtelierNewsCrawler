from datetime import datetime
from typing import Any, Optional, Union

from BrownieAtelierMongo.collection_models.asynchronous_report_model import \
    AsynchronousReportModel
from BrownieAtelierMongo.collection_models.controller_model import \
    ControllerModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import \
    CrawlerLogsModel
from BrownieAtelierMongo.collection_models.crawler_response_model import \
    CrawlerResponseModel
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.news_clip_master_model import \
    NewsClipMasterModel
from BrownieAtelierMongo.collection_models.scraped_from_response_model import \
    ScrapedFromResponseModel
from BrownieAtelierMongo.collection_models.stats_info_collect_model import \
    StatsInfoCollectModel
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect.states import State
from prefect.task_runners import SequentialTaskRunner
from prefect_lib.flows.init_flow import init_flow
from prefect_lib.tasks.end_task import end_task
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.mongo_common_task import mongo_common_task
from prefect_lib.tasks.mongo_import_task import mongo_import_task


@flow(name="Mongo import selector flow", task_runner=SequentialTaskRunner())
def mongo_import_selector_flow(
    folder_name: str,
    collections_name: list[str],
):
    init_flow()

    # ロガー取得
    logger = get_run_logger()  # PrefectLogAdapter
    # 初期処理
    init_task_result: PrefectFuture = init_task.submit()

    any: Any = init_task_result.get_state()
    state: State = any
    if state.is_completed():
        mongo: MongoModel = init_task_result.result()

        try:
            # mongo操作Flowの共通処理
            mongo_import_task(mongo, folder_name, collections_name)

        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f"=== {e}")
        finally:
            # 後続の処理を実行する
            end_task(mongo)

    else:
        logger.error(f"=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。")
