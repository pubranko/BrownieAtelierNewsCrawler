from datetime import datetime
from typing import Any, Optional

from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect.states import State
from prefect.task_runners import SequentialTaskRunner
from prefect_lib.flows.init_flow import init_flow
from prefect_lib.tasks.end_task import end_task
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.sync_check_crawler_response_task import \
    sync_check_crawler_response_task
from prefect_lib.tasks.sync_check_news_clip_master_task import \
    sync_check_news_clip_master_task
from prefect_lib.tasks.sync_check_notice_result_task import \
    sync_check_notice_result_task


@flow(name="Crawl sync check flow", task_runner=SequentialTaskRunner())
def crawl_sync_check_flow(
    domain: Optional[str] = None,
    start_time_from: Optional[datetime] = None,
    start_time_to: Optional[datetime] = None,
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
            # crawl結果とcrawler_responseが同期しているかチェックする。
            (
                response_sync_list,
                response_async_list,
                response_async_domain_aggregate,
            ) = sync_check_crawler_response_task(
                mongo, domain, start_time_from, start_time_to
            )

            # crawler_responseとnews_clip_masterが同期しているかチェックする。
            (
                master_sync_list,
                master_async_list,
                master_async_domain_aggregate,
            ) = sync_check_news_clip_master_task(
                mongo, domain, start_time_from, start_time_to, response_sync_list
            )

            # 当分の間solrとの同期チェックは中止。solr側の本格開発が始まってから開放予定。
            # solr_sync_list, solr_async_list, solr_async_domain_aggregate = sync_check_solr_news_clip(
            #     mongo, domain, start_time_from, start_time_to, master_sync_list)
            solr_async_list = []
            solr_async_domain_aggregate = {}

            sync_check_notice_result_task(
                response_async_list,
                response_async_domain_aggregate,
                master_async_list,
                master_async_domain_aggregate,
                solr_async_list,
                solr_async_domain_aggregate,
            )

        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f"=== {e}")
        finally:
            # 後続の処理を実行する
            end_task(mongo)

    else:
        logger.error(f"=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。")
