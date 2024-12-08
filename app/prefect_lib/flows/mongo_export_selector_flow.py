from typing import Any
from datetime import date

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
from prefect_lib.flows.init_flow import init_flow
from prefect_lib.tasks.end_task import end_task
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.mongo_common_task import mongo_common_task
from prefect_lib.tasks.mongo_export_task import mongo_export_task


@flow(name="Mongo export selector flow")
def mongo_export_selector_flow(
    collections_name: list[str],
    prefix: str,  # export先のフォルダyyyy-mmの先頭に拡張した名前を付与する。
    suffix: str,  # export先のフォルダyyyy-mmの末尾に拡張した名前を付与する。
    period_date_from: date,  # 月次エクスポートを行うデータの基準年月日
    period_date_to: date,  # 月次エクスポートを行うデータの基準年月日
    crawler_response__registered: bool = False,  # crawler_responseの場合、登録済みになったレコードのみエクスポートする場合True、登録済み以外のレコードも含めてエクスポートする場合False
):
    init_flow()

    # ロガー取得
    logger = get_run_logger()  # PrefectLogAdapter
    # 初期処理
    init_task_instance: PrefectFuture = init_task.submit()
    # 実行結果が返ってくるまで待機し、戻り値を保存。 
    #   ※タスクのステータスをresultを受け取る前に判定してもPendingとなる。インスタンスのステータスはリアルタイムで更新されているので注意。
    init_task_result = init_task_instance.result()

    if init_task_instance.state.is_completed():
        mongo: MongoModel = init_task_result

        try:
            # mongo操作Flowの共通処理
            dir_path, period_datetime_from, period_datetime_to = mongo_common_task(
                prefix, suffix, period_date_from, period_date_to
            )

            mongo_export_task(
                mongo,
                dir_path,
                period_datetime_from,
                period_datetime_to,
                collections_name,
                crawler_response__registered,
            )

        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f"=== {e}")
        finally:
            # 後続の処理を実行する
            end_task(mongo)

    else:
        logger.error(f"=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。")
