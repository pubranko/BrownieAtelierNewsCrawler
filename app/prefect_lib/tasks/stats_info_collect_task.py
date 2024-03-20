from typing import Any
from datetime import datetime
from prefect import task, get_run_logger
from pymongo.cursor import Cursor

from prefect_lib.flows import START_TIME
from prefect_lib.data_models.stats_info_collect_input import StatsInfoCollectInput
from prefect_lib.data_models.stats_info_collect_data import StatsInfoCollectData
from shared.timezone_recovery import timezone_recovery
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import CrawlerLogsModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import CrawlerLogsModel


@task
def stats_info_collect_task(
    mongo: MongoModel, stats_info_collect_input: StatsInfoCollectInput
) -> StatsInfoCollectData:
    """
    クローラーログより取得したスパイダーレポートをクローラーログの集計用クラスへ流し込みデータフレームを生成する。
    生成したデータフレームクラスを返す。
    """
    logger = get_run_logger()  # PrefectLogAdapter

    # クローラーログの集計用クラス（データフレーム）初期化
    stats_info_collect_data = StatsInfoCollectData()

    # クローラーログより対象データを取得する。
    crawler_logs = CrawlerLogsModel(mongo)
    base_date_from, base_date_to = stats_info_collect_input.base_date_get(START_TIME)
    conditions: list = []
    conditions.append(
        {CrawlerLogsModel.RECORD_TYPE: CrawlerLogsModel.RECORD_TYPE__SPIDER_REPORTS}
    )
    conditions.append({CrawlerLogsModel.START_TIME: {"$gte": base_date_from}})
    conditions.append({CrawlerLogsModel.START_TIME: {"$lt": base_date_to}})

    filter: dict = {"$and": conditions}

    count = crawler_logs.count(filter=filter)
    crawler_logs_records: Cursor = crawler_logs.find(
        filter=filter,
        # idやcrawl_urls_listは不要
        projection={"_id": 0, crawler_logs.CRAWL_URLS_LIST: 0},  # 不要項目を除外して取得
    )
    logger.info(f"=== クローラーログ対象件数({count})")

    # クローラーログより取得したスパイダーレポートをクローラーログの集計用クラスへ流し込みデータフレームを生成する。
    for crawler_logs_record in crawler_logs_records:
        stats_info_collect_data.spider_stats_store(
            timezone_recovery(crawler_logs_record[CrawlerLogsModel.START_TIME]),
            crawler_logs_record[CrawlerLogsModel.SPIDER_NAME],
            crawler_logs_record[CrawlerLogsModel.STATS],
        )

    return stats_info_collect_data
