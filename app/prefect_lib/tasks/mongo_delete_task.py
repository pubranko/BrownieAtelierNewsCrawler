from typing import Any
from datetime import datetime
from prefect import task, get_run_logger
from pymongo.command_cursor import CommandCursor, RawBatchCommandCursor
import copy

from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.crawler_response_model import (
    CrawlerResponseModel,
)
from BrownieAtelierMongo.collection_models.scraped_from_response_model import (
    ScrapedFromResponseModel,
)
from BrownieAtelierMongo.collection_models.news_clip_master_model import (
    NewsClipMasterModel,
)
from BrownieAtelierMongo.collection_models.crawler_logs_model import CrawlerLogsModel
from BrownieAtelierMongo.collection_models.asynchronous_report_model import (
    AsynchronousReportModel,
)
from BrownieAtelierMongo.collection_models.stats_info_collect_model import (
    StatsInfoCollectModel,
)


@task
def mongo_delete_task(
    mongo: MongoModel,
    period_from: datetime,  # 月次エクスポートを行うデータの基準年月
    period_to: datetime,  # 月次エクスポートを行うデータの基準年月
    collections_name: list[str],
    # crawler_responseの場合、登録済みになったレコードのみ削除する場合True、登録済み以外のレコードも含めて削除する場合False。その他のコレクションの場合は無視される。
    crawler_response__registered: bool,
):
    """ """
    logger = get_run_logger()  # PrefectLogAdapter
    logger.info(
        f"=== 引数 : period_from={period_from} period_to={period_to} collections_name={collections_name}"
    )

    for collection_name in collections_name:
        collection = None
        conditions: list = []
        conditions_complete: list = []
        conditions_skip: list = []

        if collection_name == CrawlerResponseModel.COLLECTION_NAME:
            collection = CrawlerResponseModel(mongo)
            conditions.append(
                {CrawlerResponseModel.CRAWLING_START_TIME: {"$gte": period_from}}
            )
            conditions.append(
                {CrawlerResponseModel.CRAWLING_START_TIME: {"$lte": period_to}}
            )
            if crawler_response__registered:
                conditions.append(
                    {
                        CrawlerResponseModel.NEWS_CLIP_MASTER_REGISTER: {
                            "$exists": True
                        }  # news_clip_masterへの登録処理を実施済みのレコードのみ削除する。
                    }
                )

                # 削除データの内訳件数を確認するための検索用conditionsを２つ用意
                conditions_complete = copy.deepcopy(conditions)
                conditions_complete.append(
                    {
                        CrawlerResponseModel.NEWS_CLIP_MASTER_REGISTER: CrawlerResponseModel.NEWS_CLIP_MASTER_REGISTER__COMPLETE
                    }
                )
                conditions_skip = copy.deepcopy(conditions)
                conditions_skip.append(
                    {
                        CrawlerResponseModel.NEWS_CLIP_MASTER_REGISTER: CrawlerResponseModel.NEWS_CLIP_MASTER_REGISTER__SKIP
                    }
                )

        elif collection_name == ScrapedFromResponseModel.COLLECTION_NAME:
            collection = ScrapedFromResponseModel(mongo)
            conditions.append(
                {ScrapedFromResponseModel.SCRAPYING_START_TIME: {"$gte": period_from}}
            )
            conditions.append(
                {ScrapedFromResponseModel.SCRAPYING_START_TIME: {"$lte": period_to}}
            )

        elif collection_name == NewsClipMasterModel.COLLECTION_NAME:
            collection = NewsClipMasterModel(mongo)
            conditions.append(
                {NewsClipMasterModel.SCRAPED_SAVE_START_TIME: {"$gte": period_from}}
            )
            conditions.append(
                {NewsClipMasterModel.SCRAPED_SAVE_START_TIME: {"$lte": period_to}}
            )

        elif collection_name == CrawlerLogsModel.COLLECTION_NAME:
            collection = CrawlerLogsModel(mongo)
            conditions.append({CrawlerLogsModel.START_TIME: {"$gte": period_from}})
            conditions.append({CrawlerLogsModel.START_TIME: {"$lte": period_to}})

        elif collection_name == AsynchronousReportModel.COLLECTION_NAME:
            collection = AsynchronousReportModel(mongo)
            conditions.append(
                {AsynchronousReportModel.START_TIME: {"$gte": period_from}}
            )
            conditions.append({AsynchronousReportModel.START_TIME: {"$lte": period_to}})

        elif collection_name == StatsInfoCollectModel.COLLECTION_NAME:
            collection = StatsInfoCollectModel(mongo)
            conditions.append({StatsInfoCollectModel.START_TIME: {"$gte": period_from}})
            conditions.append({StatsInfoCollectModel.START_TIME: {"$lte": period_to}})

        # elif collection_name == ControllerModel.COLLECTION_NAME:
        #     collection = ControllerModel(mongo)

        if collection:
            filter: Any = {"$and": conditions} if conditions else None

            # crawler_responseの場合、削除データの内訳をログに出力
            if collection_name == CrawlerResponseModel.COLLECTION_NAME:
                if conditions_complete:
                    delete_count: int = collection.count(filter=filter)

                    filter_complete: Any = (
                        {"$and": conditions_complete} if conditions_complete else None
                    )
                    delete_count_complete: int = collection.count(
                        filter=filter_complete
                    )

                    filter_skip: Any = (
                        {"$and": conditions_skip} if conditions_skip else None
                    )
                    delete_count_skip: int = collection.count(filter=filter_skip)

                    logger.info(
                        f"=== ({collection_name}) 削除予定件数: {str(delete_count)} = 登録完了分: {str(delete_count_complete)} , 登録内容に差異なしのため不要: {str(delete_count_skip)}"
                    )

            before_count = collection.count()
            delete_count: int = collection.delete_many(filter=filter)
            after_count = collection.count()

            logger.info(
                f"=== ({collection_name}) 削除前の総件数: {str(before_count)} -> 削除件数: {str(delete_count)} -> 削除後の総件数: {str(after_count)}"
            )

            # aaa = list(collection.aggregate(aggregate_key='domain'))
            # print(f'==={aaa}')
            """
            [{'_id': 'sankei.com', 'count': 133}, {'_id': 'mainichi.jp', 'count': 38}, {'_id': 'nikkei.com', 'count': 46}, {'_id': 'epochtimes.jp', 'count': 48}, {'_id': 'jp.reuters.com', 'count': 20}, {'_id': 'yomiuri.co.jp', 'count': 27}, {'_id': 'asahi.com', 'count': 1}]
            """
