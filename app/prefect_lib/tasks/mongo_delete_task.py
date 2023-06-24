import os
import copy
import pickle

from typing import Union, Any
from datetime import datetime, date, time
from dateutil.relativedelta import relativedelta
from prefect import task, get_run_logger
from shared.settings import TIMEZONE, DATA_DIR__BACKUP_BASE_DIR
from prefect_lib.flows import START_TIME

from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.crawler_response_model import CrawlerResponseModel
from BrownieAtelierMongo.collection_models.scraped_from_response_model import ScrapedFromResponseModel
from BrownieAtelierMongo.collection_models.news_clip_master_model import NewsClipMasterModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import CrawlerLogsModel
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel
from BrownieAtelierMongo.collection_models.asynchronous_report_model import AsynchronousReportModel


@task
def mongo_delete_task(
    mongo: MongoModel,
    period_from: datetime,  # 月次エクスポートを行うデータの基準年月
    period_to: datetime,  # 月次エクスポートを行うデータの基準年月
    collections_name: list[Union[
        CrawlerResponseModel, ScrapedFromResponseModel, NewsClipMasterModel,
        CrawlerLogsModel, AsynchronousReportModel,
        # ControllerModel,
        ]],
):
    '''
    '''
    logger = get_run_logger()   # PrefectLogAdapter
    logger.info(f'=== 引数 : period_from={period_from} period_to={period_to} collections_name={collections_name}')


    for collection_name in collections_name:
        collection = None
        conditions: list = []

        if collection_name == CrawlerResponseModel.COLLECTION_NAME:
            collection = CrawlerResponseModel(mongo)
            conditions.append(
                {CrawlerResponseModel.CRAWLING_START_TIME: {'$gte': period_from}})
            conditions.append(
                {CrawlerResponseModel.CRAWLING_START_TIME: {'$lte': period_to}})

        elif collection_name == ScrapedFromResponseModel.COLLECTION_NAME:
            collection = ScrapedFromResponseModel(mongo)
            conditions.append(
                {ScrapedFromResponseModel.SCRAPYING_START_TIME: {'$gte': period_from}})
            conditions.append(
                {ScrapedFromResponseModel.SCRAPYING_START_TIME: {'$lte': period_to}})

        elif collection_name == NewsClipMasterModel.COLLECTION_NAME:
            collection = NewsClipMasterModel(mongo)
            conditions.append(
                {NewsClipMasterModel.SCRAPED_SAVE_START_TIME: {'$gte': period_from}})
            conditions.append(
                {NewsClipMasterModel.SCRAPED_SAVE_START_TIME: {'$lte': period_to}})

        elif collection_name == CrawlerLogsModel.COLLECTION_NAME:
            collection = CrawlerLogsModel(mongo)
            conditions.append(
                {CrawlerLogsModel.START_TIME: {'$gte': period_from}})
            conditions.append({CrawlerLogsModel.START_TIME: {'$lte': period_to}})

        elif collection_name == AsynchronousReportModel.COLLECTION_NAME:
            collection = AsynchronousReportModel(mongo)
            conditions.append(
                {AsynchronousReportModel.START_TIME: {'$gte': period_from}})
            conditions.append({AsynchronousReportModel.START_TIME: {'$lte': period_to}})

        # elif collection_name == ControllerModel.COLLECTION_NAME:
        #     collection = ControllerModel(mongo)

        if collection:
            filter: Any = {'$and': conditions} if conditions else None

            before_count = collection.count()
            delete_count: int = collection.delete_many(filter=filter)
            after_count = collection.count()

            logger.info(
                f'=== ({collection_name}) 削除前の総件数: {str(before_count)} -> 削除件数: {str(delete_count)} -> 削除後の総件数: {str(after_count)}')
