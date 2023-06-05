import os
import copy
import pickle

from typing import Union, Any
from datetime import datetime, date, time
from dateutil.relativedelta import relativedelta
from prefect import task, get_run_logger
from shared.settings import TIMEZONE, DATA_DIR__BACKUP_BASE_DIR
from prefect_lib.flows import START_TIME
from pymongo import ASCENDING
from pymongo.cursor import Cursor

from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.crawler_response_model import CrawlerResponseModel
from BrownieAtelierMongo.collection_models.scraped_from_response_model import ScrapedFromResponseModel
from BrownieAtelierMongo.collection_models.news_clip_master_model import NewsClipMasterModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import CrawlerLogsModel
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel
from BrownieAtelierMongo.collection_models.asynchronous_report_model import AsynchronousReportModel


@task
def mongo_export_task(
    mongo: MongoModel,
    dir_path: str,   # export先のフォルダ名先頭に拡張した名前を付与する。
    period_from: datetime,  # 月次エクスポートを行うデータの基準年月
    period_to: datetime,  # 月次エクスポートを行うデータの基準年月
    collections_name: list[Union[CrawlerResponseModel, ScrapedFromResponseModel, NewsClipMasterModel,
                            CrawlerLogsModel, AsynchronousReportModel, ControllerModel]],
    crawler_response__registered: bool,
):
    '''
    mongoDBのコレクションよりimport/exportを行うための前処理を実行する。
    ①import/export先のフォルダ名の生成  ex) prefix_yyyy-mm_yyyy-mm_suffix, prefix_yyyy-mm_yyyy-mm, yyyy-mm_yyyy-mm_suffix, yyyy-mm_yyyy-mm
    ②exportを行う範囲の日時を生成
    '''
    logger = get_run_logger()   # PrefectLogAdapter
    logger.info(f'=== mongo_export_task 引数 : {dir_path} {period_from} {period_to}')

    # バックアップフォルダ直下に基準年月ごとのフォルダを作る。
    # 同一フォルダへのエクスポートは禁止。
    if os.path.exists(dir_path):
        logger.error(
            f'=== mongo_export_task : backup_dirパラメータエラー : {dir_path} は既に存在します。')
        raise ValueError(dir_path)
    else:
        os.mkdir(dir_path)

    # タイムスタンプをファイル名にした空ファイルを作成。
    with open(f'{os.path.join(dir_path, START_TIME.isoformat())}', 'w') as f:
        pass

    for collection_name in collections_name:
        # ファイル名 ＝ コレクション名
        file_path: str = os.path.join(dir_path, f"{collection_name}")

        sort_parameter: list = []
        collection = None
        conditions: list = []

        if collection_name == CrawlerResponseModel.COLLECTION_NAME:
            collection = CrawlerResponseModel(mongo)
            sort_parameter = [(CrawlerResponseModel.RESPONSE_TIME, ASCENDING)]
            conditions.append(
                {CrawlerResponseModel.CRAWLING_START_TIME: {'$gte': period_from}})
            conditions.append(
                {CrawlerResponseModel.CRAWLING_START_TIME: {'$lte': period_to}})
            if crawler_response__registered:
                conditions.append(
                    {CrawlerResponseModel.NEWS_CLIP_MASTER_REGISTER: CrawlerResponseModel.NEWS_CLIP_MASTER_REGISTER__COMPLETE})  # crawler_responseの場合、登録完了のレコードのみ保存する。

        elif collection_name == ScrapedFromResponseModel.COLLECTION_NAME:
            collection = ScrapedFromResponseModel(mongo)
            sort_parameter = []
            conditions.append(
                {ScrapedFromResponseModel.SCRAPYING_START_TIME: {'$gte': period_from}})
            conditions.append(
                {ScrapedFromResponseModel.SCRAPYING_START_TIME: {'$lte': period_to}})

        elif collection_name == NewsClipMasterModel.COLLECTION_NAME:
            collection = NewsClipMasterModel(mongo)
            sort_parameter = [(NewsClipMasterModel.RESPONSE_TIME, ASCENDING)]
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

        elif collection_name == ControllerModel.COLLECTION_NAME:
            collection = ControllerModel(mongo)

        if collection:
            filter: Any = {'$and': conditions} if conditions else None
            # export_exec(collection_name, filter,
            #             sort_parameter, collection, file_path)
            # エクスポート対象件数を確認
            record_count = collection.count(filter)
            logger.info(
                f'=== {collection_name} バックアップ対象件数 : {str(record_count)}')

            # 100件単位で処理を実施
            limit: int = 100
            skip_list = list(range(0, record_count, limit))

            # ファイルにリストオブジェクトを追記していく
            with open(file_path, 'ab') as file:
                pass  # 空ファイル作成
            for skip in skip_list:
                records: Cursor = collection.find(
                    filter=filter,
                    sort=sort_parameter,
                ).skip(skip).limit(limit)
                record_list: list = [record for record in records]
                with open(file_path, 'ab') as file:
                    file.write(pickle.dumps(record_list))




        # 誤更新防止のため、ファイルの権限を参照に限定
        os.chmod(file_path, 0o444)



'''
保存するフォルダ内の形式

prefix_yyyy-mm_yyyy-mm_suffix
    timestamp
    collection
    collection
    ,,,,
'''
