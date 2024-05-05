import copy
import glob
import os
import pickle
from typing import Union

# from BrownieAtelierMongo.collection_models.controller_model import ControllerModel
from BrownieAtelierMongo.collection_models.asynchronous_report_model import \
    AsynchronousReportModel
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
from prefect import get_run_logger, task
from shared.settings import DATA__BACKUP_BASE_DIR


@task
def mongo_import_task(
    mongo: MongoModel,
    folder_name: str,  # import元のフォルダ名
    collections_name: list[str],
):
    """ """
    logger = get_run_logger()  # PrefectLogAdapter
    logger.info(
        f"=== 引数 : folder_name = {folder_name} collections_name = {collections_name} "
    )

    # インポート元ファイルの一覧を作成
    import_files_info: dict[str, str] = {}
    import_file_name_list: list[str] = []

    folder_path: str = os.path.join(DATA__BACKUP_BASE_DIR, folder_name)
    if os.path.exists(folder_path):
        logger.info(f"=== フォルダー存在確認OK ({folder_path})")

        for collection_name in collections_name:
            file_path: str = os.path.join(
                DATA__BACKUP_BASE_DIR, folder_name, collection_name
            )
            if os.path.exists(file_path):
                import_file_name_list.append(collection_name)
                import_files_info[
                    collection_name
                ] = file_path  # ファイル名(コレクション名)をkey、インポートファイルフルパスをvalueとする。
            else:
                logger.error(f"=== 指定されたフォルダーに対象のコレクションファイルは存在しませんでした。 ({collection_name})")
    else:
        logger.error(f"=== 指定されたフォルダーは存在しませんでした。({folder_path})")

    logger.info(f"=== インポート対象ファイル : {str(import_file_name_list)}")

    # ファイルからオブジェクトを復元しインポートを実施する。
    # その際、空ファイルを除き、"_id"を除去してインポートする。
    for collection_name, file_path in import_files_info.items():
        # ファイルからオブジェクトへ復元
        collection_records: list = []
        if os.path.getsize(file_path):
            with open(file_path, "rb") as file:
                documents: list = pickle.loads(file.read())

                logger.info(
                    f"=== コレクション({collection_name}) : インポート対象レコード件数 : {str(len(documents))}"
                )

                for document in documents:
                    del document["_id"]  # idは継承しない。インポート時に新しいIDを割り当てる。
                    collection_records.append(document)

        # 空ファイル以外はコレクションごとにインポートを実施
        if len(collection_records):
            collection = None
            if collection_name == CrawlerResponseModel.COLLECTION_NAME:
                collection = CrawlerResponseModel(mongo)
            elif collection_name == ScrapedFromResponseModel.COLLECTION_NAME:
                collection = ScrapedFromResponseModel(mongo)
            elif collection_name == NewsClipMasterModel.COLLECTION_NAME:
                collection = NewsClipMasterModel(mongo)
            elif collection_name == CrawlerLogsModel.COLLECTION_NAME:
                collection = CrawlerLogsModel(mongo)
            elif collection_name == AsynchronousReportModel.COLLECTION_NAME:
                collection = AsynchronousReportModel(mongo)
            elif collection_name == StatsInfoCollectModel.COLLECTION_NAME:
                collection = StatsInfoCollectModel(mongo)
            # elif file_name == ControllerModel.COLLECTION_NAME:
            #     collection = ControllerModel(mongo)

            if collection:
                # インポート
                before_count: int = collection.count()
                collection.insert(collection_records)
                after_count: int = collection.count()

                logger.info(
                    f"=== コレクション({collection_name})  追加前の総件数: {str(before_count)} -> 追加件数: {str(len(collection_records))} -> 追加後の総件数: {str(after_count)}"
                )

            # 処理の終わったファイルオブジェクトを削除
            del collection_records


"""
保存するフォルダ内の形式

prefix_yyyy-mm_yyyy-mm_suffix
    timestamp
    collection
    collection
    ,,,,
"""
