import os
import bson
import gzip
from typing import Any
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel
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
from prefect.cache_policies import NO_CACHE
from shared.settings import DATA__BACKUP_BASE_DIR


@task(cache_policy=NO_CACHE)
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

    folder_path: str = os.path.join(DATA__BACKUP_BASE_DIR, folder_name)
    if os.path.exists(folder_path):
        logger.info(f"=== フォルダー存在確認OK ({folder_path})")

        for collection_name in collections_name:
            file_path: str = os.path.join(
                DATA__BACKUP_BASE_DIR, folder_name, f"{collection_name}.gz"
            )
            if os.path.exists(file_path):
                import_files_info[
                    collection_name
                ] = file_path  # ファイル名(コレクション名)をkey、インポートファイルフルパスをvalueとする。
            else:
                logger.error(f"=== 指定されたフォルダーに対象のコレクションファイルは存在しませんでした。 ({collection_name})")
    else:
        logger.error(f"=== 指定されたフォルダーは存在しませんでした。({folder_path})")

    logger.info(f"=== インポート対象ファイル : {str(import_files_info.values())}")

    # ファイルからオブジェクトを復元しインポートを実施する。
    for collection_name, file_path in import_files_info.items():

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

        elif collection_name == ControllerModel.COLLECTION_NAME:
            collection = ControllerModel(mongo)
            # コントローラー内のドキュメント全削除（コントローラーはデータを追加するにではなく差し替える）
            collection.delete_many(filter={})

        before_count: int = collection.count()

        # ファイルからオブジェクトへ復元
        collection_records: list = []

        with gzip.open(file_path, 'rb') as bson_file:
            bson_file:Any   # データの型=<class 'gzip.GzipFile'>となる。 後続のbson.decode_file_iterでエラーとなるため型をAnyとしている。
            
            documents: list = []
            write_count: int = 0
            for document in bson.decode_file_iter(bson_file):
                
                del document["_id"]  # idは継承しない。インポート時に新しいIDを割り当てる。
                documents.append(document)
                if (len(documents) >= 1000): # 1000件ごとにmongoDBへ書き込み
                    write_count += 1000
                    collection.insert(documents)
                    logger.info(f"=== コレクション({collection_name}) : {write_count}件書き込み完了")
                    del documents
                    documents = []

            if documents:
                collection.insert(documents)
                del documents

        after_count: int = collection.count()
        logger.info(
            f"=== コレクション({collection_name})  追加前の総件数: {str(before_count)} -> 追加件数: {str(len(collection_records))} -> 追加後の総件数: {str(after_count)}"
        )
                

"""
保存するフォルダ内の形式

prefix_yyyy-mm_yyyy-mm_suffix
    timestamp
    collection
    collection
    ,,,,
"""
