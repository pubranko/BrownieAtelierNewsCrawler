from prefect_lib.flows.mongo_export_selector_flow import mongo_export_selector_flow
from BrownieAtelierMongo.collection_models.crawler_response_model import CrawlerResponseModel
from BrownieAtelierMongo.collection_models.scraped_from_response_model import ScrapedFromResponseModel
from BrownieAtelierMongo.collection_models.news_clip_master_model import NewsClipMasterModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import CrawlerLogsModel
from BrownieAtelierMongo.collection_models.asynchronous_report_model import AsynchronousReportModel
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel

mongo_export_selector_flow(
    collections_name=[
        CrawlerResponseModel.COLLECTION_NAME,
        ScrapedFromResponseModel.COLLECTION_NAME, # 通常運用では不要なバックアップとなるがテスト用に実装している。
        NewsClipMasterModel.COLLECTION_NAME,
        CrawlerLogsModel.COLLECTION_NAME,
        AsynchronousReportModel.COLLECTION_NAME,
        ControllerModel.COLLECTION_NAME,
    ],
    prefix='test7',   # export先のフォルダyyyy-mmの先頭に拡張した名前を付与する。
    suffix='',
    period_month_from=1,  # 月次エクスポートを行うデータの基準年月
    period_month_to=0,  # 月次エクスポートを行うデータの基準年月
    crawler_response__registered=True,   # crawler_responseの場合、登録済みになったレコードのみエクスポートする場合True、登録済み以外のレコードも含めてエクスポートする場合False
)

