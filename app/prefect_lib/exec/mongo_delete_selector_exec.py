from BrownieAtelierMongo.collection_models.asynchronous_report_model import \
    AsynchronousReportModel
from BrownieAtelierMongo.collection_models.controller_model import \
    ControllerModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import \
    CrawlerLogsModel
from BrownieAtelierMongo.collection_models.crawler_response_model import \
    CrawlerResponseModel
from BrownieAtelierMongo.collection_models.news_clip_master_model import \
    NewsClipMasterModel
from BrownieAtelierMongo.collection_models.scraped_from_response_model import \
    ScrapedFromResponseModel
from prefect_lib.flows.mongo_delete_selector_flow import \
    mongo_delete_selector_flow

mongo_delete_selector_flow(
    collections_name=[
        CrawlerResponseModel.COLLECTION_NAME,
        ScrapedFromResponseModel.COLLECTION_NAME,  # 通常運用では不要なバックアップとなるがテスト用に実装している。
        NewsClipMasterModel.COLLECTION_NAME,
        CrawlerLogsModel.COLLECTION_NAME,
        AsynchronousReportModel.COLLECTION_NAME,
        ControllerModel.COLLECTION_NAME,
    ],
    period_month_from=3,  # 月次エクスポートを行うデータの基準年月
    period_month_to=3,  # 月次エクスポートを行うデータの基準年月
    # crawler_response__registered=False,
)
