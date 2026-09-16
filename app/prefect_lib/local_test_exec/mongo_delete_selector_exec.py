from datetime import date

# from prefect.testing.utilities import prefect_test_harness
from BrownieAtelierMongo.collection_models.news_clip_master_model import NewsClipMasterModel
from prefect_lib.flows.mongo_delete_selector_flow import mongo_delete_selector_flow


def test_exec():
    # with prefect_test_harness():

    mongo_delete_selector_flow(
        collections_name=[
            # CrawlerResponseModel.COLLECTION_NAME,
            # # 通常運用では不要なバックアップとなるがテスト用に実装している。
            # ScrapedFromResponseModel.COLLECTION_NAME,
            NewsClipMasterModel.COLLECTION_NAME,
            # CrawlerLogsModel.COLLECTION_NAME,
            # AsynchronousReportModel.COLLECTION_NAME,
            # ControllerModel.COLLECTION_NAME,
            # StatsInfoCollectModel.COLLECTION_NAME,
        ],
        period_date_from=date(2025, 6, 14),  # 月次エクスポートを行うデータの基準年月日
        period_date_to=date(2025, 6, 14),  # 月次エクスポートを行うデータの基準年月日
        # crawler_response__registered=False,
    )


if __name__ == "__main__":
    test_exec()
