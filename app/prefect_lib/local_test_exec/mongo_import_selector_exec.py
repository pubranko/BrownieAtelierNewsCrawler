# from prefect.testing.utilities import prefect_test_harness
from BrownieAtelierMongo.collection_models.news_clip_master_model import NewsClipMasterModel
from prefect_lib.flows.mongo_import_selector_flow import mongo_import_selector_flow


def test_exec():
    # with prefect_test_harness():

    mongo_import_selector_flow(
        folder_name="2025-06-14_2025-06-14",
        collections_name=[
            # CrawlerResponseModel.COLLECTION_NAME,
            # ScrapedFromResponseModel.COLLECTION_NAME, # 通常運用では不要なバックアップとなるがテスト用に実装している。
            NewsClipMasterModel.COLLECTION_NAME,
            # CrawlerLogsModel.COLLECTION_NAME,
            # AsynchronousReportModel.COLLECTION_NAME,
            # ControllerModel.COLLECTION_NAME,
            # StatsInfoCollectModel.COLLECTION_NAME,
        ],
    )


if __name__ == "__main__":
    test_exec()
