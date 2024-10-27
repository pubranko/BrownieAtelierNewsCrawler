from prefect.testing.utilities import prefect_test_harness

def test_exec():
    with prefect_test_harness():


        # カレントディレクトリをpythonpathに追加
        import os
        import sys
        current_directory = os.environ.get('PWD')
        if current_directory:
            sys.path.append(current_directory)

        from datetime import datetime, timedelta
        from shared.settings import TIMEZONE


        # <13>
        # mongoDBエクスポート
        #   mongo_export_selector_flow.py
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
        from BrownieAtelierMongo.collection_models.stats_info_collect_model import \
            StatsInfoCollectModel
        from prefect_lib.flows.mongo_export_selector_flow import \
            mongo_export_selector_flow
            
        from dateutil.relativedelta import relativedelta

        mongo_export_selector_flow(
            collections_name=[
                CrawlerResponseModel.COLLECTION_NAME,
                ScrapedFromResponseModel.COLLECTION_NAME,
                NewsClipMasterModel.COLLECTION_NAME,
                CrawlerLogsModel.COLLECTION_NAME,
                AsynchronousReportModel.COLLECTION_NAME,
                ControllerModel.COLLECTION_NAME,
                StatsInfoCollectModel.COLLECTION_NAME,
            ],
            prefix="",  # export先のフォルダyyyy-mmの先頭に拡張した名前を付与する。
            suffix="",
            period_month_from=1,  # 月次エクスポートを行うデータの基準年月
            period_month_to=0,  # 月次エクスポートを行うデータの基準年月
            crawler_response__registered=True,  # crawler_responseの場合、登録済みになったレコードのみエクスポートする場合True、登録済み以外のレコードも含めてエクスポートする場合False
        )

        # <14>
        # mongoDB削除
        #   mongo_delete_selector_flow.py
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
        from BrownieAtelierMongo.collection_models.stats_info_collect_model import \
            StatsInfoCollectModel
        from prefect_lib.flows.mongo_delete_selector_flow import \
            mongo_delete_selector_flow

        mongo_delete_selector_flow(
            collections_name=[
                CrawlerResponseModel.COLLECTION_NAME,
                ScrapedFromResponseModel.COLLECTION_NAME,
                NewsClipMasterModel.COLLECTION_NAME,
                CrawlerLogsModel.COLLECTION_NAME,
                AsynchronousReportModel.COLLECTION_NAME,
                ControllerModel.COLLECTION_NAME,
                StatsInfoCollectModel.COLLECTION_NAME,
            ],
            period_month_from=1,  # 月次エクスポートを行うデータの基準年月
            period_month_to=0,  # 月次エクスポートを行うデータの基準年月
            # crawler_response__registered=False,
        )

        # <15>
        # mongoDBインポート
        #   mongo_import_selector_flow.py
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
        from BrownieAtelierMongo.collection_models.stats_info_collect_model import \
            StatsInfoCollectModel
        from prefect_lib.flows.mongo_import_selector_flow import \
            mongo_import_selector_flow

        _ = datetime.now().astimezone(TIMEZONE) - relativedelta(months=1)
        yyyy_mm_from = _.strftime("%Y-%m")
        _ = datetime.now().astimezone(TIMEZONE)
        yyyy_mm_to = _.strftime("%Y-%m")

        mongo_import_selector_flow(
            folder_name=f"{yyyy_mm_from}_{yyyy_mm_to}",
            collections_name=[
                CrawlerResponseModel.COLLECTION_NAME,
                ScrapedFromResponseModel.COLLECTION_NAME,
                NewsClipMasterModel.COLLECTION_NAME,
                CrawlerLogsModel.COLLECTION_NAME,
                AsynchronousReportModel.COLLECTION_NAME,
                ControllerModel.COLLECTION_NAME,
                StatsInfoCollectModel.COLLECTION_NAME,
            ],
        )

if __name__ == "__main__":
    test_exec()
