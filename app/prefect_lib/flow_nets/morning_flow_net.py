from datetime import datetime
from prefect import flow, get_run_logger

from shared.settings import TIMEZONE
from prefect_lib.flows.crawl_sync_check_flow import crawl_sync_check_flow
from prefect_lib.flows.mongo_delete_selector_flow import \
    mongo_delete_selector_flow
from prefect_lib.flows.stats_info_collect_flow import stats_info_collect_flow
from prefect_lib.flows.stats_analysis_report_flow import stats_analysis_report_flow
from prefect_lib.flows.scraper_pattern_report_flow import scraper_pattern_report_flow
from prefect_lib.flows.mongo_export_selector_flow import \
    mongo_export_selector_flow
from prefect_lib.data_models.scraper_pattern_report_input import ScraperPatternReportConst
from prefect_lib.data_models.stats_analysis_report_input import \
    StatsAnalysisReportConst
from prefect_lib.tasks.container_end_task import container_end_task

from BrownieAtelierMongo.collection_models.scraped_from_response_model import \
    ScrapedFromResponseModel
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
from BrownieAtelierMongo.collection_models.stats_info_collect_model import \
    StatsInfoCollectModel


@flow(name="Morning Flow Net")
def morning_flow_net():
    # 日次：同期チェックを実施（全量）
    crawl_sync_check_flow()

    # 日次：不要データ削除
    mongo_delete_selector_flow(
        collections_name=[ScrapedFromResponseModel.COLLECTION_NAME,],
        period_month_from=1,  # 月次エクスポートを行うデータの基準年月
        period_month_to=0,  # 月次エクスポートを行うデータの基準年月
    )

    # Scrapyの統計情報を収集する。 引数なし->前日(0:00:00) ～ 当日(0:00:00)を対象とする。
    stats_info_collect_flow()

    # 週次・月次：フロー用に曜日を確認
    now = datetime.now().astimezone(TIMEZONE)
    today = now.date()
    weekday = today.weekday()   # 0:月曜日～6:日曜日
    
    # 週次：日曜日ならば以下のフローを実行
    if weekday == 6:
        # 先週の日曜日～前日土曜日分を対象に1日毎のレポートを作成する。
        stats_analysis_report_flow(
            report_term=StatsAnalysisReportConst.REPORT_TERM__MONTHLY,
            totalling_term=StatsAnalysisReportConst.TOTALLING_TERM__DAILY,
            base_date=today,  # 左記基準日の１日前、１週間前、１ヶ月前、１年前のデータが対象となる。
        )

        # 前日から１か月前までのレポートを作成する。
        scraper_pattern_report_flow(
            report_term=ScraperPatternReportConst.REPORT_TERM__MONTHLY,
            base_date=now,  # 左記基準日の前日分のデータが対象となる。
        )

    # 月次：月初ならば以下のフローを実行
    if today.day == 1:
        mongo_export_selector_flow(
            collections_name=[
                ScrapedFromResponseModel.COLLECTION_NAME,  # 通常運用では不要なバックアップとなるがテスト用に実装している。
                CrawlerResponseModel.COLLECTION_NAME,
                NewsClipMasterModel.COLLECTION_NAME,
                CrawlerLogsModel.COLLECTION_NAME,
                AsynchronousReportModel.COLLECTION_NAME,
                ControllerModel.COLLECTION_NAME,
                StatsInfoCollectModel.COLLECTION_NAME,
            ],
            # 次の形式でbackup_filesフォルダにデータを保存んする。 例)2024-03_2024-06
            prefix="",
            suffix="",
            period_month_from=3,  # 月次エクスポートを行うデータの基準年月from
            period_month_to=0,  # 月次エクスポートを行うデータの基準年月to
            crawler_response__registered=True,  # crawler_responseの場合、登録済みになったレコードのみエクスポートする場合True、登録済み以外のレコードも含めてエクスポートする場合False
        )

        # 保存期間を経過した不要データ削除。(当月-3)か月前のデータを削除する。
        mongo_delete_selector_flow(
            collections_name=[
                CrawlerResponseModel.COLLECTION_NAME,
                NewsClipMasterModel.COLLECTION_NAME,
                CrawlerLogsModel.COLLECTION_NAME,
                AsynchronousReportModel.COLLECTION_NAME,
                StatsInfoCollectModel.COLLECTION_NAME,
            ],
            period_month_from=3,  # 月次エクスポートを行うデータの基準年月from  0->当月、1->前月
            period_month_to=3,  # 月次エクスポートを行うデータの基準年月to  0->当月、1->前月
        )

    # # 定期観測終了後コンテナーを停止させる。
    container_end_task()