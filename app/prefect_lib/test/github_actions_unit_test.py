# カレントディレクトリをpythonpathに追加
import os
import sys
current_directory = os.environ.get('PWD')
if current_directory:
    sys.path.append(current_directory)


# 各ニュースサイト別に、スクレイピングの情報を登録する。
#   scraper_info_uploader_flow.py
from prefect_lib.flows.scraper_info_uploader_flow import \
    scraper_info_by_domain_flow
scraper_info_by_domain_flow(scraper_info_by_domain_files=[],)

# 定期観測用のスパイダーを登録する。
#   regular_observation_controller_update_flow.py
#   ３つを指定（a:産経,b:朝日,c:読売）
from prefect_lib.flows.regular_observation_controller_update_const import \
    RegularObservationControllerUpdateConst
from prefect_lib.flows.regular_observation_controller_update_flow import \
    regular_observation_controller_update_flow
regular_observation_controller_update_flow(
    register_type=RegularObservationControllerUpdateConst.REGISTER_ADD,
    spiders_name=[
        "sankei_com_sitemap",
        "asahi_com_sitemap",
        "yomiuri_co_jp_sitemap",
    ],
)

# 初回定期観測
#   first_observation_flow.py
#   上記のa:産経,b:朝日,c:読売だけが実行されるはず
from prefect_lib.flows.first_observation_flow import first_observation_flow
first_observation_flow()

# 手動クローリング
#   manual_crawling_flow.py
#   その他を全て指定 (d:エポック、e:ロイター、f:共同、g:毎日、h:日経)
from prefect_lib.flows.manual_crawling_flow import manual_crawling_flow
manual_crawling_flow(
    spider_names=[
        "epochtimes_jp_crawl",
        "jp_reuters_com_sitemap",
        "kyodo_co_jp_sitemap",
        "mainichi_jp_crawl",
        "nikkei_com_crawl",
    ],
    spider_kwargs=dict(
        debug=True,
        page_span_from=2,
        page_span_to=2,
        lastmod_term_minutes_from=60,
        lastmod_term_minutes_to=0,
    ),
    following_processing_execution=True  # 後続処理実行(scrapying,news_clip_masterへの登録,solrへの登録)
)

# 各ニュースサイト別に、定期観測クローリングのON/OFF、スクレイピングのON/OFF指定を登録する。
#   stop_controller_update_flow.py
#   a:産経:クローリングをOFF、b:朝日:スクレイピングをOFF、c:読売:何もしない。
from prefect_lib.flows.stop_controller_update_flow import (
    StopControllerUpdateConst, stop_controller_update_flow)
stop_controller_update_flow(
    domain="sankei.com",
    command=StopControllerUpdateConst.COMMAND_ADD,
    destination=StopControllerUpdateConst.CRAWLING,
    # destination=StopControllerUpdateConst.SCRAPYING,
)
stop_controller_update_flow(
    domain="asahi.com",
    command=StopControllerUpdateConst.COMMAND_ADD,
    # destination=StopControllerUpdateConst.CRAWLING,
    destination=StopControllerUpdateConst.SCRAPYING,
)

# 定期観測
#   regular_observation_flow.py
#     産経：クローリング・スクレイピングがスキップされる。
#     朝日：クローリングのみ稼働。スクレイピングがスキップされる。
#     読売：通常稼働。
from prefect_lib.flows.regular_observation_flow import regular_observation_flow
regular_observation_flow()
 
# マニュアルスクレイピング
#   manual_scrapying_flow.py
from datetime import datetime, timedelta
from prefect_lib.flows.manual_scrapying_flow import manual_scrapying_flow
from shared.settings import TIMEZONE

manual_scrapying_flow(
    # domain='sankei_com_sitemap',
    target_start_time_from=datetime.now().astimezone(TIMEZONE) - timedelta(minutes=60),
    target_start_time_to=datetime.now().astimezone(TIMEZONE),
    # urls=None,
    following_processing_execution=True,
)
 
# マニュアルニュースクリップマスター保存
#   manual_news_clip_master_save_flow.py
# from datetime import datetime
from prefect_lib.flows.manual_news_clip_master_save_flow import \
    manual_news_clip_master_save_flow
# from shared.settings import TIMEZONE

manual_news_clip_master_save_flow(
    # domain='sankei_com_sitemap',
    target_start_time_from=datetime.now().astimezone(TIMEZONE) - timedelta(minutes=60),
    target_start_time_to=datetime.now().astimezone(TIMEZONE),
)

# スクレイピング使用パターンレポート
#   scraper_pattern_report_flow.py
# from datetime import datetime
from prefect_lib.flows.scraper_pattern_report_flow import \
    scraper_pattern_report_flow
from prefect_lib.data_models.scraper_pattern_report_input import \
    ScraperPatternReportConst
# from shared.settings import TIMEZONE

# 基準日を翌日にずらしてから一週間分の情報を収集
scraper_pattern_report_flow(
    report_term=ScraperPatternReportConst.REPORT_TERM__WEEKLY,   
    base_date=datetime.now().astimezone(TIMEZONE) + timedelta(days=1),
)

# Scrapy統計情報集計
#   stats_info_collect_flow.py
# from datetime import date
from prefect_lib.flows.stats_info_collect_flow import stats_info_collect_flow
# from shared.settings import TIMEZONE

#基準日(0:00:00)〜翌日(0:00:00)までの期間が対象となる。
# stats_info_collect_flow(base_date=date(2023, 7, 1))
# 日を跨いだ場合に備えて2日分を収集
_ = datetime.now().astimezone(TIMEZONE)
stats_info_collect_flow(base_date=_.date())
_ = _ - timedelta(days=1)
stats_info_collect_flow(base_date=_.date())

# Scrapy統計情報レポート
#   stats_analysis_report_flow.py
# from datetime import date
from prefect_lib.data_models.stats_analysis_report_input import \
    StatsAnalysisReportConst
from prefect_lib.flows.stats_analysis_report_flow import \
    stats_analysis_report_flow

# 基準日(0:00:00)〜翌日(0:00:00)までの期間が対象となる。
# 一週間分を1日ごとに集計した結果を取得する。
_ = datetime.now().astimezone(TIMEZONE)
stats_analysis_report_flow(
    report_term=StatsAnalysisReportConst.REPORT_TERM__WEEKLY,
    totalling_term=StatsAnalysisReportConst.TOTALLING_TERM__DAILY,
    base_date=_.date(),
)

# 同期チェック
#   crawl_sync_check_flow.py
# from datetime import datetime
from prefect_lib.flows.crawl_sync_check_flow import crawl_sync_check_flow
from shared.settings import TIMEZONE

# 絞り込み用の引数がなければ全量チェック
crawl_sync_check_flow(
    # domain='sankei.com',
    # start_time_from=datetime(2023, 6, 1, 0, 0, 0, 000000).astimezone(TIMEZONE),
    # start_time_to=datetime(2023, 6, 30, 23, 59, 59, 999999).astimezone(TIMEZONE),
)

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
    ],
    prefix="",  # export先のフォルダyyyy-mmの先頭に拡張した名前を付与する。
    suffix="",
    period_month_from=1,  # 月次エクスポートを行うデータの基準年月
    period_month_to=0,  # 月次エクスポートを行うデータの基準年月
    crawler_response__registered=True,  # crawler_responseの場合、登録済みになったレコードのみエクスポートする場合True、登録済み以外のレコードも含めてエクスポートする場合False
)

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
    ],
    period_month_from=1,  # 月次エクスポートを行うデータの基準年月
    period_month_to=0,  # 月次エクスポートを行うデータの基準年月
    # crawler_response__registered=False,
)

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
from prefect_lib.flows.mongo_import_selector_flow import \
    mongo_import_selector_flow

_ = datetime.now().astimezone(TIMEZONE) - relativedelta(months=1)
yyyy_mm_from = _.strftime("%Y-%m")
_ = datetime.now().astimezone(TIMEZONE) - relativedelta(months=1)
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
    ],
)

