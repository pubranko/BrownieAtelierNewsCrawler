"""
各フローを登録する。
以下の操作を事前に行っておくこと。
・prefect cloud login --key xxx
・export PREFECT_HOME= xxx
・localの場合 → prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
・localコンテナーの場合 → prefect config set PREFECT_API_URL="http://0.0.0.0:4200/api"
・Cloudの場合 → prefect config set PREFECT_API_URL="https://api.prefect.cloud/api/accounts/[ACCOUNT-ID]/workspaces/[WORKSPACE-ID]"
"""
import os
import sys
current_dir = os.getcwd()
sys.path.append(current_dir)

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
from decouple import AutoConfig, config
from prefect.deployments.deployments import Deployment
from prefect.server.schemas.schedules import (CronSchedule, IntervalSchedule,
                                              RRuleSchedule)
from prefect.settings import PREFECT_API_URL, PREFECT_HOME
from prefect_lib.data_models.scraper_pattern_report_input import \
    ScraperPatternReportConst
# 必要な引数定義
from prefect_lib.data_models.stats_analysis_report_input import \
    StatsAnalysisReportConst
# check系
from prefect_lib.flows.crawl_sync_check_flow import crawl_sync_check_flow
from prefect_lib.flows.first_observation_flow import first_observation_flow
# crawl-scrape系
from prefect_lib.flows.manual_crawling_flow import manual_crawling_flow
from prefect_lib.flows.manual_news_clip_master_save_flow import \
    manual_news_clip_master_save_flow
from prefect_lib.flows.manual_scrapying_flow import manual_scrapying_flow
# mongodb系
from prefect_lib.flows.mongo_delete_selector_flow import \
    mongo_delete_selector_flow
from prefect_lib.flows.mongo_export_selector_flow import \
    mongo_export_selector_flow
from prefect_lib.flows.mongo_import_selector_flow import \
    mongo_import_selector_flow
from prefect_lib.flows.regular_observation_controller_update_flow import \
    regular_observation_controller_update_flow
from prefect_lib.flows.regular_observation_flow import regular_observation_flow
# register系
from prefect_lib.flows.scraper_info_uploader_flow import \
    scraper_info_by_domain_flow
from prefect_lib.flows.scraper_pattern_report_flow import \
    scraper_pattern_report_flow
from prefect_lib.flows.stats_analysis_report_flow import \
    stats_analysis_report_flow
# report系
from prefect_lib.flows.stats_info_collect_flow import stats_info_collect_flow
from prefect_lib.flows.stop_controller_update_flow import \
    stop_controller_update_flow

prefect_home = PREFECT_HOME.value()
print(f"=== {prefect_home =}")
prefect_api_url = PREFECT_API_URL.value()
print(f"=== {prefect_api_url = }")

if not (prefect_api_url):
    raise ValueError(
        "prefect_api_urlが参照できませんでしたので、処理を停止します。環境変数にPREFECT_HOMEが存在しない、またはPREFECT_API_URLが設定されていない可能性が高いです。"
    )
# elif prefect_api_url.startswith("http://127.0.0.1") or prefect_api_url.startswith(
#     "http://localhost"
# ):
#     # ローカル開発環境用の場合
#     path = os.getcwd()
# elif prefect_api_url.startswith("http://0.0.0.0"):
#     # ローカルコンテナー開発環境用の場合
#     path = f'/home/{str(config("CONTAINER_USER"))}/BrownieAtelier/app'
# else:
#     # コンテナー内で実行する際のカレントディレクトリ
#     path = f'/home/{str(config("CONTAINER_USER"))}/BrownieAtelier/app'
path = current_dir
print(f"=== {path = }")

work_pool_name = str(config("PREFECT__WORK_POOL", default="default-agent-pool"))
print(f"=== {work_pool_name = }")

###################
# crawl-scrape
###################
# name -> 可動タイミングがわかるように manual, daily, monthly, weekly, yearly
# tags -> 自動・手動、系統、可動タイミングがわかるように [manual, auto], [register, crawl-scrape, check, report, mongodb], [daily, monthly, weekly, yearly]
deployment__manual_crawling_flow = Deployment.build_from_flow(
    flow=manual_crawling_flow,
    name="manual",
    tags=["manual", "crawl-scrape"],
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__manual_crawling_flow 完了")
deployment__manual_scrapying_flow = Deployment.build_from_flow(
    flow=manual_scrapying_flow,
    name="manual",
    tags=["manual", "crawl-scrape"],
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__manual_scrapying_flow 完了")
deployment__manual_news_clip_master_save_flow = Deployment.build_from_flow(
    flow=manual_news_clip_master_save_flow,
    name="manual",
    tags=["manual", "crawl-scrape"],
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__manual_news_clip_master_save_flow 完了")
deployment__first_observation_flow = Deployment.build_from_flow(
    flow=first_observation_flow,
    name="manual",
    tags=["manual", "crawl-scrape"],
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__first_observation_flow 完了")
deployment__regular_observation_flow = Deployment.build_from_flow(
    flow=regular_observation_flow,
    name="daily",
    tags=["auto", "crawl-scrape", "daily"],
    # 毎日 6時〜24時の間、3時間毎、毎時1分に起動
    schedule=CronSchedule(cron="1 0,6-21/3 * * *", timezone="Asia/Tokyo"),
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__regular_observation_flow 完了")
###################
# register
###################
deployment__scraper_info_by_domain_flow = Deployment.build_from_flow(
    flow=scraper_info_by_domain_flow,
    name="manual",
    tags=["manual", "register"],
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__scraper_info_by_domain_flow 完了")
deployment__regular_observation_controller_update_flow = Deployment.build_from_flow(
    flow=regular_observation_controller_update_flow,
    name="manual",
    tags=["manual", "register"],
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__regular_observation_controller_update_flow 完了")
deployment__stop_controller_update_flow = Deployment.build_from_flow(
    flow=stop_controller_update_flow,
    name="manual",
    tags=["manual", "register"],
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__stop_controller_update_flow 完了")
###################
# check
###################
deployment__crawl_sync_check_flow = Deployment.build_from_flow(
    flow=crawl_sync_check_flow,
    name="daily",
    tags=["auto", "check", "daily"],
    schedule=CronSchedule(cron="50 5 * * *", timezone="Asia/Tokyo"),  # 毎日 5時50分に起動
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__crawl_sync_check_flow 完了")
###################
# mongodb
###################
deployment__mongo_delete_selector_flow = Deployment.build_from_flow(
    flow=mongo_delete_selector_flow,
    name="manual",
    tags=["manual", "mongodb"],
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__mongo_delete_selector_flow 完了")
deployment__mongo_export_selector_flow = Deployment.build_from_flow(
    flow=mongo_export_selector_flow,
    name="manual",
    tags=["manual", "mongodb"],
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__mongo_export_selector_flow 完了")
deployment__mongo_import_selector_flow = Deployment.build_from_flow(
    flow=mongo_import_selector_flow,
    name="manual",
    tags=["manual", "mongodb"],
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__mongo_import_selector_flow 完了")
deployment__mongo_delete_selector_flow_daily = Deployment.build_from_flow(
    flow=mongo_delete_selector_flow,
    name="daily",
    tags=["auto", "mongodb", "daily"],
    parameters=dict(
        collections_name=[ScrapedFromResponseModel.COLLECTION_NAME],
        period_month_from=1200,  # 基本的に全て削除対象
        period_month_to=0,
        crawler_response__registered=True,
    ),  # crawl結果の登録処理が完了したものを削除対象とする。
    schedule=CronSchedule(cron="51 5 * * *", timezone="Asia/Tokyo"),  # 毎日 5時51分に起動
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__mongo_delete_selector_flow_daily 完了")
deployment__mongo_delete_selector_flow_monthly = Deployment.build_from_flow(
    flow=mongo_delete_selector_flow,
    name="monthly",
    tags=["auto", "mongodb", "monthly"],
    parameters=dict(
        collections_name=[
            CrawlerResponseModel.COLLECTION_NAME,
            CrawlerLogsModel.COLLECTION_NAME,
            AsynchronousReportModel.COLLECTION_NAME,
            StatsInfoCollectModel.COLLECTION_NAME,
        ],
        period_month_from=1200,
        period_month_to=3,  # 作業年月より３ヶ月経過したものを削除対象とする。
        crawler_response__registered=True,
    ),  # crawl結果の登録処理が完了したものを削除対象とする。
    schedule=CronSchedule(cron="51 5 1 * *", timezone="Asia/Tokyo"),  # 月初 5時51分に起動
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__mongo_delete_selector_flow_monthly 完了")
deployment__mongo_export_selector_flow = Deployment.build_from_flow(
    flow=mongo_export_selector_flow,
    name="monthly",
    tags=["auto", "mongodb", "monthly"],
    parameters=dict(
        collections_name=[
            CrawlerResponseModel.COLLECTION_NAME,
            # ScrapedFromResponseModel.COLLECTION_NAME, # 通常運用では不要なバックアップとなるがテスト用に実装している。
            NewsClipMasterModel.COLLECTION_NAME,
            CrawlerLogsModel.COLLECTION_NAME,
            AsynchronousReportModel.COLLECTION_NAME,
            ControllerModel.COLLECTION_NAME,
            StatsInfoCollectModel.COLLECTION_NAME,
        ],
        prefix="",
        suffix="",
        period_month_from=1,  # 前月分をバックアップ
        period_month_to=1,
        crawler_response__registered=False,
    ),
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__mongo_export_selector_flow 完了")
###################
# report
###################
deployment__stats_info_collect_flow = Deployment.build_from_flow(
    flow=stats_info_collect_flow,
    name="daily",
    tags=["auto", "report", "daily"],
    # parameters=dict(base_date=None),
    schedule=CronSchedule(cron="52 5 * * *", timezone="Asia/Tokyo"),  # 毎日 5時51分に起動
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__stats_info_collect_flow 完了")
deployment__stats_analysis_report_flow = Deployment.build_from_flow(
    flow=stats_analysis_report_flow,
    name="weekly",
    tags=["auto", "report", "weekly"],
    parameters=dict(
        report_term=StatsAnalysisReportConst.REPORT_TERM__WEEKLY,  # １週間の間、1日単位の集計結果を求める。
        totalling_term=StatsAnalysisReportConst.TOTALLING_TERM__DAILY,
    ),
    # 日曜日 5時55分に起動。上記stats_info_collect_flow後に動かす必要あり
    schedule=CronSchedule(cron="55 5 * * 0", timezone="Asia/Tokyo"),
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__stats_analysis_report_flow 完了")
deployment__scraper_pattern_report_flow = Deployment.build_from_flow(
    flow=scraper_pattern_report_flow,
    name="weekly",
    tags=["auto", "report", "weekly"],
    parameters=dict(
        report_term=ScraperPatternReportConst.REPORT_TERM__WEEKLY,
    ),  # １週間分の集計結果を求める。
    schedule=CronSchedule(cron="53 5 * * 0", timezone="Asia/Tokyo"),  # 日曜日 5時53分に起動
    version="0.1",
    apply=True,
    is_schedule_active=False,
    work_pool_name=work_pool_name,
    path=path,
)
print(f"deployment__scraper_pattern_report_flow 完了")
