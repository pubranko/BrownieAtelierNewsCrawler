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
# from prefect.deployments.deployments import Deployment
from prefect.flows import flow
from prefect.server.schemas.schedules import (CronSchedule, IntervalSchedule,
                                              RRuleSchedule)
# from prefect.settings import PREFECT_API_URL, PREFECT_HOME
from prefect.settings import get_current_settings
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
# flow_net系
from prefect_lib.flow_nets.morning_flow_net import morning_flow_net


_ = get_current_settings()
prefect_home = _.home
print(f"=== {prefect_home = }")
prefect_api_url = _.ui_api_url
print(f"=== {prefect_api_url = }")

if not (prefect_api_url):
    raise ValueError(
        "PREFECT_API_URLが参照できませんでしたので、処理を停止します。環境変数にPREFECT_HOMEが存在しない、またはPREFECT_API_URLが設定されていない可能性が高いです。"
    )
path = current_dir
print(f"=== {path =}")

work_pool_name = str(config("PREFECT__WORK_POOL"))
print(f"=== {work_pool_name =}")

###################
# crawl-scrape
###################
# name -> デプロイの名前。可動タイミングがわかるように manual, daily, monthly, weekly, yearly
# tags -> 自動・手動、系統、可動タイミングがわかるように [manual, auto], [register, crawl-scrape, check, report, mongodb], [daily, monthly, weekly, yearly]

_ = manual_crawling_flow.to_deployment(name="dummy")
manual_crawling_flow_deployment = _.from_entrypoint(
    name = "manual-crawl-scrape",
    entrypoint= str(_.entrypoint),
    tags = ["manual", "crawl-scrape"],
    work_pool_name = work_pool_name,
)
uuid = manual_crawling_flow_deployment.apply()
print(f"deployment -> manual_crawling_flow 完了  ({uuid =})")

_ = manual_scrapying_flow.to_deployment(name="dummy")
manual_scrapying_flow_deployment = _.from_entrypoint(
    name = "manual-crawl-scrape",
    entrypoint= str(_.entrypoint),
    tags = ["manual", "crawl-scrape"],
    work_pool_name = work_pool_name,
)
uuid = manual_scrapying_flow_deployment.apply()
print(f"deployment -> manual_scrapying_flow 完了")

_ = manual_news_clip_master_save_flow.to_deployment(name="dummy")
manual_news_clip_master_save_flow_deployment = _.from_entrypoint(
    name = "manual-crawl-scrape",
    entrypoint= str(_.entrypoint),
    tags = ["manual", "crawl-scrape"],
    work_pool_name = work_pool_name,
)
uuid = manual_news_clip_master_save_flow_deployment.apply()
print(f"deployment -> manual_news_clip_master_save_flow 完了")


_ = first_observation_flow.to_deployment(name="dummy")
first_observation_flow_deployment = _.from_entrypoint(
    name = "manual-crawl-scrape",
    entrypoint= str(_.entrypoint),
    tags = ["manual", "crawl-scrape"],
    work_pool_name = work_pool_name,
)
uuid = first_observation_flow_deployment.apply()
print(f"deployment -> first_observation_flow 完了")

_ = regular_observation_flow.to_deployment(name="dummy")
regular_observation_flow_deployment = _.from_entrypoint(
    name = "auto-crawl-scrape",
    entrypoint= str(_.entrypoint),
    tags = ["auto", "daily", "crawl-scrape"],
    work_pool_name = work_pool_name,
)
uuid = regular_observation_flow_deployment.apply()
print(f"deployment -> regular_observation_flow 完了")

###################
# register
###################
_ = scraper_info_by_domain_flow.to_deployment(name="dummy")
scraper_info_by_domain_flow_deployment = _.from_entrypoint(
    name = "register",
    entrypoint= str(_.entrypoint),
    tags = ["manual", "register"],
    work_pool_name = work_pool_name,
)
uuid = scraper_info_by_domain_flow_deployment.apply()
print(f"deployment -> scraper_info_by_domain_flow 完了")

_ = regular_observation_controller_update_flow.to_deployment(name="dummy")
regular_observation_controller_update_flow_deployment = _.from_entrypoint(
    name = "register",
    entrypoint= str(_.entrypoint),
    tags = ["manual", "register"],
    work_pool_name = work_pool_name,
)
uuid = regular_observation_controller_update_flow_deployment.apply()
print(f"deployment -> regular_observation_controller_update_flow 完了")

_ = stop_controller_update_flow.to_deployment(name="dummy")
stop_controller_update_flow_deployment = _.from_entrypoint(
    name = "register",
    entrypoint= str(_.entrypoint),
    tags = ["manual", "register"],
    work_pool_name = work_pool_name,
)
uuid = stop_controller_update_flow_deployment.apply()
print(f"deployment -> stop_controller_update_flow 完了")

###################
# check
###################
_ = crawl_sync_check_flow.to_deployment(name="dummy")
crawl_sync_check_flow_deployment = _.from_entrypoint(
    name = "check",
    entrypoint= str(_.entrypoint),
    tags = ["manual", "check", "report"],
    work_pool_name = work_pool_name,
)
uuid = crawl_sync_check_flow_deployment.apply()
print(f"deployment -> crawl_sync_check_flow 完了")

###################
# mongodb
###################
_ = mongo_delete_selector_flow.to_deployment(name="dummy")
mongo_delete_selector_flow_deployment = _.from_entrypoint(
    name = "mongodb",
    entrypoint= str(_.entrypoint),
    tags = ["manual", "mongodb"],
    work_pool_name = work_pool_name,
)
uuid = mongo_delete_selector_flow_deployment.apply()
print(f"deployment -> mongo_delete_selector_flow 完了")

_ = mongo_export_selector_flow.to_deployment(name="dummy")
mongo_export_selector_flow_deployment = _.from_entrypoint(
    name = "mongodb",
    entrypoint= str(_.entrypoint),
    tags = ["manual", "mongodb"],
    work_pool_name = work_pool_name,
)
uuid = mongo_export_selector_flow_deployment.apply()
print(f"deployment -> mongo_export_selector_flow 完了")

_ = mongo_import_selector_flow.to_deployment(name="dummy")
mongo_import_selector_flow_deployment = _.from_entrypoint(
    name = "mongodb",
    entrypoint= str(_.entrypoint),
    tags = ["manual", "mongodb"],
    work_pool_name = work_pool_name,
)
uuid = mongo_import_selector_flow_deployment.apply()
print(f"deployment -> mongo_import_selector_flow 完了")

###################
# report
###################
_ = stats_info_collect_flow.to_deployment(name="dummy")
stats_info_collect_flow_deployment = _.from_entrypoint(
    name = "report",
    entrypoint= str(_.entrypoint),
    tags = ["manual", "report"],
    work_pool_name = work_pool_name,
)
uuid = stats_info_collect_flow_deployment.apply()
print(f"deployment -> stats_info_collect_flow 完了")

_ = stats_analysis_report_flow.to_deployment(name="dummy")
stats_analysis_report_flow_deployment = _.from_entrypoint(
    name = "report",
    entrypoint= str(_.entrypoint),
    tags = ["manual", "report"],
    # parameters=dict(
    #     report_term=StatsAnalysisReportConst.REPORT_TERM__WEEKLY,  # １週間の間、1日単位の集計結果を求める。
    #     totalling_term=StatsAnalysisReportConst.TOTALLING_TERM__DAILY,
    # ),
   work_pool_name = work_pool_name,
)
uuid = stats_analysis_report_flow_deployment.apply()
print(f"deployment -> stats_analysis_report_flow 完了")

_ = scraper_pattern_report_flow.to_deployment(name="dummy")
scraper_pattern_report_flow_deployment = _.from_entrypoint(
    name = "report",
    entrypoint= str(_.entrypoint),
    tags = ["manual", "report"],
   work_pool_name = work_pool_name,
)
uuid = scraper_pattern_report_flow_deployment.apply()
print(f"deployment -> scraper_pattern_report_flow 完了")

####################
# Flow Net系
####################
_ = morning_flow_net.to_deployment(name="dummy")
morning_flow_net_deployment = _.from_entrypoint(
    name = "daily-morning",
    entrypoint= str(_.entrypoint),
    tags = ["daily", "morning", "net", "report", "mongodb"],
   work_pool_name = work_pool_name,
)
uuid = morning_flow_net_deployment.apply()
print(f"deployment -> morning_flow_net 完了")




