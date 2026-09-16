"""
各フローを登録する。
以下の操作を事前に行っておくこと。
・prefect cloud login --key xxx
・export PREFECT_HOME= xxx
・localの場合 → prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
・localコンテナーの場合 → prefect config set PREFECT_API_URL="http://0.0.0.0:4200/api"
・Cloudの場合 → prefect config set PREFECT_API_URL="https://api.prefect.cloud/api/accounts/[ACCOUNT-ID]/workspaces/[WORKSPACE-ID]"
"""

import asyncio
import os
import sys


async def main() -> None:
    from decouple import config

    # from prefect.deployments.deployments import Deployment
    # from prefect.settings import PREFECT_API_URL, PREFECT_HOME
    from prefect.settings import get_current_settings

    # flow_net系
    from prefect_lib.flow_nets.morning_flow_net import morning_flow_net

    # 必要な引数定義
    # check系
    from prefect_lib.flows.crawl_sync_check_flow import crawl_sync_check_flow
    from prefect_lib.flows.first_observation_flow import first_observation_flow

    # crawl-scrape系
    from prefect_lib.flows.manual_crawling_flow import manual_crawling_flow
    from prefect_lib.flows.manual_news_clip_master_save_flow import manual_news_clip_master_save_flow
    from prefect_lib.flows.manual_scrapying_flow import manual_scrapying_flow

    # mongodb系
    from prefect_lib.flows.mongo_delete_selector_flow import mongo_delete_selector_flow
    from prefect_lib.flows.mongo_export_selector_flow import mongo_export_selector_flow
    from prefect_lib.flows.mongo_import_selector_flow import mongo_import_selector_flow
    from prefect_lib.flows.regular_observation_controller_update_flow import regular_observation_controller_update_flow
    from prefect_lib.flows.regular_observation_flow import regular_observation_flow

    # register系
    from prefect_lib.flows.scraper_info_uploader_flow import scraper_info_by_domain_flow
    from prefect_lib.flows.scraper_pattern_report_flow import scraper_pattern_report_flow
    from prefect_lib.flows.stats_analysis_report_flow import stats_analysis_report_flow

    # report系
    from prefect_lib.flows.stats_info_collect_flow import stats_info_collect_flow
    from prefect_lib.flows.stop_controller_update_flow import stop_controller_update_flow

    settings = get_current_settings()
    prefect_home = settings.home
    print(f"=== {prefect_home = }")
    prefect_api_url = settings.api.url
    print(f"=== {prefect_api_url = }")

    if not (prefect_api_url):
        raise ValueError("PREFECT_API_URLが設定されていないため、フロー登録を停止します。")
    path = os.getcwd()
    print(f"=== {path =}")

    work_pool_name = str(config("PREFECT__WORK_POOL"))
    print(f"=== {work_pool_name =}")

    ###################
    # crawl-scrape
    ###################
    # name -> デプロイの名前。可動タイミングがわかるように manual, daily, monthly, weekly, yearly
    # tags -> 自動・手動、系統、実行タイミングを指定する。

    manual_crawling_flow_deployment = await manual_crawling_flow.ato_deployment(
        name="manual-crawl-scrape",
        tags=["manual", "crawl-scrape"],
        work_pool_name=work_pool_name,
    )
    uuid = await manual_crawling_flow_deployment.aapply()
    print(f"deployment -> manual_crawling_flow 完了  ({uuid =})")

    manual_scrapying_flow_deployment = await manual_scrapying_flow.ato_deployment(
        name="manual-crawl-scrape",
        tags=["manual", "crawl-scrape"],
        work_pool_name=work_pool_name,
    )
    uuid = await manual_scrapying_flow_deployment.aapply()
    print("deployment -> manual_scrapying_flow 完了")

    manual_news_clip_master_save_flow_deployment = await manual_news_clip_master_save_flow.ato_deployment(
        name="manual-crawl-scrape",
        tags=["manual", "crawl-scrape"],
        work_pool_name=work_pool_name,
    )
    uuid = await manual_news_clip_master_save_flow_deployment.aapply()
    print("deployment -> manual_news_clip_master_save_flow 完了")

    first_observation_flow_deployment = await first_observation_flow.ato_deployment(
        name="manual-crawl-scrape",
        tags=["manual", "crawl-scrape"],
        work_pool_name=work_pool_name,
    )
    uuid = await first_observation_flow_deployment.aapply()
    print("deployment -> first_observation_flow 完了")

    regular_observation_flow_deployment = await regular_observation_flow.ato_deployment(
        name="auto-crawl-scrape",
        tags=["auto", "daily", "crawl-scrape"],
        work_pool_name=work_pool_name,
    )
    uuid = await regular_observation_flow_deployment.aapply()
    print("deployment -> regular_observation_flow 完了")

    ###################
    # register
    ###################
    scraper_info_by_domain_flow_deployment = await scraper_info_by_domain_flow.ato_deployment(
        name="register",
        tags=["manual", "register"],
        work_pool_name=work_pool_name,
    )
    uuid = await scraper_info_by_domain_flow_deployment.aapply()
    print("deployment -> scraper_info_by_domain_flow 完了")

    regular_observation_controller_update_flow_deployment = (
        await regular_observation_controller_update_flow.ato_deployment(
            name="register",
            tags=["manual", "register"],
            work_pool_name=work_pool_name,
        )
    )
    uuid = await regular_observation_controller_update_flow_deployment.aapply()
    print("deployment -> regular_observation_controller_update_flow 完了")

    stop_controller_update_flow_deployment = await stop_controller_update_flow.ato_deployment(
        name="register",
        tags=["manual", "register"],
        work_pool_name=work_pool_name,
    )
    uuid = await stop_controller_update_flow_deployment.aapply()
    print("deployment -> stop_controller_update_flow 完了")

    ###################
    # check
    ###################
    crawl_sync_check_flow_deployment = await crawl_sync_check_flow.ato_deployment(
        name="check",
        tags=["manual", "check", "report"],
        work_pool_name=work_pool_name,
    )
    uuid = await crawl_sync_check_flow_deployment.aapply()
    print("deployment -> crawl_sync_check_flow 完了")

    ###################
    # mongodb
    ###################
    mongo_delete_selector_flow_deployment = await mongo_delete_selector_flow.ato_deployment(
        name="mongodb",
        tags=["manual", "mongodb"],
        work_pool_name=work_pool_name,
    )
    uuid = await mongo_delete_selector_flow_deployment.aapply()
    print("deployment -> mongo_delete_selector_flow 完了")

    mongo_export_selector_flow_deployment = await mongo_export_selector_flow.ato_deployment(
        name="mongodb",
        tags=["manual", "mongodb"],
        work_pool_name=work_pool_name,
    )
    uuid = await mongo_export_selector_flow_deployment.aapply()
    print("deployment -> mongo_export_selector_flow 完了")

    mongo_import_selector_flow_deployment = await mongo_import_selector_flow.ato_deployment(
        name="mongodb",
        tags=["manual", "mongodb"],
        work_pool_name=work_pool_name,
    )
    uuid = await mongo_import_selector_flow_deployment.aapply()
    print("deployment -> mongo_import_selector_flow 完了")

    ###################
    # report
    ###################
    stats_info_collect_flow_deployment = await stats_info_collect_flow.ato_deployment(
        name="report",
        tags=["manual", "report"],
        work_pool_name=work_pool_name,
    )
    uuid = await stats_info_collect_flow_deployment.aapply()
    print("deployment -> stats_info_collect_flow 完了")

    stats_analysis_report_flow_deployment = await stats_analysis_report_flow.ato_deployment(
        name="report",
        tags=["manual", "report"],
        # parameters=dict(
        #     report_term=StatsAnalysisReportConst.REPORT_TERM__WEEKLY,  # １週間の間、1日単位の集計結果を求める。
        #     totalling_term=StatsAnalysisReportConst.TOTALLING_TERM__DAILY,
        # ),
        work_pool_name=work_pool_name,
    )
    uuid = await stats_analysis_report_flow_deployment.aapply()
    print("deployment -> stats_analysis_report_flow 完了")

    scraper_pattern_report_flow_deployment = await scraper_pattern_report_flow.ato_deployment(
        name="report",
        tags=["manual", "report"],
        work_pool_name=work_pool_name,
    )
    uuid = await scraper_pattern_report_flow_deployment.aapply()
    print("deployment -> scraper_pattern_report_flow 完了")

    ####################
    # Flow Net系
    ####################
    morning_flow_net_deployment = await morning_flow_net.ato_deployment(
        name="daily-morning",
        tags=["daily", "morning", "net", "report", "mongodb"],
        work_pool_name=work_pool_name,
    )
    uuid = await morning_flow_net_deployment.aapply()
    print("deployment -> morning_flow_net 完了")


if __name__ == "__main__":
    sys.path.append(os.getcwd())
    asyncio.run(main())
