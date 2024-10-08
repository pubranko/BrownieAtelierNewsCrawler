import os
from openpyxl import Workbook
from prefect import get_run_logger, task
from BrownieAtelierNotice import settings
from BrownieAtelierNotice.slack.slack_notice import slack_notice
from prefect_lib.data_models.scraper_pattern_report_input import \
    ScraperPatternReportInput
from prefect_lib.flows import START_TIME
from shared.settings import DATA

@task
def scraper_pattern_report_notice_task(
    scraper_pattern_report_input: ScraperPatternReportInput, workbook: Workbook
):
    """
    scrapyによるクロールを実行するための対象スパイダー情報の一覧を生成する。
    """
    logger = get_run_logger()  # PrefectLogAdapter

    # レポートファイルを保存
    file_name: str = "scraper_pattern_analysis_report.xlsx"
    file_path = os.path.join(DATA, file_name)
    workbook.save(file_path)

    logger.info(f"=== レポート対象ファイル保存完了 : {file_path}")

    """メールにレポートファイルを添付して送信"""
    base_date_from, base_date_to = scraper_pattern_report_input.base_date_get()

    message = f"""
    【scraper_pattern_analysis_report】
    
    各種実行結果を解析したレポート
    === 実行条件 ============================================================
    start_time = {START_TIME.isoformat()}
    base_date_from = {base_date_from.isoformat()}
    base_date_to = {base_date_to.isoformat()}
    report_term = {scraper_pattern_report_input.report_term}
    =========================================================================
    """

    slack_notice(
        logger=logger,
        channel_id=settings.BROWNIE_ATELIER_NOTICE__SLACK_CHANNEL_ID__ERROR,
        message=message,
        file=file_path,
        file_name=file_name,
    )
