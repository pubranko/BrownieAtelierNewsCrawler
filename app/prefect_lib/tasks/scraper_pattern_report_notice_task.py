import os

from BrownieAtelierNotice.mail_attach_send import mail_attach_send
from openpyxl import Workbook
from prefect import get_run_logger, task
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

    title = "scraper_pattern_analysis_report"

    messege = f"""
    <html>
        <body>
            <p>各種実行結果を解析したレポート</p>
            <p>=== 実行条件 ============================================================</p>
            <p>start_time = {START_TIME.isoformat()}</p>
            <p>base_date_from = {base_date_from.isoformat()}</p>
            <p>base_date_to = {base_date_to.isoformat()}</p>
            <p>report_term = {scraper_pattern_report_input.report_term}</p>
            <p>=========================================================================</p>
        </body>
    </html>"""
    # メール送信
    mail_attach_send(title=title, msg=messege, filepath=file_path, param_logger=logger)
