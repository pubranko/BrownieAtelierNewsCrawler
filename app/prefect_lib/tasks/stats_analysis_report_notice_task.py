import os
from prefect import get_run_logger, task
from BrownieAtelierNotice.slack.slack_notice import slack_notice
from BrownieAtelierNotice import settings
from prefect_lib.data_models.stats_analysis_report_excel import \
    StatsAnalysisReportExcel
from prefect_lib.data_models.stats_analysis_report_input import \
    StatsAnalysisReportInput
from prefect_lib.flows import START_TIME
from shared.settings import DATA


@task
def stats_analysis_report_notice_task(
    stats_analysis_report_input: StatsAnalysisReportInput,
    stats_analysis_report_excel: StatsAnalysisReportExcel,
):
    """
    scrapyによるクロールを実行するための対象スパイダー情報の一覧を生成する。
    """
    logger = get_run_logger()  # PrefectLogAdapter

    # レポートファイルを保存
    file_name: str = "stats_analysis_report.xlsx"
    file_path = os.path.join(DATA, file_name)
    stats_analysis_report_excel.workbook.save(file_path)

    logger.info(f"=== StatsAnalysisReportTask run  : レポート対象ファイル保存完了 : {file_path}")

    """メールにレポートファイルを添付して送信"""
    base_date_from, base_date_to = stats_analysis_report_input.base_date_get(START_TIME)

    title = "stats_analysis_report"
    if (
        StatsAnalysisReportExcel.stats_warning_flg
        or StatsAnalysisReportExcel.robots_warning_flg
        or StatsAnalysisReportExcel.downloder_warning_flg
    ):
        title = title + "(ワーニング有り)"

    warning_messege: str = ""
    if StatsAnalysisReportExcel.stats_warning_flg:
        warning_messege = "<p>statsでワーニング発生</p>"
    if StatsAnalysisReportExcel.robots_warning_flg:
        warning_messege = f"{warning_messege}<p>robots response statusでワーニング発生</p>"
    if StatsAnalysisReportExcel.downloder_warning_flg:
        warning_messege = f"{warning_messege}<p>downloder response statusでワーニング発生</p>"

    message = f"""
    【stats_analysis_report】
    
    各種実行結果を解析したレポート
    === 実行条件 ============================================================
    start_time = {START_TIME.isoformat()}
    base_date_from = {base_date_from.isoformat()}
    base_date_to = {base_date_to.isoformat()}
    report_term = {stats_analysis_report_input.report_term}
    totalling_term = {stats_analysis_report_input.totalling_term}
    =========================================================================
    {warning_messege}
    """

    slack_notice(
        logger=logger,
        channel_id=settings.BROWNIE_ATELIER_NOTICE__SLACK_CHANNEL_ID__NOMAL,
        message=message,
        file=file_path,
        file_name=file_name,
    )
