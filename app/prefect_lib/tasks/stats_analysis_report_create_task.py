from prefect import task

from prefect_lib.data_models.stats_analysis_report_excel import StatsAnalysisReportExcel
from prefect_lib.data_models.stats_analysis_report_input import StatsAnalysisReportInput
from prefect_lib.data_models.stats_info_collect_data import StatsInfoCollectData


@task
def stats_analysis_report_create_task(
    stats_analysis_report_input: StatsAnalysisReportInput,
    stats_info_collect_data: StatsInfoCollectData,
) -> StatsAnalysisReportExcel:
    """スクレイパー情報解析レポート用Excelを作成"""

    stats_analysis_report_excel = StatsAnalysisReportExcel(
        stats_info_collect_data, stats_analysis_report_input.datetime_term_list()
    )

    return stats_analysis_report_excel
