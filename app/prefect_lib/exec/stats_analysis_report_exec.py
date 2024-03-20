from datetime import date
from prefect_lib.data_models.stats_analysis_report_input import StatsAnalysisReportConst
from prefect_lib.flows.stats_analysis_report_flow import stats_analysis_report_flow


"""基準日(0:00:00)〜翌日(0:00:00)までの期間が対象となる。"""
stats_analysis_report_flow(
    # report_term=StatsAnalysisReportConst.REPORT_TERM__DAILY,
    # report_term=StatsAnalysisReportConst.REPORT_TERM__WEEKLY,
    report_term=StatsAnalysisReportConst.REPORT_TERM__MONTHLY,
    # report_term=StatsAnalysisReportConst.REPORT_TERM__YEARLY,
    totalling_term=StatsAnalysisReportConst.TOTALLING_TERM__DAILY,
    # totalling_term=StatsAnalysisReportConst.TOTALLING_TERM__WEEKLY,
    # totalling_term=StatsAnalysisReportConst.TOTALLING_TERM__MONTHLY,
    # totalling_term=StatsAnalysisReportConst.TOTALLING_TERM__YEARLY,
    base_date=date(2023, 8, 1),  # 左記基準日の１日前、１週間前、１ヶ月前、１年前のデータが対象となる。
)
