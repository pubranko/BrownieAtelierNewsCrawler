from datetime import datetime

# from prefect_lib.flow.scraper_pattern_report_flow import flow
from prefect_lib.flows.scraper_pattern_report_flow import \
    scraper_pattern_report_flow
from prefect_lib.data_models.scraper_pattern_report_input import ScraperPatternReportConst
from shared.settings import TIMEZONE

# flow.run(parameters=dict(
#     report_term='daily',
#     # report_term='weekly',
#     #report_term='monthly',
#     #report_term='yearly',
#     base_date=datetime(2023, 3, 20).astimezone(TIMEZONE),   # 左記基準日の前日分のデータが対象となる。
# ))

scraper_pattern_report_flow(
    # report_term=ScraperPatternReportConst.REPORT_TERM__DAILY,
    # report_term=ScraperPatternReportConst.REPORT_TERM__WEEKLY,
    report_term=ScraperPatternReportConst.REPORT_TERM__MONTHLY,
    # report_term=ScraperPatternReportConst.REPORT_TERM__YEARLY,
    base_date=datetime(2024, 10, 13).astimezone(TIMEZONE),  # 左記基準日の前日分のデータが対象となる。
)
