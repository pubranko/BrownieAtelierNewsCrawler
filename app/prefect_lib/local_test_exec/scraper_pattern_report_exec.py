from datetime import datetime
from prefect.testing.utilities import prefect_test_harness
from prefect_lib.flows.scraper_pattern_report_flow import \
    scraper_pattern_report_flow
from prefect_lib.data_models.scraper_pattern_report_input import ScraperPatternReportConst
from shared.settings import TIMEZONE


def test_exec():
    with prefect_test_harness():

        scraper_pattern_report_flow(
            # report_term=ScraperPatternReportConst.REPORT_TERM__DAILY,
            # report_term=ScraperPatternReportConst.REPORT_TERM__WEEKLY,
            report_term=ScraperPatternReportConst.REPORT_TERM__MONTHLY,
            # report_term=ScraperPatternReportConst.REPORT_TERM__YEARLY,
            base_date=datetime(2024, 10, 13).astimezone(TIMEZONE),  # 左記基準日の前日分のデータが対象となる。
        )

if __name__ == "__main__":
    test_exec()