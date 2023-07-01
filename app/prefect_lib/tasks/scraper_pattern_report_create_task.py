from prefect import task
from openpyxl import Workbook

from prefect_lib.data_models.scraper_pattern_report_excel import ScraperPatternReportExcel
from prefect_lib.data_models.scraper_pattern_report_data import ScraperPatternReportData

@task
def scraper_pattern_report_create_task(scraper_pattern_report_data: ScraperPatternReportData) -> Workbook:
    '''スクレイパー情報解析レポート作成'''

    # スクレイパー情報解析レポート用Excelを作成
    scraper_pattern_report_excel = ScraperPatternReportExcel(scraper_pattern_report_data)

    return scraper_pattern_report_excel.workbook
