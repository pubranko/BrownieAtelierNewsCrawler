from datetime import datetime
from typing import Optional, Tuple

from prefect import get_run_logger, task
from prefect_lib.data_models.scraper_pattern_report_input import \
    ScraperPatternReportInput
from prefect_lib.flows import START_TIME
from pydantic import ValidationError


@task
def scraper_pattern_report_args_check_task(
    report_term: str, base_date: Optional[datetime] = None
) -> ScraperPatternReportInput:
    """
    ・入力（Flowの引数）のバリデーションチェック。
    ・戻り値: 入力データクラス
    """
    logger = get_run_logger()  # PrefectLogAdapter
    logger.info(f"=== 引数 : report_term= {report_term},  base_date = {base_date}")

    # 入力パラメータのバリデーション
    try:
        scraper_pattern_report_input = ScraperPatternReportInput(
            start_time=START_TIME,
            report_term=report_term,
            base_date=base_date,
        )
    except ValidationError as e:
        logger.error(f"=== バリデーションエラー: {e.errors()}")
        raise ValueError()

    logger.info(f"=== 基準日from ~ to : {scraper_pattern_report_input.base_date_get()}")

    return scraper_pattern_report_input
