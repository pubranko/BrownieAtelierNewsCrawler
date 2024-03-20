from typing import Optional
from datetime import date
from prefect import task, get_run_logger
from pydantic import ValidationError

from prefect_lib.flows import START_TIME
from prefect_lib.data_models.stats_analysis_report_input import StatsAnalysisReportInput


@task
def stats_analysis_report_args_check_task(
    report_term: str, totalling_term: str, base_date: Optional[date] = None
) -> StatsAnalysisReportInput:
    """
    ・入力（Flowの引数）のバリデーションチェック。
    ・戻り値: 入力データクラス
    """
    logger = get_run_logger()  # PrefectLogAdapter
    logger.info(
        f"=== 引数 : report_term= {report_term}, totalling_term= {totalling_term}, base_date = {base_date}"
    )

    # 入力パラメータのバリデーション
    try:
        stats_analysis_report_input = StatsAnalysisReportInput(
            start_time=START_TIME,
            report_term=report_term,
            totalling_term=totalling_term,
            base_date=base_date,
        )
    except ValidationError as e:
        logger.error(f"=== バリデーションエラー: {e.errors()}")
        raise ValueError()

    logger.info(
        f"=== 基準日from ~ to : {stats_analysis_report_input.base_date_get(START_TIME)}"
    )

    return stats_analysis_report_input
