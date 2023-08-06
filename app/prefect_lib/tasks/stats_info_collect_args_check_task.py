from typing import Any, Optional
from datetime import datetime, date
from prefect import task, get_run_logger
from pydantic import ValidationError

from prefect_lib.data_models.stats_info_collect_input import StatsInfoCollectInput
from prefect_lib.flows import START_TIME


@task
def stats_info_collect_args_check_task(base_date: Optional[date] = None) -> StatsInfoCollectInput:
    '''
    '''
    logger = get_run_logger()   # PrefectLogAdapter
    logger.info(f'=== 引数 : base_date={base_date}')

    try:
        stats_info_collect_input = StatsInfoCollectInput(base_date=base_date)
    except ValidationError as e:
        # e.json()エラー結果をjson形式で見れる。
        # e.errors()エラー結果をdict形式で見れる。
        # str(e)エラー結果をlist形式で見れる。
        logger.error(
            f'=== エラー内容: {e.errors()}')
        raise ValueError()

    logger.info(
        f'=== 基準日from ~ to : {stats_info_collect_input.base_date_get(START_TIME)}')

    return stats_info_collect_input