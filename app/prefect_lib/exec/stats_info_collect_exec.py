from datetime import date

from prefect_lib.flows.stats_info_collect_flow import stats_info_collect_flow
from shared.settings import TIMEZONE

"""
基準日(0:00:00)〜翌日(0:00:00)までの期間が対象となる。
引数なし→前日(0:00:00)～当日(0:00:00)が対象となる。
"""
stats_info_collect_flow(base_date=date(2024, 10, 8))
# stats_info_collect_flow()
