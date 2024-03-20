from datetime import date

from prefect_lib.flows.stats_info_collect_flow import stats_info_collect_flow
from shared.settings import TIMEZONE

"""基準日(0:00:00)〜翌日(0:00:00)までの期間が対象となる。"""
# stats_info_collect_flow(base_date=date(2023, 7, 1))
stats_info_collect_flow(base_date=date(2023, 7, 1))
