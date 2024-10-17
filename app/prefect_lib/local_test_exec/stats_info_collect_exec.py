from datetime import date
from prefect.testing.utilities import prefect_test_harness
from prefect_lib.flows.stats_info_collect_flow import stats_info_collect_flow
from shared.settings import TIMEZONE

def test_exec():
    with prefect_test_harness():

        """
        基準日(0:00:00)〜翌日(0:00:00)までの期間が対象となる。
        引数なし→前日(0:00:00)～当日(0:00:00)が対象となる。
        """
        stats_info_collect_flow(base_date=date(2024, 10, 12))
        # stats_info_collect_flow()

if __name__ == "__main__":
    test_exec()