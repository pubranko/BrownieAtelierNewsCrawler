from datetime import datetime
# from prefect.testing.utilities import prefect_test_harness
from prefect_lib.flows.crawl_sync_check_flow import crawl_sync_check_flow
from shared.settings import TIMEZONE


def test_exec():
    # with prefect_test_harness():

    # 絞り込み用の引数がなければ全量チェック
    crawl_sync_check_flow(
        # domain='sankei.com',
        start_time_from=datetime(2025, 6, 14, 0, 0, 0, 000000).astimezone(TIMEZONE),
        start_time_to=datetime(2025, 6, 14, 23, 59, 59, 999999).astimezone(TIMEZONE),
        domain=None,
        # start_time_from=None,
        # start_time_to=None,
    )

if __name__ == "__main__":
    test_exec()