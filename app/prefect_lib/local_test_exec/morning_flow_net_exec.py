from datetime import datetime, timedelta
from prefect_lib.flow_nets.morning_flow_net import morning_flow_net
from prefect.testing.utilities import prefect_test_harness
from shared.settings import TIMEZONE

def test_exec():
    with prefect_test_harness():

        # 絞り込み用の引数がなければ全量チェック
        morning_flow_net()

        morning_flow_net(
            datetime(2024, 10, 20, 0, 0, 0, 000000).astimezone(TIMEZONE)
        )
    
if __name__ == "__main__":
    test_exec()