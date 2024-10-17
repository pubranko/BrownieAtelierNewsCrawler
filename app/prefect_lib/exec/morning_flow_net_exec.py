from prefect_lib.flow_nets.morning_flow_net import morning_flow_net
from prefect.testing.utilities import prefect_test_harness

def test_exec():
    with prefect_test_harness():

        # 絞り込み用の引数がなければ全量チェック
        morning_flow_net()
    
if __name__ == "__main__":
    test_exec()