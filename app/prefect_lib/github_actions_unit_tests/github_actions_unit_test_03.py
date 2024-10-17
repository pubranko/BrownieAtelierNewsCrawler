from prefect.testing.utilities import prefect_test_harness

def test_exec():
    with prefect_test_harness():
        # カレントディレクトリをpythonpathに追加
        import os
        import sys
        current_directory = os.environ.get('PWD')
        if current_directory:
            sys.path.append(current_directory)

        # <5>
        # 各ニュースサイト別に、定期観測クローリングのON/OFF、スクレイピングのON/OFF指定を登録する。
        #   stop_controller_update_flow.py
        #   a:産経:クローリングをOFF、b:朝日:スクレイピングをOFF、c:読売:何もしない。
        from prefect_lib.flows.stop_controller_update_const import (
            StopControllerUpdateConst)
        from prefect_lib.flows.stop_controller_update_flow import (
            stop_controller_update_flow)
        stop_controller_update_flow(
            domain="sankei.com",
            command=StopControllerUpdateConst.COMMAND_ADD,
            destination=StopControllerUpdateConst.CRAWLING,
            # destination=StopControllerUpdateConst.SCRAPYING,
        )
        stop_controller_update_flow(
            domain="asahi.com",
            command=StopControllerUpdateConst.COMMAND_ADD,
            # destination=StopControllerUpdateConst.CRAWLING,
            destination=StopControllerUpdateConst.SCRAPYING,
        )


        # <6>
        # 定期観測
        #   regular_observation_flow.py
        #     産経：クローリング・スクレイピングがスキップされる。
        #     朝日：クローリングのみ稼働。スクレイピングがスキップされる。
        #     読売：通常稼働。
        from prefect_lib.flows.regular_observation_flow import regular_observation_flow
        regular_observation_flow()

if __name__ == "__main__":
    test_exec()
