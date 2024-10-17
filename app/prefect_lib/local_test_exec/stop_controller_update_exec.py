import glob
import os
from prefect.testing.utilities import prefect_test_harness
# app/prefect_lib/flows/stop_controller_update_const.py
from prefect_lib.flows.stop_controller_update_const import StopControllerUpdateConst
from prefect_lib.flows.stop_controller_update_flow import stop_controller_update_flow


def test_exec():
    with prefect_test_harness():

        stop_controller_update_flow(
            domain="sankei.com",
            # domain='epochtimes.jp',
            # command=StopControllerUpdateConst.COMMAND_ADD,
            command=StopControllerUpdateConst.COMMAND_DELETE,
            destination=StopControllerUpdateConst.CRAWLING,
            # destination=StopControllerUpdateConst.SCRAPYING,
        )


        # テスト時のログファイル削除漏れ防止用
        for log_file in glob.glob("/tmp/prefect_log_*"):
            print(f"削除漏れlog_file削除: {log_file}")
            os.remove(log_file)


if __name__ == "__main__":
    test_exec()