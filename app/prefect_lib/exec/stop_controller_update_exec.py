import os
import glob
from prefect_lib.flows.stop_controller_update_flow import (
    stop_controller_update_flow,
    StopControllerUpdateConst,
)

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
