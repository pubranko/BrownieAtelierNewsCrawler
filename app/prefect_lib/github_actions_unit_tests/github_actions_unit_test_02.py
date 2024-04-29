# カレントディレクトリをpythonpathに追加
import os
import sys
current_directory = os.environ.get('PWD')
if current_directory:
    sys.path.append(current_directory)

# <3>
# 各ニュースサイト別に、定期観測クローリングのON/OFF、スクレイピングのON/OFF指定を登録する。
#   stop_controller_update_flow.py
#   a:産経:クローリングをOFF、b:朝日:スクレイピングをOFF、c:読売:何もしない。
from prefect_lib.flows.stop_controller_update_flow import (
    StopControllerUpdateConst, stop_controller_update_flow)
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

# <4>
# 定期観測
#   regular_observation_flow.py
#     産経：クローリング・スクレイピングがスキップされる。
#     朝日：クローリングのみ稼働。スクレイピングがスキップされる。
#     読売：通常稼働。
from prefect_lib.flows.regular_observation_flow import regular_observation_flow
regular_observation_flow()
 
# <5>
# マニュアルスクレイピング
#   manual_scrapying_flow.py
from datetime import datetime, timedelta
from prefect_lib.flows.manual_scrapying_flow import manual_scrapying_flow
from shared.settings import TIMEZONE

manual_scrapying_flow(
    # domain='sankei_com_sitemap',
    target_start_time_from=datetime.now().astimezone(TIMEZONE) - timedelta(minutes=60),
    target_start_time_to=datetime.now().astimezone(TIMEZONE),
    # urls=None,
    following_processing_execution=True,
)

# <6>
# マニュアルニュースクリップマスター保存
#   manual_news_clip_master_save_flow.py
# from datetime import datetime
from prefect_lib.flows.manual_news_clip_master_save_flow import \
    manual_news_clip_master_save_flow
# from shared.settings import TIMEZONE

manual_news_clip_master_save_flow(
    # domain='sankei_com_sitemap',
    target_start_time_from=datetime.now().astimezone(TIMEZONE) - timedelta(minutes=60),
    target_start_time_to=datetime.now().astimezone(TIMEZONE),
)
