from datetime import datetime
from prefect.testing.utilities import prefect_test_harness
from prefect_lib.flows.manual_news_clip_master_save_flow import \
    manual_news_clip_master_save_flow
from shared.settings import TIMEZONE

def test_exec():
    with prefect_test_harness():

        manual_news_clip_master_save_flow(
            # domain='sankei_com_sitemap',
            domain=None,
            target_start_time_from=datetime(2024, 10, 12, 0, 0, 0, 0).astimezone(TIMEZONE),
            target_start_time_to=datetime(2024, 10, 12, 23, 59, 0, 0).astimezone(TIMEZONE),
        )

if __name__ == "__main__":
    test_exec()

# from datetime import datetime
# from shared.settings import TIMEZONE
# from prefect_lib.flow.scraped_news_clip_master_save_flow import flow

# # domain、scrapying_start_time_*による絞り込みは任意
# flow.run(parameters=dict(
#     domain='yomiuri_co_jp',
#     #domain='kyodo.co.jp',
#     #domain='epochtimes.jp',
#     target_start_time_from=datetime(2023, 5, 9, 21, 0, 0).astimezone(TIMEZONE),
#     target_start_time_to=datetime(2023, 5, 9, 22, 18, 12, 160000).astimezone(TIMEZONE),
#     #target_start_time_from=datetime(2021, 9, 25, 15, 26, 37, 344148).astimezone(TIMEZONE),
#     #target_start_time_to=datetime(2021, 9, 25, 15, 26, 37, 344148).astimezone(TIMEZONE),
# ))
