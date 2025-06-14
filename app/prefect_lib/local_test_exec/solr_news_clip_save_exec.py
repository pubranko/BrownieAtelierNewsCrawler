from datetime import datetime
# from prefect.testing.utilities import prefect_test_harness
# from prefect_lib.flow.solr_news_clip_save_flow import flow
from shared.settings import TIMEZONE


def test_exec():
    # with prefect_test_harness():
    pass
    # # scraped_save_start_time_*による絞り込みは任意
    # flow.run(
    #     parameters=dict(
    #         # domain='',
    #         scraped_save_start_time_from=datetime(2022, 2, 11, 17, 0, 0).astimezone(
    #             TIMEZONE
    #         ),
    #         # scraped_save_start_time_to=datetime(2021, 8, 21, 10, 18, 12, 160000).astimezone(TIMEZONE),
    #         # scraped_save_start_time_from=datetime(2021, 8, 21, 10, 18, 12, 161000).astimezone(TIMEZONE),
    #         # scraped_save_start_time_to=datetime(2021, 8, 21, 10, 18, 12, 160000).astimezone(TIMEZONE),
    #     )
    # )

if __name__ == "__main__":
    test_exec()