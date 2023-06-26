from datetime import datetime
from prefect_lib.flows.crawl_sync_check_flow import crawl_sync_check_flow
from shared.settings import TIMEZONE


crawl_sync_check_flow(
    domain='sankei.com',
    start_time_from=datetime(2023, 6, 1, 0, 0, 0, 000000).astimezone(TIMEZONE),
    start_time_to=datetime(2023, 6, 30, 23, 59, 59, 999999).astimezone(TIMEZONE),
)