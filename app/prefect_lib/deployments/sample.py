from typing import Any
from prefect_lib.flows.scraper_info_uploader_flow import scraper_info_by_domain_flow
from prefect.deployments import Deployment
from prefect.server import schemas
from prefect.server.schemas.schedules import IntervalSchedule, CronSchedule, RRuleSchedule
from shared.settings import TIMEZONE


scraper_info_by_domain_flow.flow_run_name


#スケジュールのリファレンス https://docs.prefect.io/2.10.21/api-ref/server/schemas/schedules/
# (type alias) SCHEDULE_TYPES: type[IntervalSchedule] | type[CronSchedule] | type[RRuleSchedule]
# schemas.schedules.SCHEDULE_TYPES
# schemas.schedules.

# cron -> A cron expression of the format '{second} {minute} {hour} {day} {month} {day of week}' to specify the schedule.
#       "schedule": "20 3 3 7 1 *"
#For a `TimerTrigger` to work, you provide a schedule in the form of a [cron expression](https://en.wikipedia.org/wiki/Cron#CRON_expression)
# (See the link for full details). 
# A cron expression is a string with 6 separate expressions which represent a given schedule via patterns. 
# The pattern we use to represent every 5 minutes is `0 */5 * * * *`. 
# This, in plain text, means: "When seconds is equal to 0, minutes is divisible by 5, for any hour, day of the month, month, day of the week, or year".

cron_schedule = CronSchedule(cron='20 3 3 7 1 *', timezone='Asia/Tokyo')

deployment = Deployment.build_from_flow(
    flow=scraper_info_by_domain_flow,
    name='test-deploy',
    version="1.1",
    tags=["demo"],
    parameters=dict(scraper_info_by_domain_files=['nikkei_com.json']),
    apply=True,
    # schedule= cron_schedule,
    is_schedule_active=False,    # デプロイ直後から有効にする場合True。デプロイ直後は無効とする場合False
    # work_pool_name='register',
    work_pool_name='default-agent-pool',
    # entrypoint='prefect_lib/flows/scraper_info_uploader_flow.py:scraper_info_by_domain_flow',
)
print(deployment.__dict__)
# if hasattr(deployment, 'work_queue_name'):
#     print(deployment.work_queue_name)

# if hasattr(deployment, 'apply'):
#     deployment.apply()
    
# deployment.schedule