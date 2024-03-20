from datetime import datetime
from shared.settings import TIMEZONE
from prefect_lib.flows.manual_scrapying_flow import manual_scrapying_flow


manual_scrapying_flow(
    # domain='sankei_com_sitemap',
    domain="nikkei.com",
    target_start_time_from=datetime(2024, 1, 2, 15, 0, 0, 0).astimezone(TIMEZONE),
    target_start_time_to=datetime(2024, 1, 2, 15, 45, 0, 0).astimezone(TIMEZONE),
    urls=None,
    following_processing_execution=True,
    # following_processing_execution=False,
)
# from prefect_lib.flow.scrapying_flow import flow
# domain、target_start_time_*による絞り込みは任意
# flow.run(parameters=dict(
#     # domain='sankei.com',
#     # domain='asahi.com',
#     # domain='kyodo.co.jp',
#     # domain='yomiuri.co.jp',
#     # domain='jp_reuters.com',
#     # domain='epochtimes.jp',
#     # domain='mainichi.jp',
#     # domain='nikkei.com',
#     # target_start_time_from=datetime(2022, 2, 11, 16, 45, 0, 0).astimezone(TIMEZONE),
#     # target_start_time_from=datetime(2023, 3, 16, 00, 00, 0, 0).astimezone(TIMEZONE),
#     # target_start_time_to=datetime(2021, 9, 25, 11, 8, 28, 286000).astimezone(TIMEZONE),
#     # urls=['https://www.sankei.com/article/20210829-2QFVABFPMVIBNHSINK6TBYWEXE/?outputType=theme_tokyo2020',]
#     # following_processing_execution=False,    # 後続処理実行(news_clip_masterへの登録,solrへの登録)
#     # following_processing_execution=True,    # 後続処理実行(news_clip_masterへの登録,solrへの登録)
#     # following_processing_execution='Yes',    # 後続処理実行(news_clip_masterへの登録,solrへの登録)
# ))

"""
{
    "domain": "nikkei.com",
    "target_start_time_from": "2024-01-02T14:00:00+09:00",
    "target_start_time_to": "2024-01-02T15:00:00+09:00",
    "following_processing_execution": true
}

{
    "domain": "nikkei.com",
    "target_start_time_from": "2024-01-02T14:00:00+09:00",
    "target_start_time_to": "2024-01-02T15:00:00+09:00",
    "following_processing_execution": false
}


"""
