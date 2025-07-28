from datetime import datetime
from typing import Any, Optional

from BrownieAtelierMongo.collection_models.asynchronous_report_model import \
    AsynchronousReportModel
from BrownieAtelierMongo.collection_models.crawler_response_model import \
    CrawlerResponseModel
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.news_clip_master_model import \
    NewsClipMasterModel
from prefect import get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect_lib.flows import START_TIME


@task(cache_policy=NO_CACHE)
def sync_check_news_clip_master_task(
    mongo: MongoModel,
    domain: Optional[str],
    start_time_from: Optional[datetime],
    start_time_to: Optional[datetime],
    response_sync_list: list,
):
    """crawler_responseの結果news_clip_masterとの同期チェック"""

    # ロガー取得
    logger = get_run_logger()  # PrefectLogAdapter
    # 各コレクションモデル生成
    news_clip_master = NewsClipMasterModel(mongo)
    asynchronous_report_model = AsynchronousReportModel(mongo)

    """②crawler_responseとnews_clip_masterの同期チェック"""
    mastar_conditions: list = []
    master_sync_list: list = []
    master_async_list: list = []
    master_async_domain_aggregate: dict = {}
    master_filter: Any = ""


    logger.info(
        f"=== 同期チェック(response_sync_list)件数 : {len(response_sync_list)})"
    )
    processed_count:int = 0

    # crawler_responseで同期しているリストを順に読み込み、news_clip_masterに登録されているか確認する。
    for response_sync in response_sync_list:
        mastar_conditions = []
        mastar_conditions.append(
            {NewsClipMasterModel.URL: response_sync[NewsClipMasterModel.URL]}
        )
        mastar_conditions.append(
            {
                NewsClipMasterModel.RESPONSE_TIME: response_sync[
                    NewsClipMasterModel.RESPONSE_TIME
                ]
            }
        )
        master_filter = {"$and": mastar_conditions}

        # news_clip_master側に存在しないcrawler_responseがある場合
        if news_clip_master.count(filter=master_filter) == 0:
            if not CrawlerResponseModel.NEWS_CLIP_MASTER_REGISTER in response_sync:
                master_async_list.append(response_sync[CrawlerResponseModel.URL])

                # 非同期ドメイン集計カウントアップ
                if (
                    response_sync[CrawlerResponseModel.DOMAIN]
                    in master_async_domain_aggregate
                ):
                    master_async_domain_aggregate[
                        response_sync[CrawlerResponseModel.DOMAIN]
                    ] += 1
                else:
                    master_async_domain_aggregate[
                        response_sync[CrawlerResponseModel.DOMAIN]
                    ] = 1

            elif (
                response_sync[CrawlerResponseModel.NEWS_CLIP_MASTER_REGISTER]
                == "登録内容に差異なしのため不要"
            ):
                pass  # 内容に差異なしのため不要なデータ。問題なし

        # crawler_responseとnews_clip_masterで同期している場合、同期リストへ保存
        # ※通常1件しかないはずだが、障害によりリカバリした場合などは複数件存在する可能性がある。
        # for master_record in master_records:
        for master_record in news_clip_master.limited_find(
            filter=master_filter,
            projection={
                NewsClipMasterModel.URL: 1,
                NewsClipMasterModel.RESPONSE_TIME: 1,
                NewsClipMasterModel.DOMAIN: 1,
            },
        ):
            # レスポンスにあるのにマスターへの登録処理が行われていない。
            _ = {
                NewsClipMasterModel.URL: master_record[NewsClipMasterModel.URL],
                NewsClipMasterModel.RESPONSE_TIME: master_record[
                    NewsClipMasterModel.RESPONSE_TIME
                ],
                NewsClipMasterModel.DOMAIN: master_record[NewsClipMasterModel.DOMAIN],
            }
            master_sync_list.append(_)

        # 処理済みの件数を５００件ごとにログへ出力
        processed_count += 1
        if processed_count % 500 == 0:
            logger.info(f"=== 同期チェック(response_sync_list)処理済み件数 : {processed_count}/{len(response_sync_list)}")


    # スクレイピングミス分のurlがあれば、非同期レポートへ保存
    if len(master_async_list) > 0:
        asynchronous_report_model.insert_one(
            {
                AsynchronousReportModel.RECORD_TYPE: AsynchronousReportModel.RECORD_TYPE__NEWS_CLIP_MASTER_ASYNC,
                AsynchronousReportModel.START_TIME: START_TIME,
                AsynchronousReportModel.PARAMETER: {
                    AsynchronousReportModel.DOMAIN: domain,
                    AsynchronousReportModel.START_TIME_FROM: start_time_from,
                    AsynchronousReportModel.START_TIME_TO: start_time_to,
                },
                AsynchronousReportModel.ASYNC_LIST: master_async_list,
            }
        )
        counter = f"エラー({len(master_async_list)})/正常({len(master_sync_list)})"
        logger.warning(f"=== 同期チェック結果(response -> master) : NG({counter})")
    else:
        logger.info(
            f"=== 同期チェック結果(response -> master) : OK(件数 : {len(master_sync_list)})"
        )
    return master_sync_list, master_async_list, master_async_domain_aggregate
