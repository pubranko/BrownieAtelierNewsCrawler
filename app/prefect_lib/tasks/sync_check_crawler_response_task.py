from datetime import datetime
from typing import Any, Optional

from BrownieAtelierMongo.collection_models.asynchronous_report_model import \
    AsynchronousReportModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import \
    CrawlerLogsModel
from BrownieAtelierMongo.collection_models.crawler_response_model import \
    CrawlerResponseModel
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from prefect import get_run_logger, task
from prefect_lib.flows import START_TIME
from shared.timezone_recovery import timezone_recovery


@task
def sync_check_crawler_response_task(
    mongo: MongoModel,
    domain: Optional[str],
    start_time_from: Optional[datetime],
    start_time_to: Optional[datetime],
):
    """crawl対象のurlとcrawler_responseの同期チェック"""

    # ロガー取得
    logger = get_run_logger()  # PrefectLogAdapter
    # 各コレクションモデル生成
    crawler_logs = CrawlerLogsModel(mongo)
    crawler_response = CrawlerResponseModel(mongo)
    asynchronous_report_model = AsynchronousReportModel(mongo)

    # スパイダーレポートより、クロール対象となったurlのリストを取得し一覧にする。
    conditions: list = []
    conditions.append(
        {CrawlerLogsModel.RECORD_TYPE: CrawlerLogsModel.RECORD_TYPE__SPIDER_REPORTS}
    )
    if domain:
        conditions.append({CrawlerLogsModel.DOMAIN: domain})
    if start_time_from:
        conditions.append({CrawlerLogsModel.START_TIME: {"$gte": start_time_from}})
    if start_time_to:
        conditions.append({CrawlerLogsModel.START_TIME: {"$lte": start_time_to}})
    if conditions:
        log_filter: Any = {"$and": conditions}
    else:
        log_filter = None


    #####################################
    # クローラーレスポンスの有無をチェック #
    #####################################
    conditions: list = []
    if domain:
        conditions.append({CrawlerResponseModel.DOMAIN: domain})
    if start_time_from:
        conditions.append(
            {CrawlerResponseModel.CRAWLING_START_TIME: {"$gte": start_time_from}}
        )
    if start_time_to:
        conditions.append(
            {CrawlerResponseModel.CRAWLING_START_TIME: {"$lte": start_time_to}}
        )

    response_sync_list: list = []  # crawler_logsとcrawler_responseで同期
    response_async_list: list = []  # crawler_logsとcrawler_responseで非同期
    response_async_domain_aggregate: dict = {}
    # for log_record in log_records:
    
    crawler_logs_count:int = crawler_logs.count_documents(filter=log_filter)
    logger.info(
        f"=== 同期チェック(crawler_logs)件数 : {crawler_logs_count})"
    )

    processed_count:int = 0

    for log_record in crawler_logs.limited_find(
        filter=log_filter,
        projection={CrawlerLogsModel.CRAWL_URLS_LIST: 1, CrawlerLogsModel.DOMAIN: 1},
    ):
        # domain別の集計エリアを初期設定
        if not log_record[CrawlerLogsModel.DOMAIN] in response_async_domain_aggregate:
            response_async_domain_aggregate[log_record[CrawlerLogsModel.DOMAIN]] = 0

        # crawl_urls_listからをクロール対象となったurlを抽出
        loc_crawl_urls: list = []
        for temp in log_record[CrawlerLogsModel.CRAWL_URLS_LIST]:
            loc_crawl_urls.extend(
                [
                    item[CrawlerLogsModel.CRAWL_URLS_LIST__LOC]
                    for item in temp[CrawlerLogsModel.CRAWL_URLS_LIST__ITEMS]
                ]
            )

        # スパイダーレポートよりクロール対象となったurlを順に読み込み、crawler_responseに登録されていることを確認する。
        for crawl_url in loc_crawl_urls:
            conditions.append({CrawlerResponseModel.URL: crawl_url})
            master_filter: Any = {"$and": conditions}

            # crawler_response側に存在しないクロール対象urlがある場合
            if crawler_response.count_documents(filter=master_filter) == 0:
                response_async_list.append(crawl_url)
                # 非同期ドメイン集計カウントアップ
                response_async_domain_aggregate[
                    log_record[CrawlerLogsModel.DOMAIN]
                ] += 1

            # クロール対象とcrawler_responseで同期している場合、同期リストへ保存
            # ※定期観測では1件しか存在しないないはずだが、start_time_from〜toで一定の範囲の
            # 同期チェックを行った場合、複数件発生する可能性がある。
            # for response_record in response_records:
            for response_record in crawler_response.limited_find(
                filter=master_filter,
                projection={
                    CrawlerResponseModel.URL: 1,
                    CrawlerResponseModel.RESPONSE_TIME: 1,
                    CrawlerResponseModel.DOMAIN: 1,
                },
            ):
                _ = {
                    CrawlerResponseModel.URL: response_record[CrawlerResponseModel.URL],
                    CrawlerResponseModel.RESPONSE_TIME: timezone_recovery(
                        response_record[CrawlerResponseModel.RESPONSE_TIME]
                    ),
                    CrawlerResponseModel.DOMAIN: response_record[
                        CrawlerResponseModel.DOMAIN
                    ],
                }
                if CrawlerResponseModel.NEWS_CLIP_MASTER_REGISTER in response_record:
                    _[CrawlerResponseModel.NEWS_CLIP_MASTER_REGISTER] = response_record[
                        CrawlerResponseModel.NEWS_CLIP_MASTER_REGISTER
                    ]
                response_sync_list.append(_)

            # 参照渡しなので最後に消さないと上述のresponse_recordsを参照した段階でエラーとなる
            conditions.pop(-1)

        # 処理済みの件数を５００件ごとにログへ出力
        processed_count += 1
        if processed_count % 500 == 0:
            logger.info(f"=== 同期チェック(crawler_logs)処理済み件数 : {processed_count}/{crawler_logs_count}")

    # クロールミス分のurlがあれば、非同期レポートへ保存
    if len(response_async_list) > 0:
        asynchronous_report_model.insert_one(
            {
                AsynchronousReportModel.RECORD_TYPE: AsynchronousReportModel.RECORD_TYPE__NEWS_CRAWL_ASYNC,
                AsynchronousReportModel.START_TIME: START_TIME,
                AsynchronousReportModel.PARAMETER: {
                    AsynchronousReportModel.DOMAIN: domain,
                    AsynchronousReportModel.START_TIME_FROM: start_time_from,
                    AsynchronousReportModel.START_TIME_TO: start_time_to,
                },
                AsynchronousReportModel.ASYNC_LIST: response_async_list,
            }
        )
        counter = f"エラー({len(response_async_list)})/正常({len(response_sync_list)})"
        logger.warning(f"=== 同期チェック結果(crawler -> response) : NG({counter})")
    else:
        logger.info(
            f"=== 同期チェック(crawler -> response)結果 : OK(件数 : {len(response_sync_list)})"
        )

    return response_sync_list, response_async_list, response_async_domain_aggregate
