from typing import Any, Optional
from prefect import task, get_run_logger
from datetime import datetime
from pymongo import ASCENDING
from pymongo.cursor import Cursor
from prefect_lib.flows import START_TIME
from prefect_lib.common_module.scraped_record_error_check import (
    scraped_record_error_check,
)
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.scraped_from_response_model import (
    ScrapedFromResponseModel,
)
from BrownieAtelierMongo.collection_models.crawler_response_model import (
    CrawlerResponseModel,
)
from BrownieAtelierMongo.collection_models.news_clip_master_model import (
    NewsClipMasterModel,
)


@task
def news_clip_master_save_task(
    mongo: MongoModel,
    domain: Optional[str],
    target_start_time_from: Optional[datetime],
    target_start_time_to: Optional[datetime],
):
    """
    scrapyによるクロールを実行する。
    ・対象のスパイダーを指定できる。
    ・スパイダーの実行時オプションを指定できる。
    ・後続のスクレイピング〜news_clipへの登録まで一括で実行するか選択できる。
    """
    logger = get_run_logger()  # PrefectLogAdapter

    scraped_from_response: ScrapedFromResponseModel = ScrapedFromResponseModel(mongo)
    news_clip_master: NewsClipMasterModel = NewsClipMasterModel(mongo)
    crawler_response: CrawlerResponseModel = CrawlerResponseModel(mongo)

    conditions: list = []
    if domain:
        conditions.append({ScrapedFromResponseModel.DOMAIN: domain})
    if target_start_time_from:
        conditions.append(
            {
                ScrapedFromResponseModel.SCRAPYING_START_TIME: {
                    "$gte": target_start_time_from
                }
            }
        )
    if target_start_time_to:
        conditions.append(
            {
                ScrapedFromResponseModel.SCRAPYING_START_TIME: {
                    "$lte": target_start_time_to
                }
            }
        )

    if conditions:
        filter: Any = {"$and": conditions}
    else:
        filter = None

    logger.info("=== scraped_from_response へのfilter: " + str(filter))

    # 対象件数を確認
    record_count = scraped_from_response.count(
        filter=filter,
    )
    logger.info("=== news_clip_master への登録チェック対象件数 : " + str(record_count))

    # 100件単位で処理を実施
    limit: int = 100
    skip_list = list(range(0, record_count, limit))

    for skip in skip_list:
        records: Cursor = (
            scraped_from_response.find(
                filter=filter,
                sort=[(ScrapedFromResponseModel.RESPONSE_TIME, ASCENDING)],
            )
            .skip(skip)
            .limit(limit)
        )

        for record in records:
            # # データチェック
            # if not scraped_record_error_check(record):
            # 重複チェック
            filter = {
                "$and": [
                    {NewsClipMasterModel.URL: record[ScrapedFromResponseModel.URL]},
                    {NewsClipMasterModel.TITLE: record[ScrapedFromResponseModel.TITLE]},
                    {
                        NewsClipMasterModel.ARTICLE: record[
                            ScrapedFromResponseModel.ARTICLE
                        ]
                    },
                ]
            }
            news_clip_records = news_clip_master.find(filter=filter)

            # 重複するレコードがなければ保存する。
            if news_clip_master.count(filter) == 0:
                # record[news_clip_master.SCRAPED_SAVE_START_TIME] = start_time
                _ = {}
                _[NewsClipMasterModel.SCRAPED_SAVE_START_TIME] = START_TIME
                _.update(record)
                news_clip_master.insert_one(_)
                logger.info(
                    f"=== news_clip_master への登録 : {record[ScrapedFromResponseModel.URL]}"
                )

                news_clip_master_register: str = (
                    CrawlerResponseModel.NEWS_CLIP_MASTER_REGISTER__COMPLETE
                )  #'登録完了'
                crawler_response.news_clip_master_register_result(
                    record[ScrapedFromResponseModel.URL],
                    record[ScrapedFromResponseModel.RESPONSE_TIME],
                    news_clip_master_register,
                )
            else:
                for news_clip_record in news_clip_records:
                    if (
                        news_clip_record[NewsClipMasterModel.RESPONSE_TIME]
                        == record[ScrapedFromResponseModel.RESPONSE_TIME]
                    ):
                        logger.info(
                            f"=== news_clip_master への登録処理済みデータのためスキップ : {record[ScrapedFromResponseModel.URL]}"
                        )
                    else:
                        news_clip_master_register: str = (
                            CrawlerResponseModel.NEWS_CLIP_MASTER_REGISTER__SKIP
                        )  #'登録内容に差異なしのため不要'
                        crawler_response.news_clip_master_register_result(
                            record[ScrapedFromResponseModel.URL],
                            record[ScrapedFromResponseModel.RESPONSE_TIME],
                            news_clip_master_register,
                        )
                        logger.info(
                            f"=== news_clip_master の登録内容に変更がないためスキップ : {record[ScrapedFromResponseModel.URL]}"
                        )
