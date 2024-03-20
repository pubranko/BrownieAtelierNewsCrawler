from typing import Any

from BrownieAtelierMongo.collection_models.controller_model import \
    ControllerModel
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from prefect import get_run_logger, task
from shared.directory_search_spiders import DirectorySearchSpiders


@task
def first_crawling_target_spiders_task(mongo: MongoModel) -> list[dict[str, Any]]:
    """
    scrapyによるクロールを実行するための対象スパイダー情報の一覧を生成する。
    """
    logger = get_run_logger()  # PrefectLogAdapter

    # 初回観測の対象spiders_infoを抽出
    directory_search_spiders = DirectorySearchSpiders()
    controller: ControllerModel = ControllerModel(mongo)
    regular_observation_spider_name_set: set = (
        controller.regular_observation_spider_name_set_get()
    )

    # 初回観測の対象スパイダー情報、スパイダー名称保存リスト
    crawling_target_spiders: list = []
    crawling_target_spiders_name: list = []

    # 対象スパイダーの絞り込み
    for spider_name in directory_search_spiders.spiders_name_list_get():
        spider_info = directory_search_spiders.spiders_info[spider_name]
        crawl_point_record: dict = controller.crawl_point_get(
            spider_info[directory_search_spiders.DOMAIN_NAME],
            spider_name,
        )

        if not spider_name in regular_observation_spider_name_set:
            logger.info(f"=== 定期観測に登録がないスパイダーは対象外 : {spider_name}")
        elif len(crawl_point_record):
            logger.info(f"=== クロールポイントがある（既にクロール実績がある）スパイダーは対象外 : {spider_name}")
        else:
            crawling_target_spiders.append(spider_info)
            crawling_target_spiders_name.append(spider_name)

    logger.info(f"=== 初回定期観測対象スパイダー一覧 : {crawling_target_spiders_name}")

    return crawling_target_spiders
