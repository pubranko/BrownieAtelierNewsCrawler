from typing import Any

from BrownieAtelierMongo.collection_models.controller_model import \
    ControllerModel
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from prefect import get_run_logger, task
from prefect.cache_policies import NO_CACHE
from shared.directory_search_spiders import DirectorySearchSpiders


@task(cache_policy=NO_CACHE)
def regular_observation_task(mongo: MongoModel) -> list[dict[str, Any]]:
    """
    scrapyによるクロールを実行するための対象スパイダー情報の一覧を生成する。
    """
    logger = get_run_logger()  # PrefectLogAdapter

    directory_search_spiders = DirectorySearchSpiders()
    controller: ControllerModel = ControllerModel(mongo)
    spider_name_set: set = controller.regular_observation_spider_name_set_get()
    stop_domain: list = controller.crawling_stop_domain_list_get()

    # 定期観測の対象スパイダー情報、スパイダー名称保存リスト
    crawling_target_spiders: list = []
    crawling_target_spiders_name: list = []

    # 対象スパイダーの絞り込み
    for spider_name in directory_search_spiders.spiders_name_list_get():
        spider_info = directory_search_spiders.spiders_info[spider_name]
        crawl_point_record: dict = controller.crawl_point_get(
            spider_info[directory_search_spiders.DOMAIN_NAME],
            spider_name,
        )

        domain = spider_info[directory_search_spiders.DOMAIN]
        if domain in stop_domain:
            logger.info(
                f"=== Stop domainの指定によりクロール中止 : ドメイン({domain}) : spider_name({spider_name})"
            )
        elif not spider_name in spider_name_set:
            logger.info(
                f"=== 定期観測に登録がないスパイダーは対象外 : ドメイン({domain}) : spider_name({spider_name})"
            )
        elif len(crawl_point_record) == 0:
            logger.info(
                f"=== クロールポイントがない（初回未実行）スパイダーは対象外 : ドメイン({domain}) : spider_name({spider_name})"
            )
        else:
            crawling_target_spiders.append(spider_info)
            crawling_target_spiders_name.append(spider_name)

    logger.info(f"=== 定期観測対象スパイダー : {str(crawling_target_spiders_name)}")

    return crawling_target_spiders
