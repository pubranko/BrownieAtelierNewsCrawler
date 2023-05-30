import prefect
import prefect.context
import logging
from typing import Any
from prefect import flow, get_run_logger
from prefect.context import FlowRunContext
from prefect.futures import PrefectFuture
from prefect.task_runners import SequentialTaskRunner
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.end_task import end_task
from prefect_lib.flows.common_flow import common_flow
from prefect_lib.tasks.manual_crawling_input_create_task import manual_crawling_input_create_task
from prefect_lib.tasks.manual_crawling_target_spiders_task import manual_crawling_target_spiders_task
from prefect_lib.tasks.crawling_task import crawling_task
from prefect_lib.tasks.scrapying_task import scrapying_task
from prefect_lib.tasks.news_clip_master_save_task import news_clip_master_save_task
from news_crawl.news_crawl_input import NewsCrawlInput
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel



@flow(
    flow_run_name='[CRAWL_003] Scrapy crawling flow',
    task_runner=SequentialTaskRunner())
@common_flow
def manual_crawling_flow(spider_names: list[str], spider_kwargs: dict, following_processing_execution: bool):

    # ロガー取得
    logger = get_run_logger()   # PrefectLogAdapter
    # 初期処理
    init_task_result: PrefectFuture = init_task.submit()

    if init_task_result.get_state().is_completed():
        mongo: MongoModel = init_task_result.result()

        # prefect flow context取得
        any: Any = prefect.context.get_run_context()
        flow_context: FlowRunContext = any

        try:
            # クローラー用引数を生成、クロール対象スパイダーを生成し、クローリングを実行する。
            news_crawl_input: NewsCrawlInput = manual_crawling_input_create_task(spider_kwargs)
            crawling_target_spiders = manual_crawling_target_spiders_task(spider_names)
            crawling_task(news_crawl_input, crawling_target_spiders)

            if following_processing_execution:
                # 後続処理実施指定がある場合、クロール結果のスクレイピングを実施
                scrapying_task(mongo)
                # スクレイピング結果をニュースクリップマスターへ保存
                news_clip_master_save_task(mongo)

        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f'=== {e}')
        finally:
            # 後続の処理を実行する
            end_task(mongo, flow_context.flow_run.name)

    else:
        logger.error(f'=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。')
