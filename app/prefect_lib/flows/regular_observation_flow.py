from typing import Any
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect.task_runners import SequentialTaskRunner
from prefect_lib.flows import START_TIME
from prefect_lib.flows.init_flow import init_flow
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.end_task import end_task
from prefect_lib.tasks.crawling_input_create_task import crawling_input_create_task
from prefect_lib.tasks.regular_observation_task import regular_observation_task
from prefect_lib.tasks.crawling_task import crawling_task
from prefect_lib.tasks.scrapying_task import scrapying_task
from prefect_lib.tasks.news_clip_master_save_task import news_clip_master_save_task
from news_crawl.news_crawl_input import NewsCrawlInput
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel


@flow(
    name='Regular observation flow',
    task_runner=SequentialTaskRunner())
def regular_observation_flow():
    init_flow()

    # ロガー取得
    logger = get_run_logger()   # PrefectLogAdapter
    # 初期処理
    init_task_result: PrefectFuture = init_task.submit()

    if init_task_result.get_state().is_completed():
        mongo: MongoModel = init_task_result.result()

        try:
            # クローラー用引数を生成、クロール対象スパイダーを生成し、クローリングを実行する。
            news_crawl_input: NewsCrawlInput = crawling_input_create_task(dict(
                crawling_start_time = START_TIME,
                continued = True))
            crawling_target_spiders = regular_observation_task(mongo)
            if len(crawling_target_spiders):
                crawling_task(news_crawl_input, crawling_target_spiders)

                # クロール結果のスクレイピングを実施
                scrapying_task(mongo, '', [], START_TIME, START_TIME)
                # スクレイピング結果をニュースクリップマスターへ保存
                news_clip_master_save_task(mongo,'', START_TIME, START_TIME)

        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f'=== {e}')
        finally:
            # 後続の処理を実行する
            end_task(mongo)

    else:
        logger.error(f'=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。')

