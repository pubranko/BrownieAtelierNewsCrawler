from typing import Any
from prefect import flow, task, get_run_logger
from datetime import datetime
import prefect
import prefect.context
from prefect.context import FlowRunContext
from prefect.futures import PrefectFuture
from prefect.task_runners import SequentialTaskRunner
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.end_task import end_task
from prefect_lib.run import scrapy_crawling_run, scrapying_run, scraped_news_clip_master_save_run
from prefect_lib.flows import START_TIME
from prefect_lib.flows.common_flow import common_flow
from shared.directory_search_spiders import DirectorySearchSpiders
from news_crawl.news_crawl_input import NewsCrawlInput
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel


@task
def scrapy_crawling_task(spider_names: list[str], spider_kwargs: dict, following_processing_execution: bool, start_time: datetime, mongo: MongoModel):
    '''
    scrapyによるクロールを実行する。
    ・対象のスパイダーを指定できる。
    ・スパイダーの実行時オプションを指定できる。
    ・後続のスクレイピング〜news_clipへの登録まで一括で実行するか選択できる。
    '''
    logger = get_run_logger()   # PrefectLogAdapter
    logger.info(f'=== scrapy_crawling_task 引数 : spider_names={spider_names}, spider_kwargs={spider_kwargs}, following_processing_execution={following_processing_execution}')

    error_spider_names: list = []
    # threads: list[threading.Thread] = []
    directory_search_spiders = DirectorySearchSpiders()

    # spidersディレクトリより取得した一覧に存在するかチェック
    args_spiders_name = set(spider_names)
    for args_spider_name in args_spiders_name:
        # spidersディレクトリより取得した一覧に存在するかチェック
        if not args_spider_name in directory_search_spiders.spiders_name_list_get():
            error_spider_names.append(args_spider_name)
    if len(error_spider_names):
        logger.error(
            f'=== scrapy crwal run : 指定されたspider_nameは存在しませんでした : {error_spider_names}')
        raise ValueError(error_spider_names)

    # spider_kwargsで指定された引数にスタートタイムを追加し、scrapyを実行するための引数へ補正を行う。
    _ = dict(crawling_start_time=start_time)
    _.update(spider_kwargs)
    news_crawl_input = NewsCrawlInput(**_)
    # seleniumの使用有無により分けられた単位でマルチスレッド処理を実行する。
    for separate_spiders_info in directory_search_spiders.spiders_info_list_get(args_spiders_name):
        scrapy_crawling_run.custom_runner_run(
            logger=logger,
            start_time=start_time,
            scrapy_crawling_kwargs=news_crawl_input.__dict__,
            spiders_info=separate_spiders_info)

    if following_processing_execution:
        # 後続のスクレイピング処理を実行
        scrapying_run.exec(
            start_time = start_time,
            mongo = mongo,
            domain = None,
            urls = [],
            target_start_time_from = start_time,
            target_start_time_to = start_time,
        )

        # 後続のスクレイプ結果のニュースクリップマスターへの保存処理を実行
        scraped_news_clip_master_save_run.check_and_save(
            start_time = start_time,
            mongo = mongo,
            domain = None,
            target_start_time_from = start_time,
            target_start_time_to = start_time,
        )

        ### 本格開発までsolrへの連動を一時停止 ###
        # solr_news_clip_save_run.check_and_save(kwargs)


@flow(
    flow_run_name='[CRAWL_003] Scrapy crawling flow',
    task_runner=SequentialTaskRunner())
@common_flow
def scrapy_crawling_flow(spider_names: list[str], spider_kwargs: dict, following_processing_execution: bool):

    # ロガー取得
    logger = get_run_logger()   # PrefectLogAdapter
    # 初期処理
    init_task_result: PrefectFuture = init_task.submit()

    if init_task_result.get_state().is_completed():
        mongo = init_task_result.result()

        # prefect flow context取得
        any: Any = prefect.context.get_run_context()
        flow_context: FlowRunContext = any

        try:
            scrapy_crawling_task(spider_names, spider_kwargs, following_processing_execution, START_TIME, mongo)
        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f'=== {e}')
        finally:
            # 後続の処理を実行する
            end_task(mongo, flow_context.flow_run.name)

    else:
        logger.error(f'=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。')
