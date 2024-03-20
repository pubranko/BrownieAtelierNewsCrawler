from prefect import task, get_run_logger
from news_crawl.news_crawl_input import NewsCrawlInput
from prefect_lib.flows import START_TIME


@task
def crawling_input_create_task(spider_kwargs: dict) -> NewsCrawlInput:
    """
    scrapyによるクロールを実行するための引数チェック&生成を行う。
    """
    logger = get_run_logger()  # PrefectLogAdapter
    logger.info(f"=== 引数 : spider_kwargs={spider_kwargs}")

    # spider_kwargsで指定された引数にスタートタイムを追加し、scrapyを実行するための引数へ補正を行う。
    _ = dict(crawling_start_time=START_TIME)
    _.update(spider_kwargs)

    return NewsCrawlInput(**_)
