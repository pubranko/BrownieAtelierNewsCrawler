import logging
from logging import StreamHandler
from typing import Any
from prefect import task, get_run_logger
from news_crawl.news_crawl_input import NewsCrawlInput
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from twisted.internet import reactor
from scrapy.utils.project import get_project_settings


@task
def crawling_task(news_crawl_input: NewsCrawlInput, crawling_target_spiders: list[dict[str, Any]]):
    '''
    scrapyによるクロールを実行する。
    ・対象のスパイダーを指定できる。
    ・スパイダーの実行時オプションを指定できる。
    ・後続のスクレイピング〜news_clipへの登録まで一括で実行するか選択できる。に存在するかチェック
    '''
    logger = get_run_logger()   # PrefectLogAdapter

    runner = CrawlerRunner(settings=get_project_settings())
    configure_logging(install_root_handler=True)   # Scrapy側でrootロガーへ追加

    for spider_info in crawling_target_spiders:
        runner.crawl(spider_info['class_instans'], **news_crawl_input.__dict__)
    run = runner.join()
    reac:Any = reactor
    run.addBoth(lambda _: reac.stop())
    reac.run(0)

    # Scrapy実行後に、rootロガーに追加されているストリームハンドラを削除(これをやらないとログが二重化する)
    # root_logger = logging.getLogger()
    # for handler in root_logger.handlers:
    #     if type(handler) == StreamHandler:
    #         root_logger.removeHandler(handler)
    # root_logger = logging.getLogger()

    # logger.info('=== 不要なのroot logger handlers 削除後の確認:' +
    #             str(root_logger.handlers))
