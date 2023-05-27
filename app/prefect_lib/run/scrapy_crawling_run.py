import os
import sys
import logging
from logging import Logger, StreamHandler ,LoggerAdapter
from datetime import datetime
from typing import Any, Union
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor
path = os.getcwd()
sys.path.append(path)
import time
#from scrapy.utils.reactor import install_reactor
#install_reactor('twisted.internet.asyncioreactor.AsyncioSelectorReactor')

def scrapy_deco(func):
    def deco(*args, **kwargs):
        ### 初期処理 ###
        logger: Union[Logger,LoggerAdapter] = kwargs['logger']
        ### 主処理 ###
        func(*args, **kwargs)
        ### 終了処理 ###
        # Scrapy実行後に、rootロガーに追加されているストリームハンドラを削除(これをやらないとログが二重化する)
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            if type(handler) == StreamHandler:
                root_logger.removeHandler(handler)
        root_logger = logging.getLogger()
        logger.info('=== 不要なのroot logger handlers 削除後の確認:' +
                    str(root_logger.handlers))
    return deco

@scrapy_deco
def custom_crawl_run(logger: Union[Logger,LoggerAdapter], start_time: datetime, scrapy_crawling_kwargs: dict, spiders_info: list[dict[str, Any]]):
    '''
    '''
    process = CrawlerProcess(settings=get_project_settings())
    configure_logging(install_root_handler=False)   # Scrapy側でrootロガーへ追加しない

    for spider_info in spiders_info:
        process.crawl(spider_info['class_instans'], **scrapy_crawling_kwargs)
    process.start()
    #process.start(stop_after_crawl=False)

@scrapy_deco
def custom_runner_run(logger: Union[Logger,LoggerAdapter], start_time: datetime, scrapy_crawling_kwargs: dict, spiders_info: list[dict[str, Any]]):
    '''
    検討したがcustom_crawl_runを使用することにした。
    とりあえずサンプルとしてソースは残している。
    '''
    runner = CrawlerRunner(settings=get_project_settings())
    configure_logging(install_root_handler=True)   # Scrapy側でrootロガーへ追加

    for spider_info in spiders_info:
        runner.crawl(spider_info['class_instans'], **scrapy_crawling_kwargs)
    run = runner.join()
    reac:Any = reactor
    run.addBoth(lambda _: reac.stop())
    reac.run(0)
    # run.send()

