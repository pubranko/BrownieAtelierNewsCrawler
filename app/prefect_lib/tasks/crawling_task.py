import logging
from logging import FileHandler, StreamHandler
from typing import Any

from news_crawl.news_crawl_input import NewsCrawlInput
from prefect import get_run_logger, task
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor


@task
def crawling_task(
    news_crawl_input: NewsCrawlInput, crawling_target_spiders: list[dict[str, Any]]
):
    """
    scrapyによるクロールを実行する。
    ・対象のスパイダーを指定できる。
    ・スパイダーの実行時オプションを指定できる。
    ・後続のスクレイピング〜news_clipへの登録まで一括で実行するか選択できる。に存在するかチェック
    """
    logger = get_run_logger()  # PrefectLogAdapter

    scrapy_settings = get_project_settings()  # Scrapyの設定（news_crawl.settings.py）を取得
    runner = CrawlerRunner(settings=scrapy_settings)
    configure_logging(install_root_handler=True)  # Scrapy側でrootロガーへ追加
    scrapy_logger = logging.getLogger(
        "scrapy"
    )  # ここでscrapyのトップロガーのレベルを設定しないとdebugになってしまう。
    scrapy_logger.setLevel(logging.getLevelName(scrapy_settings.get("LOG_LEVEL")))

    for spider_info in crawling_target_spiders:
        runner.crawl(spider_info["class_instans"], **news_crawl_input.__dict__)
    run = runner.join()
    reac: Any = reactor
    run.addBoth(lambda _: reac.stop())
    reac.run(0)

    # Scrapy実行後に、rootロガーに追加されているストリームハンドラ、ファイルハンドラを削除(これをやらないとログが二重化する)
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if type(handler) == StreamHandler:
            root_logger.removeHandler(handler)
        if type(handler) == FileHandler:
            root_logger.removeHandler(handler)
    root_logger = logging.getLogger()

    logger.info(f"=== 不要なのroot logger handlers 削除後の確認:{str(root_logger.handlers)}")
