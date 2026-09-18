from __future__ import annotations  # ExtensionsSitemapSpiderの循環参照を回避するため

from typing import TYPE_CHECKING, Any

from BrownieAtelierMongo.collection_models.controller_model import ControllerModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import CrawlerLogsModel
from scrapy.statscollectors import MemoryStatsCollector
from shared.resource_check import resource_check

if TYPE_CHECKING:  # 型チェック時のみインポート
    from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
    from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider

    # from news_crawl.spiders.extensions_class.extensions_xml_feed import ExtensionsXmlFeedSpider


def spider_closed(
    spider: ExtensionsSitemapSpider | ExtensionsCrawlSpider,
):
    """spider共通の終了処理"""
    try:
        _save_crawl_results(spider)
    finally:
        # 保存に失敗した場合もDB接続・排他ロックを解放する。
        try:
            spider.mongo.close()
        finally:
            control = getattr(spider, "_crawling_domain_control", None)
            if control is not None:
                control.lock.release()


def _save_crawl_results(spider: ExtensionsSitemapSpider | ExtensionsCrawlSpider):
    any: Any = spider.crawler.stats
    stats: MemoryStatsCollector = any

    if spider.news_crawl_input.crawl_point_non_update:
        spider.logger.info("=== closed : 次回クロールポイント情報の更新Skip")
    else:
        controller = ControllerModel(spider.mongo)
        # 発見した最新位置をそのまま保存せず、記事・分割ページの保存結果から安全な位置を求める。
        # 一部失敗ならその手前まで進め、一覧自体が未解析などで判断できなければ前回位置を維持する。
        safe_point = spider._crawl_progress.safe_point(spider)
        if safe_point != spider._crawl_point:
            spider.logger.warning("未取得・未保存のページがあるため、安全に完了した地点まで保存します。")
        controller.crawl_point_update(spider._domain_name, spider.name, safe_point)
        spider.logger.info(f"=== closed : controllerに次回クロールポイント情報を保存 \n {safe_point}")

    resource_check(spider.logger)

    # クロールの統計結果とクロールを行ったサイトの一覧情報を「spider_report」としてログに保存する。
    crawler_logs = CrawlerLogsModel(spider.mongo)

    crawler_logs.spider_report_insert(
        spider.news_crawl_input.crawling_start_time,
        spider.allowed_domains[0],
        spider.name,
        stats,
        spider.crawl_urls_list,
    )

    spider.logger.info(f"=== Spider closed: {spider.name}")
