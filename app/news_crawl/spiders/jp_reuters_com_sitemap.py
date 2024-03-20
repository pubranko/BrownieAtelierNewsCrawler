import sys
from datetime import timedelta

from news_crawl.spiders.common.term_days_Calculation import \
    term_days_Calculation
from news_crawl.spiders.extensions_class.extensions_sitemap import \
    ExtensionsSitemapSpider
from scrapy.exceptions import CloseSpider


class JpReutersComSitemapSpider(ExtensionsSitemapSpider):
    name: str = "jp_reuters_com_sitemap"
    allowed_domains = ["jp.reuters.com"]
    sitemap_urls: list = [
        # 'https://jp.reuters.com/arc/outboundfeeds/news-sitemap-index/?outputType=xml',
        "https://jp.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml",
        "https://jp.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml&from=100",
        "https://jp.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml&from=200",
    ]
    _domain_name: str = "jp_reuters_com"  # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0
