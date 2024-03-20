import scrapy
from news_crawl.spiders.common.custom_sitemap import CustomSitemap
from news_crawl.spiders.extensions_class.extensions_sitemap import \
    ExtensionsSitemapSpider
from scrapy.http import Request, Response, TextResponse
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
from scrapy.spiders.sitemap import iterloc
from scrapy.utils.sitemap import sitemap_urls_from_robots


class SankeiComSitemapSpider(ExtensionsSitemapSpider):
    name: str = "sankei_com_sitemap"
    allowed_domains: list = ["sankei.com"]
    sitemap_urls: list = [
        "https://www.sankei.com/feeds/sitemap/?outputType=xml&from=0",
        # 'https://www.sankei.com/robots.txt',
        # 'https://feed.etf.sankei.com/global/sitemap',
        # 'https://www.sankei.com/feeds/google-sitemapindex/?outputType=xml',
    ]
    _domain_name: str = "sankei_com"  # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    # sitemap_follow = [
    #     '/feeds/google-sitemapindex/',
    #     '/feeds/google-sitemap/',
    #     ]

    # splash_mode: bool = True
    # selenium_mode: bool = True
    # sitemap_rules = [(r'.*', 'selenium_parse')]
    # custom_settings = {
    #     'DOWNLOADER_MIDDLEWARES' : {
    #         'news_crawl.scrapy_selenium_custom_middlewares.SeleniumMiddleware': 585,
    #     }
    # }

    sitemap_type = (
        ExtensionsSitemapSpider.SITEMAP_TYPE__GOOGLE_NEWS_SITEMAP
    )  # googleのニュースサイトマップ用にカスタマイズしたタイプ

    known_pagination_css_selectors: list[str] = [
        ".pagination  a[href]",
    ]

    loading_site_map_continued_mode = True

    def loading_site_map_continued(self, current_sitemap_url: str) -> str:
        """
        現在のサイトマップURLより次のサイトマップURLを生成し返す。存在しない場合空文字を返す。
        例) https://www.sankei.com/feeds/sitemap/?outputType=xml&from=0  →  https://www.sankei.com/feeds/sitemap/?outputType=xml&from=100
        """
        # サイトマップURLのベース部分
        sitemap_index_base = (
            "https://www.sankei.com/feeds/sitemap/?outputType=xml&from="
        )
        value = int(current_sitemap_url.replace(sitemap_index_base, ""))
        value = value + 100

        next_sitemap_url: str = ""
        if value <= 9900:  # 存在する最後のサイトパップ以下である場合
            next_sitemap_url = f"{sitemap_index_base}{value}"

        return next_sitemap_url
