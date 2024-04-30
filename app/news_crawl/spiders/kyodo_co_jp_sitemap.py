from news_crawl.spiders.extensions_class.extensions_sitemap import \
    ExtensionsSitemapSpider


class KyodoCoJpSitemapSpider(ExtensionsSitemapSpider):
    name = "kyodo_co_jp_sitemap"
    allowed_domains = ["kyodo.co.jp"]
    sitemap_urls: list = ["https://www.kyodo.co.jp/sitemap.xml"]
    _domain_name: str = "kyodo_co_jp"  # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    # https://www.kyodo.co.jp/post-sitemap.xml	https://www.kyodo.co.jp/post-sitemap2.xml
    sitemap_follow = ["/post-sitemap"]
