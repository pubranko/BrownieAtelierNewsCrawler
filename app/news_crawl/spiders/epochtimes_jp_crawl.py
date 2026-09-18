import urllib.parse
from collections.abc import Callable, Iterable
from typing import Any, Final, cast

import scrapy
from news_crawl.spiders.common.start_request_debug_file_generate import LASTMOD as debug_file__LASTMOD
from news_crawl.spiders.common.start_request_debug_file_generate import LOC as debug_file__LOC
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate
from news_crawl.spiders.common.url_pattern_skip_check import url_pattern_skip_check
from news_crawl.spiders.common.urls_continued_skip_check import UrlsContinuedSkipCheck
from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
from scrapy.http import TextResponse

base_start_url: str = "https://www.epochtimes.jp/latest"


class EpochtimesJpCrawlSpider(ExtensionsCrawlSpider):
    name: str = "epochtimes_jp_crawl"
    allowed_domains: list = ["epochtimes.jp"]
    start_urls: list = [base_start_url]
    _domain_name: str = "epochtimes_jp"
    _spider_version: float = 1.0
    custom_settings: dict[str, Any] | None = {"DEPTH_LIMIT": 0, "DEPTH_STATS_VERBOSE": True}
    _crawl_point: dict = {}
    playwright_mode__start_request: bool = True
    ITEMS_ON_PAGE_COUNT: Final[int] = 30

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.page_from, self.page_to = self.pages_setting(1, 3)
        self.page = self.page_from
        self.all_urls_list: list[dict[str, str]] = []
        self.url_continued = UrlsContinuedSkipCheck(
            self._crawl_point, self.start_urls[0], self.news_crawl_input.continued
        )
        if not self.url_continued.continued:
            self.start_urls = [f"{base_start_url}/{page}" for page in range(self.page_from, self.page_to + 1)]

    def parse_start_response_continued_crawl_mode(self, response: TextResponse) -> Iterable[scrapy.Request]:
        yield from self._parse_listing(response, continued=True)

    def parse_start_response_page_crawl_mode(self, response: TextResponse) -> Iterable[scrapy.Request]:
        yield from self._parse_listing(response, continued=False)

    def _parse_listing(self, response: TextResponse, *, continued: bool) -> Iterable[scrapy.Request]:
        self.logger.info("=== parse_start_response 現在解析中のURL = %s", response.url)
        links = response.css(".main_content > .left_col > .posts_list .post_title > a[href]::attr(href)").getall()
        original_url = response.meta.get("progress_url", response.url)
        page_segment = urllib.parse.urlparse(original_url).path.rstrip("/").rsplit("/", 1)[-1]
        page_number = self.page if continued else (int(page_segment) if page_segment.isdigit() else self.page_from)
        # スキップ判定前の一覧をページ番号付きで保持し、途中失敗時の再開用 URL 群の選択に使う。
        self._crawl_progress.record_listing(
            base_start_url,
            page_number,
            [{"loc": urllib.parse.unquote(response.urljoin(link)), "lastmod": ""} for link in links],
        )
        self.logger.info("=== ページ内の記事件数 = %s", len(links))
        if len(links) != self.ITEMS_ON_PAGE_COUNT:
            self.logger.warning("=== 1ページ内で取得できた件数が想定の30件と異なる。確認要。 (%s 件)", len(links))

        for link in links:
            url = urllib.parse.unquote(response.urljoin(link))
            self.all_urls_list.append({debug_file__LOC: url, debug_file__LASTMOD: ""})
            if url_pattern_skip_check(url, self.news_crawl_input.url_pattern):
                continue
            if continued and self.url_continued.skip_check(url):
                continue
            self.crawl_urls_list.append(
                {
                    self.CRAWL_URLS_LIST__LOC: url,
                    self.CRAWL_URLS_LIST__LASTMOD: "",
                    self.CRAWL_URLS_LIST__SOURCE_URL: response.url,
                }
            )
            if not continued:
                yield scrapy.Request(url, callback=cast(Callable, self.parse_news))

        start_request_debug_file_generate(
            self.name, response.url, self.all_urls_list[-self.ITEMS_ON_PAGE_COUNT :], self.news_crawl_input.debug
        )
        if continued and not self.url_continued.skip_flg:
            if not links or self.page >= self.settings.getint("CONTINUED_MAX_LISTING_PAGES", 100):
                raise RuntimeError("前回のクロールポイントに到達できませんでした。再開位置を維持します。")
            self.page += 1
            yield scrapy.Request(
                f"{self.start_urls[0]}/{self.page}",
                callback=cast(Callable, self.parse_start_response_continued_crawl_mode),
                meta={
                    "playwright": True,
                    "playwright_page_goto_kwargs": {"wait_until": "domcontentloaded", "timeout": 60_000},
                },
            )
            return

        for crawl_url in self.crawl_urls_list:
            if continued:
                yield scrapy.Request(crawl_url[self.CRAWL_POINT__LOC], callback=cast(Callable, self.parse_news))
        self._crawl_point[base_start_url] = {
            self.CRAWL_POINT__URLS: self.all_urls_list[: self.url_continued.check_count],
            self.CRAWL_POINT__CRAWLING_START_TIME: self.news_crawl_input.crawling_start_time,
        }
