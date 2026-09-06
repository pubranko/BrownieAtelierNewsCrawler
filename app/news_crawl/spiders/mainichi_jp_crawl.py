import urllib.parse
from collections.abc import AsyncIterator, Callable
from typing import Any, cast

import scrapy
from dateutil import parser
from news_crawl.spiders.common.start_request_debug_file_generate import LASTMOD as debug_file__LASTMOD
from news_crawl.spiders.common.start_request_debug_file_generate import LOC as debug_file__LOC
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate
from news_crawl.spiders.common.url_pattern_skip_check import url_pattern_skip_check
from news_crawl.spiders.common.urls_continued_skip_check import UrlsContinuedSkipCheck
from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
from playwright.async_api import Page
from scrapy.http import TextResponse

base_start_url = "https://mainichi.jp/flash/"


class MainichiJpCrawlSpider(ExtensionsCrawlSpider):
    name = "mainichi_jp_crawl"
    allowed_domains = ["mainichi.jp"]
    start_urls = [base_start_url]
    _domain_name = "mainichi_jp"
    _spider_version = 1.0
    custom_settings: dict[str, Any] | None = {"DEPTH_LIMIT": 0, "DEPTH_STATS_VERBOSE": True}
    _crawl_point: dict = {}
    playwright_mode__start_request = True
    playwright_include_page = True

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.page_from, self.page_to = self.pages_setting(1, 3)
        self.all_urls_list: list[dict[str, Any]] = []
        self.url_continued = UrlsContinuedSkipCheck(self._crawl_point, base_start_url, self.news_crawl_input.continued)

    async def _extract(self, page: Page) -> list[dict[str, Any]]:
        links = await page.locator("#article-list > ul > li > a[href]").evaluate_all("els => els.map(e => e.href)")
        lastmods = await page.locator(
            "#article-list > ul > li > a > div > div.articlelist-detail > div > span.articletag-date"
        ).all_inner_texts()
        return [
            {"link": link, "lastmod": parser.parse(lastmod)}
            for link, lastmod in zip(links, lastmods, strict=False)
        ]

    async def _load_until(self, page: Page, item_count: int) -> None:
        await page.locator(f"#article-list > ul > li:nth-child({item_count})").wait_for(
            state="attached", timeout=60_000
        )
        await page.locator("div.main-contents span.link-more").wait_for(state="visible", timeout=60_000)

    async def parse_start_response_continued_crawl_mode(self, response: TextResponse) -> AsyncIterator[scrapy.Request]:
        async for request in self._parse_listing(response, continued=True):
            yield request

    async def parse_start_response_page_crawl_mode(self, response: TextResponse) -> AsyncIterator[scrapy.Request]:
        async for request in self._parse_listing(response, continued=False):
            yield request

    async def _parse_listing(self, response: TextResponse, *, continued: bool) -> AsyncIterator[scrapy.Request]:
        page: Page = response.meta["playwright_page"]
        try:
            max_page = 1 if continued else self.page_to
            for page_number in range(1, max_page + 1):
                await self._load_until(page, 20 * page_number)
                if page_number < max_page:
                    await page.locator("div.main-contents span.link-more").click()
            extracts = await self._extract(page)
            start = 0 if continued else 20 * (self.page_from - 1)
            end = None if continued else 20 * self.page_to
            for extract in extracts[start:end]:
                url = urllib.parse.unquote(response.urljoin(extract["link"]))
                self.all_urls_list.append({debug_file__LOC: url, debug_file__LASTMOD: extract["lastmod"]})
                if url_pattern_skip_check(url, self.news_crawl_input.url_pattern):
                    continue
                if continued and self.url_continued.skip_check(url):
                    continue
                self.crawl_urls_list.append({
                    self.CRAWL_URLS_LIST__LOC: url,
                    self.CRAWL_URLS_LIST__LASTMOD: extract["lastmod"],
                    self.CRAWL_URLS_LIST__SOURCE_URL: page.url,
                })
                self.crawl_target_urls.append(url)
                yield scrapy.Request(url, callback=cast(Callable, self.parse_news))
            self._crawl_point[base_start_url] = {
                self.CRAWL_POINT__URLS: self.all_urls_list[: self.url_continued.check_count],
                self.CRAWL_POINT__CRAWLING_START_TIME: self.news_crawl_input.crawling_start_time,
            }
            start_request_debug_file_generate(self.name, page.url, self.all_urls_list, self.news_crawl_input.debug)
        finally:
            await page.close()
