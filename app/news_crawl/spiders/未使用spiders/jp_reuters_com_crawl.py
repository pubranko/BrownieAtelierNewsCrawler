import urllib.parse
from collections.abc import Callable
from datetime import datetime
from typing import Any, cast

import scrapy
from news_crawl.spiders.common.start_request_debug_file_generate import LASTMOD as debug_file__LASTMOD
from news_crawl.spiders.common.start_request_debug_file_generate import LOC as debug_file__LOC
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate
from news_crawl.spiders.common.url_pattern_skip_check import url_pattern_skip_check
from news_crawl.spiders.common.urls_continued_skip_check import UrlsContinuedSkipCheck
from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
from scrapy.http import TextResponse
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

"""
このソースは現在未使用。
"""


class JpReutersComCrawlSpider(ExtensionsCrawlSpider):
    name: str = "jp_reuters_com_crawl"
    allowed_domains: list = ["jp.reuters.com"]
    start_urls: list = [
        # 'https://jp.reuters.com/news/archive?view=page&page=1&pageSize=10'  # 最新ニュース
        # 'https://jp.reuters.com/news/archive?view=page&page=2&pageSize=10' #2ページ目
    ]
    _domain_name: str = "jp_reuters_com"  # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    custom_settings: dict = {
        "DEPTH_LIMIT": 0,
        "DEPTH_STATS_VERBOSE": True,
        "DOWNLOADER_MIDDLEWARES": {
            "news_crawl.scrapy_selenium_custom_middlewares.SeleniumMiddleware": 585,
        },
    }

    # _crawl_point: dict = {}
    # '''次回クロールポイント情報 (ExtensionsCrawlSpiderの同項目をオーバーライド必須)'''

    # rules = (
    #     Rule(LinkExtractor(
    #         allow=(r'/article/')), callback='parse_news'),
    # )
    # seleniumモード
    selenium_mode: bool = True
    def __init__(self, *args, **kwargs):
        """(拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        """
        super().__init__(*args, **kwargs)

        # クロールする対象ページを決定する。デフォルト１〜３。scrapy起動引数に指定がある場合、そちらを使う。
        self.page_from, self.page_to = self.pages_setting(1, 3)
        self.page: int = self.page_from
        self.all_urls_list: list = []
        self.session_id: str = self.name + datetime.now().isoformat()

        # 開始ページからURLを生成
        url = f"https://jp.reuters.com/news/archive?view=page&page={self.page_from}&pageSize=10"
        self.start_urls.append(url)
        # 開始URLからクエリ文字列を除去し、ベースURLを取り出す。
        _ = url.split("?")[0]
        # keyにドット(.)があるとエラーMongoDBがエラーとなるためアンダースコアに置き換え
        self.base_url = _.replace(".", "_")

        self.url_continued = UrlsContinuedSkipCheck(self._crawl_point, self.base_url, self.news_crawl_input.continued)

    def start_requests(self):
        """ """
        if self.selenium_mode:
            for url in self.start_urls:
                yield SeleniumRequest(url=url, callback=self.parse_start_response_selenium)

    def parse_start_response_selenium(self, response: TextResponse):
        """(拡張メソッド)
        取得したレスポンスよりDBへ書き込み(selenium版)
        """
        r: Any = response.request
        driver: WebDriver = r.meta["driver"]

        while self.page <= self.page_to:
            self.logger.info(f"=== parse_start_response 現在解析中のURL = {driver.current_url}")
            driver.set_page_load_timeout(60)
            driver.set_script_timeout(60)

            next_page_element = (
                f'div.control-nav > a.control-nav-next[href="?view=page&page={self.page + 1}&pageSize=10"]'
            )
            WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.CSS_SELECTOR, next_page_element)))

            # ページ内の対象urlを抽出
            _ = driver.find_elements(By.CSS_SELECTOR, ".story-content a[href]")
            # _ = driver.find_elements_by_css_selector('.story-content a[href]')
            links: list = [link.get_attribute("href") for link in _]
            self.logger.info(f"=== ページ内の記事件数 = {len(links)}")
            # ページ内記事は通常10件。それ以外の場合はワーニングメール通知（環境によって違うかも、、、）
            if not len(links) == 10:
                self.logger.warning(
                    "=== parse_start_response "
                    f"1ページ内で取得できた件数が想定の10件と異なる。確認要。 ( {len(links)} 件)"
                )

            for link in links:
                # 相対パスの場合絶対パスへ変換。また%エスケープされたものはUTF-8へ変換
                url: str = urllib.parse.unquote(response.urljoin(link))
                self.all_urls_list.append({debug_file__LOC: url, debug_file__LASTMOD: ""})
                # 前回からの続きの指定がある場合、
                # 前回取得したurlが確認できたら確認済み（削除）にする。

                if self.url_continued.skip_check(url):
                    pass
                elif url_pattern_skip_check(url, self.news_crawl_input.url_pattern):
                    pass
                else:
                    # クロール対象のURL情報を保存
                    self.crawl_urls_list.append(
                        {
                            self.CRAWL_URLS_LIST__LOC: url,
                            self.CRAWL_URLS_LIST__LASTMOD: "",
                            self.CRAWL_URLS_LIST__SOURCE_URL: driver.current_url,
                        }
                    )
                    self.crawl_target_urls.append(url)

            # debug指定がある場合、現ページの１０件をデバック用ファイルに保存
            start_request_debug_file_generate(
                self.name,
                driver.current_url,
                self.all_urls_list[-10:],
                self.news_crawl_input.debug,
            )

            # 前回の5件のURLをすべて確認したら、前回以降の記事は取得済みとする。
            if self.url_continued.skip_flg:
                self.logger.info(
                    f"=== parse_start_response 前回の続きまで再取得完了 ({driver.current_url})",
                )
                self.page = self.page_to + 1
                break

            # 次のページを読み込む
            self.page += 1
            elem: WebElement = driver.find_element(By.CSS_SELECTOR, "div.control-nav > a.control-nav-next")
            # elem: WebElement = driver.find_element_by_css_selector(
            #     'div.control-nav > a.control-nav-next')
            elem.click()

        # リスト(self.urls_list)に溜めたurlをリクエストへ登録する。
        for _ in self.crawl_urls_list:
            yield scrapy.Request(
                response.urljoin(_[self.CRAWL_POINT__LOC]),
                callback=cast(Callable, self.parse_news),
            )
        # 次回向けに1ページ目の5件をcontrollerへ保存する
        self._crawl_point[self.base_url] = {
            self.CRAWL_POINT__URLS: self.all_urls_list[0 : self.url_continued.check_count],
            self.CRAWL_POINT__CRAWLING_START_TIME: self.news_crawl_input.crawling_start_time,
        }
