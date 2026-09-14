"""実サイト・MongoDBには接続せず、送信制御と取得漏れ防止を検証する。"""

import asyncio
import copy
import os
import runpy
import unittest
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from time import monotonic
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from news_crawl.adaptive_throttle import (
    AdaptiveThrottle,
    RateLimitRetryMiddleware,
    _send_rate_limit_notice,
    retry_after_seconds,
)
from news_crawl.spiders.common.crawl_progress import CrawlProgress
from scrapy import Request, Spider
from scrapy.core.downloader import Slot
from scrapy.crawler import AsyncCrawlerRunner
from scrapy.http import HtmlResponse, Response

TEST_MONGO_ENV = {
    f"BROWNIE_ATELIER_MONGO__MONGO_{key}": "test"
    for key in ("SERVER", "PORT", "USE_DB", "USER", "PASS", "TLS_CA_FILE", "TLS_CERTTIFICATE_KEY_FILE")
}


def article(index):
    return f"https://example.test/{index}"


def stamp(minute):
    return datetime(2026, 9, 12, 10, minute, tzinfo=UTC)


class ProgressTests(unittest.TestCase):
    def sitemap(self, minutes=(0, 5, 10)):
        previous = {"latest_lastmod": stamp(0) - timedelta(minutes=5)}
        tracker = CrawlProgress(previous)
        rows = [{"loc": article(i), "lastmod": stamp(minute)} for i, minute in enumerate(minutes)]
        spider = SimpleNamespace(crawl_urls_list=rows, _crawl_point={"latest_lastmod": stamp(max(minutes))})
        for row in rows:
            tracker.scheduled_request(Request(row["loc"]))
        return tracker, spider

    def test_partial_lastmod_and_full_recovery(self):
        tracker, spider = self.sitemap()
        tracker.saved.update([article(0), article(2)])
        self.assertEqual(tracker.safe_point(spider)["latest_lastmod"], stamp(0))
        tracker.saved.add(article(1))
        self.assertEqual(tracker.safe_point(spider)["latest_lastmod"], stamp(10))

    def test_same_timestamp_is_one_group(self):
        tracker, spider = self.sitemap((0, 5, 5, 10))
        tracker.saved.update([article(0), article(1), article(3)])
        self.assertEqual(tracker.safe_point(spider)["latest_lastmod"], stamp(0))

    def test_unknown_sitemap_does_not_advance(self):
        tracker, spider = self.sitemap()
        tracker.saved.update(row["loc"] for row in spider.crawl_urls_list)
        tracker.scheduled_request(Request("https://example.test/missing.xml"))
        self.assertEqual(tracker.safe_point(spider), tracker.previous)

    def test_start_interrupted_before_all_sitemaps_are_scheduled(self):
        tracker, spider = self.sitemap()
        tracker.saved.update(row["loc"] for row in spider.crawl_urls_list)
        tracker.expect_discovery(["https://example.test/not-scheduled.xml"])
        self.assertEqual(tracker.safe_point(spider), tracker.previous)

    def test_listing_page_retries_do_not_change_chronological_order(self):
        rows = [{"loc": article(i), "lastmod": ""} for i in range(30)]
        tracker = CrawlProgress({"listing": {"urls": rows[20:]}})
        tracker.record_listing("listing", 2, rows[15:])
        tracker.record_listing("listing", 1, rows[:15])
        tracker.saved.update(row["loc"] for row in rows if row["loc"] != article(9))
        spider = SimpleNamespace(
            crawl_urls_list=rows,
            all_urls_list=rows[15:] + rows[:15],
            url_continued=SimpleNamespace(check_count=10),
            _crawl_point={"listing": {"urls": rows[15:25]}},
        )
        self.assertEqual(tracker.safe_point(spider)["listing"]["urls"], rows[10:20])
        tracker.saved.add(article(9))
        self.assertEqual(tracker.safe_point(spider)["listing"]["urls"], rows[:10])

    def test_parse_exception_even_after_item_saved(self):
        tracker, spider = self.sitemap()
        tracker.saved.update(row["loc"] for row in spider.crawl_urls_list)
        tracker.failed.add(article(1))
        self.assertEqual(tracker.safe_point(spider)["latest_lastmod"], stamp(0))

    def test_unsaved_pagination_blocks_its_article(self):
        tracker, spider = self.sitemap()
        tracker.saved.update(row["loc"] for row in spider.crawl_urls_list)
        tracker.scheduled_request(Request(article("page2"), meta={"checkpoint_root": article(1)}))
        self.assertEqual(tracker.safe_point(spider)["latest_lastmod"], stamp(0))

    def test_lastmod_from_mongo_without_timezone(self):
        tracker, spider = self.sitemap()
        tracker.previous["latest_lastmod"] = stamp(0).replace(tzinfo=None)
        tracker.saved.update([article(0), article(2)])
        self.assertEqual(tracker.safe_point(spider), tracker.previous)

    def test_url_anchors_are_older_than_oldest_failure(self):
        rows = [{"loc": article(i), "lastmod": ""} for i in range(30)]
        previous = {"listing": {"urls": rows[20:]}}
        tracker = CrawlProgress(previous)
        tracker.saved.update(row["loc"] for row in rows if row["loc"] not in (article(2), article(9)))
        spider = SimpleNamespace(
            crawl_urls_list=rows,
            all_urls_list=rows,
            url_continued=SimpleNamespace(check_count=10),
            _crawl_point={"listing": {"urls": rows[:10]}},
        )
        self.assertEqual(tracker.safe_point(spider)["listing"]["urls"], rows[10:20])
        tracker.saved.remove(article(25))
        self.assertEqual(tracker.safe_point(spider), previous)

    def test_redirect_save_counts_for_original_url(self):
        tracker, spider = self.sitemap()
        tracker.saved.update([article(0), article(2)])
        response = HtmlResponse(
            article("redirected"), request=Request(article("redirected"), meta={"progress_url": article(1)})
        )
        tracker.saved_response(response)
        self.assertEqual(tracker.safe_point(spider)["latest_lastmod"], stamp(10))


class HeaderTests(unittest.TestCase):
    def test_notice_uses_existing_sender_and_normal_channel(self):
        sender = Mock()
        settings = SimpleNamespace(BROWNIE_ATELIER_NOTICE__SLACK_CHANNEL_ID__NOMAL="test-normal")
        with patch.dict("sys.modules", {
            "BrownieAtelierNotice": SimpleNamespace(settings=settings),
            "BrownieAtelierNotice.slack.slack_notice": SimpleNamespace(slack_notice=sender),
        }):
            _send_rate_limit_notice("test message")
        self.assertEqual(sender.call_args.kwargs["channel_id"], "test-normal")
        self.assertEqual(sender.call_args.kwargs["message"], "test message")

    def test_retry_after(self):
        now = stamp(0)
        self.assertEqual(retry_after_seconds(b"120", now), 120)
        self.assertEqual(retry_after_seconds(format_datetime(now + timedelta(seconds=180)).encode(), now), 180)
        for value in (None, b"invalid", b"-10", b"inf", b"\xff"):
            self.assertIsNone(retry_after_seconds(value, now))

    def test_debug_follows_log_level(self):
        for level, expected in (("DEBUG", True), ("debug", True), ("INFO", False), ("WARNING", False)):
            with patch.dict(os.environ, {"SCRAPY__LOG_LEVEL": level}):
                settings = runpy.run_module("news_crawl.settings")
                self.assertEqual(settings["AUTOTHROTTLE_DEBUG"], expected)

    def test_controller_updates_are_isolated_by_document_type(self):
        # 設定値は import のためだけに用意。MongoModel の生成・接続は行わない。
        keys = ("SERVER", "PORT", "USE_DB", "USER", "PASS", "TLS_CA_FILE", "TLS_CERTTIFICATE_KEY_FILE")
        with patch.dict(os.environ, {f"BROWNIE_ATELIER_MONGO__MONGO_{key}": "test" for key in keys}):
            from BrownieAtelierMongo.collection_models.controller_model import ControllerModel
        model = object.__new__(ControllerModel)
        model.update_one = Mock()
        model.crawl_point_update("domain", "spider", {"latest_lastmod": stamp(0)})
        model.update_one.assert_called_with(
            {"domain": "domain", "document_type": "crawl_point"},
            {"$set": {"spider": {"latest_lastmod": stamp(0)}}},
        )
        model.download_control_update("domain", {"download_delay": 6})
        model.update_one.assert_called_with(
            {"domain": "domain", "document_type": "download_control"}, {"$set": {"download_delay": 6}}
        )

    @patch.dict(os.environ, TEST_MONGO_ENV)
    def test_test_flag_and_cleanup_when_checkpoint_save_fails(self):
        from news_crawl.spiders.common import spider_closed as module

        spider = SimpleNamespace(
            news_crawl_input=SimpleNamespace(crawl_point_non_update=True, crawling_start_time=stamp(0)),
            mongo=Mock(),
            _crawling_domain_control=Mock(),
            logger=Mock(),
            crawler=SimpleNamespace(stats=Mock()),
            allowed_domains=["example.test"],
            name="test",
            _domain_name="example_test",
            crawl_urls_list=[],
            _crawl_progress=Mock(),
            _crawl_point={},
        )
        with (
            patch.object(module, "ControllerModel") as controller,
            patch.object(module, "CrawlerLogsModel"),
            patch.object(module, "resource_check"),
        ):
            module.spider_closed(spider)
            controller.assert_not_called()
            spider.mongo.close.assert_called_once()
            spider._crawling_domain_control.lock.release.assert_called_once()
            spider.news_crawl_input.crawl_point_non_update = False
            controller.return_value.crawl_point_update.side_effect = RuntimeError("simulated save failure")
            with self.assertRaises(RuntimeError):
                module.spider_closed(spider)
            self.assertEqual(spider._crawling_domain_control.lock.release.call_count, 2)


class MemoryController:
    def __init__(self, state=None):
        self.state = state or {}
        self.history = []

    def download_control_get(self, domain):
        return copy.deepcopy(self.state)

    def download_control_update(self, domain, state):
        self.state = copy.deepcopy(state)
        self.history.append(copy.deepcopy(state))


class FakeHandler:
    """ダウンロードハンドラーまで到達した時刻を計測し、HTTP応答を模擬する。"""

    lazy = True

    def __init__(self, crawler):
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    async def download_request(self, request):
        spider = self.crawler.spider
        spider.sent.append((request.url, monotonic()))
        results = spider.responses.setdefault(request.url, [200])
        result = results.pop(0) if len(results) > 1 else results[0]
        if isinstance(result, Exception):
            raise result
        status, headers = result if isinstance(result, tuple) else (result, {})
        request.meta["download_latency"] = 0.001
        return HtmlResponse(request.url, request=request, status=status, headers=headers, body=b"<html></html>")

    async def close(self):
        pass


class FakePipeline:
    def process_item(self, item):
        if item.get("fail_save"):
            raise RuntimeError("simulated Mongo failure")
        return item


class ControlSpider(Spider):
    name = "control_test"
    _domain_name = "example_test"

    def __init__(self, responses=None, controller=None, fail_save=None, **kwargs):
        super().__init__(**kwargs)
        self.responses = responses or {}
        self.sent = []
        self._controller = controller or MemoryController()
        self._crawl_progress = CrawlProgress({"latest_lastmod": stamp(0) - timedelta(minutes=5)})
        self.crawl_urls_list = [{"loc": article(i), "lastmod": stamp(i * 5)} for i in range(3)]
        self._crawl_point = {"latest_lastmod": stamp(10)}
        self.fail_save = fail_save

    async def start(self):
        for row in self.crawl_urls_list:
            yield Request(row["loc"], callback=self.parse)

    def parse(self, response):
        yield {"url": response.url, "fail_save": response.url == self.fail_save}

    def closed(self, reason):
        self.safe_point = self._crawl_progress.safe_point(self)


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # 模擬応答テストから実 Slack へ通知しない。
        notice_patch = patch("news_crawl.adaptive_throttle._send_rate_limit_notice")
        self.notice = notice_patch.start()
        self.addCleanup(notice_patch.stop)

    async def test_429_sends_notice_for_each_backoff(self):
        await self.run_crawl(responses={article(0): [429, 429, 200]})
        self.assertEqual(self.notice.call_count, 2)
        first, second = [call.args[0] for call in self.notice.call_args_list]
        self.assertIn("HTTP 429", first)
        self.assertIn("0.01 → 0.02 秒", first)
        self.assertIn("0.02 → 0.03 秒", second)
        self.assertIn("再開可能日時 (UTC)", first)

    async def test_503_does_not_send_rate_limit_notice(self):
        await self.run_crawl(responses={article(0): [(503, {"Retry-After": "0"}), 200]})
        self.notice.assert_not_called()

    async def test_notice_failure_does_not_stop_crawl(self):
        self.notice.side_effect = RuntimeError("mock Slack failure")
        crawler = await self.run_crawl(responses={article(0): [429, 200]})
        self.notice.assert_called_once()
        self.assertEqual(crawler.spider.safe_point["latest_lastmod"], stamp(10))
        self.assertFalse(crawler.get_extension(AdaptiveThrottle).stopped)

    async def run_crawl(self, *, settings=None, **kwargs):
        # 外部通信と実DBを使わず、Scrapyのキュー・ミドルウェア・シグナルは実装本体を動かす。
        config = {
            "TWISTED_REACTOR_ENABLED": False,
            "TELNETCONSOLE_ENABLED": False,
            "LOG_ENABLED": False,
            "ROBOTSTXT_OBEY": False,
            "RETRY_TIMES": 1,
            "DOWNLOAD_DELAY": 0.01,
            "RANDOMIZE_DOWNLOAD_DELAY": False,
            "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
            "AUTOTHROTTLE_ENABLED": True,
            "AUTOTHROTTLE_START_DELAY": 0.01,
            "AUTOTHROTTLE_MAX_DELAY": 0.1,
            "RATE_LIMIT_DELAY_STEP": 0.01,
            "RATE_LIMIT_COOLDOWN": 0.04,
            "RATE_LIMIT_MAX_RETRIES": 2,
            "RATE_LIMIT_MAX_WAIT": 1,
            "DOWNLOADER": "news_crawl.adaptive_throttle.ThrottledDownloader",
            "EXTENSIONS": {
                "scrapy.extensions.throttle.AutoThrottle": None,
                "news_crawl.adaptive_throttle.AdaptiveThrottle": 0,
            },
            "DOWNLOADER_MIDDLEWARES": {
                "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,
                "news_crawl.adaptive_throttle.RateLimitRetryMiddleware": 550,
            },
            "SPIDER_MIDDLEWARES": {"news_crawl.spiders.common.crawl_progress.CrawlProgressMiddleware": 50},
            "DOWNLOAD_HANDLERS": {"http": FakeHandler, "https": FakeHandler},
            "ITEM_PIPELINES": {FakePipeline: 300},
        }
        config.update(settings or {})
        runner = AsyncCrawlerRunner(config)
        crawler = runner.create_crawler(ControlSpider)
        await asyncio.wait_for(runner.crawl(crawler, **kwargs), timeout=5)
        return crawler

    async def test_429_pauses_already_queued_requests_and_retries(self):
        crawler = await self.run_crawl(responses={article(0): [429, 429, 200]})
        spider = crawler.spider
        self.assertEqual(spider.safe_point["latest_lastmod"], stamp(10))
        self.assertEqual([round(state["download_delay"], 2) for state in spider._controller.history], [0.02, 0.03])
        for index, (url, sent_at) in enumerate(spider.sent[:-1]):
            if url == article(0) and index < 3:
                self.assertGreaterEqual(spider.sent[index + 1][1] - sent_at, 0.039)
        self.assertEqual(crawler.stats.get_value("rate_limit/retries"), 2)

    async def test_retry_exhaustion_does_not_send_queued_requests(self):
        crawler = await self.run_crawl(responses={article(1): [429]})
        self.assertIn(crawler.stats.get_value("finish_reason"), ("rate_limit_retry_exhausted",))
        self.assertEqual(crawler.spider.safe_point["latest_lastmod"], stamp(0))
        self.assertEqual(sum(url == article(1) for url, _ in crawler.spider.sent), 3)

    async def test_persisted_delay_and_deadline_are_loaded(self):
        controller = MemoryController(
            {"download_delay": 0.035, "retry_after_until": datetime.now(UTC) + timedelta(seconds=0.08)}
        )
        start = monotonic()
        crawler = await self.run_crawl(controller=controller)
        sent = crawler.spider.sent
        self.assertGreaterEqual(sent[0][1] - start, 0.07)
        self.assertTrue(all(b[1] - a[1] >= 0.034 for a, b in zip(sent, sent[1:], strict=False)))
        self.assertEqual(controller.history, [])  # 応答遅延による一時的な調整値は永続化しない。

    async def test_503_retry_after_waits_without_increasing_floor(self):
        crawler = await self.run_crawl(responses={article(0): [(503, {"Retry-After": "0"}), 200]})
        self.assertEqual(crawler.spider._controller.state["download_delay"], 0.01)
        self.assertEqual(crawler.stats.get_value("rate_limit/retries"), 1)

    async def test_503_without_retry_after_uses_standard_retry(self):
        crawler = await self.run_crawl(responses={article(0): [503, 200]})
        self.assertEqual(crawler.spider._controller.history, [])
        self.assertEqual(crawler.stats.get_value("retry/count"), 1)
        self.assertEqual(crawler.spider.safe_point["latest_lastmod"], stamp(10))

    async def test_site_cooldown_does_not_stop_another_crawler(self):
        waiting, normal = await asyncio.gather(
            self.run_crawl(responses={article(0): [429, 200]}, settings={"RATE_LIMIT_COOLDOWN": 0.2}),
            self.run_crawl(),
        )
        self.assertLess(normal.spider.sent[-1][1], waiting.spider.sent[1][1])

    async def test_database_failure_during_backoff_stops_sending(self):
        controller = MemoryController()
        controller.download_control_update = Mock(side_effect=RuntimeError("simulated DB failure"))
        crawler = await self.run_crawl(controller=controller, responses={article(0): [429]})
        self.assertEqual(crawler.stats.get_value("finish_reason"), "rate_limit_persistence_failed")
        self.assertEqual(len(crawler.spider.sent), 1)

    async def test_persisted_cooldown_beyond_budget_sends_nothing(self):
        controller = MemoryController(
            {"download_delay": 0.01, "retry_after_until": datetime.now(UTC) + timedelta(hours=1)}
        )
        crawler = await self.run_crawl(controller=controller)
        self.assertEqual(crawler.spider.sent, [])

    async def test_playwright_page_is_closed_before_retry(self):
        crawler = await self.run_crawl()
        middleware = RateLimitRetryMiddleware.from_crawler(crawler)
        page = SimpleNamespace(is_closed=lambda: False, close=AsyncMock())
        request = Request(article(0), meta={"rate_limit_response": True, "playwright_page": page})
        retry = await middleware.process_response(request, Response(article(0), status=429))
        page.close.assert_awaited_once()
        self.assertNotIn("playwright_page", retry.meta)
        self.assertEqual(retry.meta["rate_limit_retries"], 1)

    @patch.dict(os.environ, TEST_MONGO_ENV)
    async def test_mainichi_reads_until_checkpoint_beyond_first_page(self):
        from news_crawl.spiders.common.urls_continued_skip_check import UrlsContinuedSkipCheck
        from news_crawl.spiders.mainichi_jp_crawl import MainichiJpCrawlSpider, base_start_url
        from scrapy.settings import Settings

        rows = [{"loc": article(i), "lastmod": stamp(0)} for i in range(40)]
        previous = {base_start_url: {"urls": rows[30:40]}}
        spider = object.__new__(MainichiJpCrawlSpider)
        spider.settings = Settings({"CONTINUED_MAX_LISTING_PAGES": 4})
        spider._crawl_progress = CrawlProgress(previous)
        spider._crawl_point = copy.deepcopy(previous)
        spider.url_continued = UrlsContinuedSkipCheck(previous, base_start_url, True)
        spider.news_crawl_input = SimpleNamespace(url_pattern=None, crawling_start_time=stamp(0), debug=False)
        spider.all_urls_list = []
        spider.crawl_urls_list = []
        spider.crawl_target_urls = []
        spider._load_until = AsyncMock()
        extracts = [{"link": row["loc"], "lastmod": row["lastmod"]} for row in rows]
        spider._extract = AsyncMock(side_effect=[extracts[:20], extracts])
        button = SimpleNamespace(click=AsyncMock())
        page = SimpleNamespace(url=base_start_url, close=AsyncMock(), locator=lambda _: button)
        response = HtmlResponse(base_start_url, request=Request(base_start_url, meta={"playwright_page": page}))
        requests = [request async for request in spider._parse_listing(response, continued=True)]
        self.assertTrue(spider.url_continued.skip_flg)
        self.assertIn(article(29), [request.url for request in requests])
        button.click.assert_awaited_once()
        page.close.assert_awaited_once()

    async def test_403_and_mongo_save_error_leave_safe_frontier(self):
        crawler = await self.run_crawl(responses={article(1): [403]})
        self.assertEqual(crawler.spider.safe_point["latest_lastmod"], stamp(0))
        self.assertEqual(crawler.spider._controller.history, [])
        crawler = await self.run_crawl(fail_save=article(1))
        self.assertEqual(crawler.spider.safe_point["latest_lastmod"], stamp(0))

    async def test_long_retry_after_stops_without_shortening_server_deadline(self):
        crawler = await self.run_crawl(responses={article(0): [(429, {"Retry-After": "3600"})]})
        self.assertEqual(len(crawler.spider.sent), 1)
        self.assertGreater(
            crawler.spider._controller.state["retry_after_until"], datetime.now(UTC) + timedelta(minutes=59)
        )

    async def test_autothrottle_floor_changes_by_three_seconds(self):
        crawler = await self.run_crawl()
        throttle = crawler.get_extension(AdaptiveThrottle)
        throttle.base_delay = 9
        slot = Slot(1, 12, False)
        for _ in range(10):
            throttle._adjust_delay(slot, 0.01, Response(article(0)))
        self.assertGreaterEqual(slot.delay, 9)
        throttle.controller = Mock()
        throttle.stop = Mock()
        throttle.base_delay = 3
        throttle.step = 3
        for expected in (6, 9, 12):
            throttle._response_downloaded(Response(article(0), status=429), Request(article(0)), crawler.spider)
            self.assertEqual(throttle.base_delay, expected)


if __name__ == "__main__":
    unittest.main()
