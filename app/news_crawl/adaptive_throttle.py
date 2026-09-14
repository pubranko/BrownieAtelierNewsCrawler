"""全サイト共通の送信制御。基準間隔は永続化し、応答時間による調整は実行中だけ保持する。"""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from time import monotonic
from typing import TYPE_CHECKING, Any

from scrapy import Request, Spider, signals
from scrapy.core.downloader import Downloader, Slot
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.exceptions import CloseSpider, IgnoreRequest
from scrapy.extensions.throttle import AutoThrottle
from scrapy.http import Response
from scrapy.utils.defer import _schedule_coro

if TYPE_CHECKING:
    from scrapy.crawler import Crawler

logger = logging.getLogger(__name__)


def _send_rate_limit_notice(message: str) -> None:
    """既存の同期 Slack 通知を通常チャンネルへ送る。ワーカースレッドから呼ぶ。"""
    from BrownieAtelierNotice import settings
    from BrownieAtelierNotice.slack.slack_notice import slack_notice

    slack_notice(
        logger=logger,
        channel_id=settings.BROWNIE_ATELIER_NOTICE__SLACK_CHANNEL_ID__NOMAL,
        message=message,
    )


def retry_after_seconds(value: bytes | None, now: datetime) -> float | None:
    """Retry-After の秒数形式と HTTP-date 形式に対応。不正な値は通常の待機時間を使う。"""
    if value is None:
        return None
    try:
        text = value.decode("ascii").strip()
        if text.isdigit():
            seconds = float(text)
        else:
            date = parsedate_to_datetime(text)
            seconds = (date.replace(tzinfo=UTC) if date.tzinfo is None else date).timestamp() - now.timestamp()
        return max(0.0, seconds) if math.isfinite(seconds) else None
    except ValueError, TypeError, OverflowError, UnicodeError:
        return None


class AdaptiveThrottle(AutoThrottle):
    """AutoThrottle の下限を controller の基準間隔に合わせ、429/503 の待機を管理する。"""

    def __init__(self, crawler: Crawler):
        super().__init__(crawler)
        settings = crawler.settings
        self.base_delay = settings.getfloat("DOWNLOAD_DELAY")
        self.step = settings.getfloat("RATE_LIMIT_DELAY_STEP", 3)
        self.delay_cap = max(self.base_delay, settings.getfloat("RATE_LIMIT_MAX_DELAY", 60))
        self.cooldown = settings.getfloat("RATE_LIMIT_COOLDOWN", 60)
        self.retry_limit = settings.getint("RATE_LIMIT_MAX_RETRIES", 5)
        self.wait_limit = settings.getfloat("RATE_LIMIT_MAX_WAIT", 900)
        self.retry_until = datetime.now(UTC)
        self.wait_used = 0.0
        self.last_sent = 0.0
        # HTTP/HTTPS・サブドメインのキューをまたいでも、同じサイトの送信を直列化する。
        self.send_lock = asyncio.Lock()
        self.stopped = False
        self.controller: Any = None
        self.domain = ""
        self.notice_tasks: set[asyncio.Task] = set()
        crawler.signals.connect(self._wait_for_notices, signal=signals.spider_closed)

    async def _notify_rate_limit(self, message: str) -> None:
        """Slack の同期通信を別スレッドへ逃がし、通知例外をクロール制御から切り離す。"""
        try:
            await asyncio.to_thread(_send_rate_limit_notice, message)
        except Exception:
            logger.exception("429 の Slack 通知に失敗: %s", self.domain)

    async def _wait_for_notices(self) -> None:
        """通常終了時は、送信中の通知を待って取りこぼしを防ぐ。"""
        if self.notice_tasks:
            await asyncio.gather(*self.notice_tasks)

    def _spider_opened(self, spider: Spider) -> None:
        self.controller = getattr(spider, "_controller", None)
        self.domain = getattr(spider, "_domain_name", spider.name)
        if self.controller is not None:
            try:
                saved = self.controller.download_control_get(self.domain)
                delay = float(saved.get("download_delay", self.base_delay))
                if not math.isfinite(delay) or delay < 0:
                    raise ValueError("保存された download_delay が不正です")
                # 既存の保存値より速くしない。上限値を設定で下げても起動時に勝手に加速しない。
                self.base_delay = max(self.base_delay, delay)
                self.delay_cap = max(self.delay_cap, self.base_delay)
                until = saved.get("retry_after_until")
                if until is not None and not isinstance(until, datetime):
                    raise ValueError("保存された retry_after_until が日時ではありません")
                if isinstance(until, datetime):
                    self.retry_until = until.replace(tzinfo=UTC) if until.tzinfo is None else until
            except Exception as exc:
                self.stopped = True
                raise CloseSpider("rate_limit_state_load_failed") from exc
        super()._spider_opened(spider)
        remaining = max(0.0, (self.retry_until - datetime.now(UTC)).total_seconds())
        self.wait_used = remaining
        logger.info("送信制御: %s 基準間隔=%.1f秒 起動時待機=%.1f秒", self.domain, self.base_delay, remaining)
        if remaining > self.wait_limit:
            self.stop("rate_limit_wait_exhausted")

    def _min_delay(self) -> float:
        return self.base_delay

    def _adjust_delay(self, slot: Slot, latency: float, response: Response) -> None:
        # AutoThrottle は起動時に下限を保存するため、429 による変更をここでも反映する。
        self.mindelay = self.base_delay
        self.maxdelay = max(self.base_delay, self._max_delay())
        super()._adjust_delay(slot, latency, response)

    def _response_downloaded(self, response: Response, request: Request, spider: Spider) -> None:
        super()._response_downloaded(response, request, spider)
        now = datetime.now(UTC)
        retry_after = retry_after_seconds(response.headers.get(b"Retry-After"), now)
        if response.status != 429 and not (response.status == 503 and retry_after is not None):
            return

        # 429 は基準間隔を +3秒。503 は一時障害の可能性があるため待機期限だけを保存する。
        previous_delay = self.base_delay
        if response.status == 429:
            self.base_delay = min(self.delay_cap, self.base_delay + self.step)
        wait = max(self.cooldown, retry_after or 0.0)
        self.retry_until = max(self.retry_until, now + timedelta(seconds=wait))
        request.meta["rate_limit_response"] = True
        self.wait_used += wait
        for slot in self.crawler.engine.downloader.slots.values():
            slot.delay = max(slot.delay, self.base_delay)
        self.crawler.engine.downloader._delay = max(self.base_delay, self._start_delay())
        try:
            if self.controller is not None:
                self.controller.download_control_update(
                    self.domain,
                    {
                        "download_delay": self.base_delay,
                        "retry_after_until": self.retry_until,
                    },
                )
        except Exception:
            # 減速状態を保存できなければ、それ以上送信せず、取得済み地点を終了処理で確定する。
            logger.exception("送信制御の保存に失敗: %s", self.domain)
            self.stop("rate_limit_persistence_failed")
            return
        self.crawler.stats.inc_value("rate_limit/responses")
        self.crawler.stats.set_value("rate_limit/base_delay", self.base_delay)
        logger.warning(
            "HTTP %s: %s を %.1f秒待機、基準間隔 %.1f秒", response.status, self.domain, wait, self.base_delay
        )
        if response.status == 429:
            # 警告ごとに通知する。上限到達後も待機は発生するため、間隔が変わらなくても通知する。
            message = (
                "【クローラー HTTP 429：待機・間隔調整】\n"
                f"サイト: {self.domain}\nスパイダー: {spider.name}\n"
                f"基準間隔: {previous_delay:g} → {self.base_delay:g} 秒\n"
                f"待機時間: {wait:g} 秒\n"
                f"再開可能日時 (UTC): {self.retry_until.isoformat()}\n"
                "再試行・総待機時間の上限に達した場合はクロールを停止します。"
            )
            task = asyncio.create_task(self._notify_rate_limit(message))
            self.notice_tasks.add(task)
            task.add_done_callback(self.notice_tasks.discard)
        if self.wait_used > self.wait_limit or request.meta.get("rate_limit_retries", 0) >= self.retry_limit:
            self.stop("rate_limit_retry_exhausted")

    def stop(self, reason: str) -> None:
        if not self.stopped:
            self.stopped = True
            logger.error("送信を中止: %s (%s)。保存済みの安全な地点まで進捗を確定します。", self.domain, reason)
            _schedule_coro(self.crawler.engine.close_spider_async(reason=reason))

    async def wait_for_send(self, slot: Slot) -> None:
        """ネットワーク送信の直前で待つ。待機時間は download_latency に混ぜない。"""
        while True:
            engine_slot = self.crawler.engine._slot
            if self.stopped or (engine_slot is not None and engine_slot.closing):
                raise IgnoreRequest("サイトへの送信停止中")
            wait = max(
                (self.retry_until - datetime.now(UTC)).total_seconds(),
                self.last_sent + max(self.base_delay, slot.delay) - monotonic(),
            )
            if wait <= 0:
                self.last_sent = monotonic()
                return
            # 非同期で待機し、Ctrl+C などの終了要求も1秒以内に確認する。
            await asyncio.sleep(min(wait, 1.0))


class ThrottledDownloader(Downloader):
    """ミドルウェア通過済みのキューにも、送信直前の待機・停止を適用する。"""

    async def _download(self, slot: Slot, request: Request) -> Response:
        throttle = self.crawler.get_extension(AdaptiveThrottle)
        if throttle is None:
            return await super()._download(slot, request)
        async with throttle.send_lock:
            await throttle.wait_for_send(slot)
            # response_downloaded シグナルで429を処理し終えるまでロックを保持する。
            return await super()._download(slot, request)


class RateLimitRetryMiddleware:
    """待機を伴うリトライは専用の回数上限を使い、それ以外は Scrapy 標準に任せる。"""

    crawler: Crawler
    standard: RetryMiddleware

    @classmethod
    def from_crawler(cls, crawler):
        instance = cls()
        instance.crawler = crawler
        instance.standard = RetryMiddleware.from_crawler(crawler)
        return instance

    def process_exception(self, request, exception):
        return self.standard.process_exception(request, exception)

    async def process_response(self, request: Request, response: Response):
        if not request.meta.pop("rate_limit_response", False):
            # 403 は標準設定でもリトライ対象外。ヘッダーなし503・タイムアウトは標準の有限リトライ。
            return self.standard.process_response(request, response)
        # include_page=True の429ページを放置すると、ブラウザーのページ数上限で停止してしまう。
        page = request.meta.pop("playwright_page", None)
        if page is not None and not page.is_closed():
            await page.close()
        throttle = self.crawler.get_extension(AdaptiveThrottle)
        if throttle is None or throttle.stopped or request.meta.get("dont_retry", False):
            return response
        retry = request.copy()
        retry.dont_filter = True
        retry.priority += 1  # スケジューラー内で再取得を優先する。送信キュー済みの要求は追い越さない。
        retry.meta["rate_limit_retries"] = request.meta.get("rate_limit_retries", 0) + 1
        self.crawler.stats.inc_value("rate_limit/retries")
        return retry
