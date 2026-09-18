"""再取得を許容し、未取得・未保存の記事を飛び越えないクロールポイントを求める。

起動時に前回位置を保持し、実行中の要求・解析・保存結果をメモリー上で集計する。
終了時に lastmod または URL の並びから安全な再開位置を返し、呼び出し元が
controller に保存する。このモジュール自体は DB 更新やリトライ制御を行わない。
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any
from urllib.parse import unquote, urldefrag

from scrapy import Request, signals
from scrapy.crawler import Crawler


def url_key(url: str) -> str:
    """フラグメントを除去し、URL エンコードを戻した照合用キーを返す。

    要求・保存結果・一覧の記事 URL を比較するための変換であり、送信 URL は変更しない。
    """
    return unquote(urldefrag(url)[0])


def utc_date(value: Any) -> datetime | None:
    """日時を UTC にそろえる。datetime 以外は解析せず None を返す。

    タイムゾーンを持たない MongoDB 由来などの日時は UTC として扱う。
    """
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class CrawlProgress:
    """一回のクロールの進捗を集計し、未完了の記事より先への位置更新を防ぐ。

    要求の登録、解析完了、パイプライン通過、失敗を別々に記録する。
    HTTP 応答の取得だけでは記事の保存成功と見なさず、分割記事の後続ページも確認する。
    一覧・サイトマップの解析が未完了で記事の全体像が不明な場合は前回位置を維持する。
    """

    def __init__(self, previous: dict):
        """controller から読み込んだ前回位置のコピーと、今回の集計領域を用意する。

        Args:
            previous: 前回のクロールポイント。初回は空の辞書。
                実行中にスパイダー側の候補位置が変わっても前回値を保持する。
        """
        self.previous = deepcopy(previous)
        # 各集合には url_key でそろえた URL を保存する。再試行の重複は集計しない。
        self.scheduled: set[str] = set()
        self.parsed: set[str] = set()
        self.saved: set[str] = set()
        self.failed: set[str] = set()
        # 要求 URL → 元記事 URL。分割記事の未保存を元記事全体の未完了として扱う。
        self.roots: dict[str, str] = {}
        # URL を特定できない失敗と、解析が必須な一覧・サイトマップの起点。
        self.discovery_failed = False
        self.expected_discovery: set[str] = set()
        # 一覧の起点 URL → ページ番号 → 新しい順の記事情報。
        self.listing_pages: dict[str, dict[int, list[dict]]] = {}

    def record_listing(self, source: str, page: int, rows: list[dict]) -> None:
        """再開位置の URL を選ぶため、スキップ判定前の一覧をページ単位で保持する。

        Args:
            source: controller のキーにも使う一覧の起点 URL。
            page: 新しい一覧から順に増えるページ番号。
            rows: 一覧の表示順（新しい順）の記事情報。各要素は loc と lastmod を持つ。

        同じページの再登録はコピーで置き換える。ここでの記録は記事の保存成功を意味しない。
        """
        # 再試行でページの取得順が逆転しても、新しい→古い順をページ番号で復元できるようにする。
        self.listing_pages.setdefault(source, {})[page] = deepcopy(rows)

    def expect_discovery(self, urls) -> None:
        """記事発見に必要な一覧・サイトマップの起点 URL 群を要求生成前に登録する。

        起点の要求がまだスケジュールされないうちに終了しても、未解析と判定できるようにする。
        """
        self.expected_discovery.update(url_key(url) for url in urls)

    def scheduled_request(self, request: Request) -> None:
        """要求の登録を記録し、元 URL と分割記事の親 URL を要求メタ情報で関連付ける。

        progress_url はリダイレクト・再試行をまたぐ照合キー、checkpoint_root は
        分割記事の元記事を表す。要求の登録は、送信や保存の完了を意味しない。
        """
        # リダイレクト後も要求元URLで照合する。リトライではこのキーを引き継ぐ。
        original = str(request.meta.setdefault("progress_url", request.url))
        key = url_key(original)
        self.scheduled.add(key)
        self.roots[key] = url_key(request.meta.get("checkpoint_root", original))

    def parsed_response(self, response: Any) -> None:
        """応答の解析結果を最後まで取り出せたことを記録する。記事の保存とは区別する。"""
        self.parsed.add(url_key(response.meta.get("progress_url", response.url)))

    def saved_response(self, response: Any) -> None:
        """item_scraped 通知の応答を、全パイプラインを通過した保存済み URL として記録する。"""
        # パイプラインを通過した記事だけを取得済みにする。HTTP 200 だけでは不足。
        self.saved.add(url_key(response.meta.get("progress_url", response.url)))

    def failed_response(self, response: Any) -> None:
        """解析・保存の例外、アイテム破棄、不正サイトマップなどの失敗を記録する。

        応答が None で対象 URL を特定できない場合は、安全のため前回位置を維持させる。
        再試行中の HTTP エラーではなく、呼び出し元が確定した失敗を渡す。
        """
        if response is None:
            self.discovery_failed = True
        else:
            self.failed.add(url_key(response.meta.get("progress_url", response.url)))

    def safe_point(self, spider: Any) -> dict:
        """終了時の進捗から、未完了の記事を飛び越えないクロールポイントを返す。

        Args:
            spider: crawl_urls_list に今回の対象記事、_crawl_point に候補位置を持つ
                スパイダー。URL 方式では一覧情報と url_continued.check_count も参照する。

        Returns:
            controller に保存する辞書。記事発見が未完了なら前回位置、完了していれば
            lastmod 方式または URL 方式で安全と判定した位置のコピーを返す。

        スパイダーの候補位置や DB は変更しない。記事の後続ページが未保存の場合も
        元記事を未完了に含め、取得済みの記事の再取得よりも取りこぼし防止を優先する。
        """
        records = spider.crawl_urls_list
        candidate = deepcopy(spider._crawl_point)
        targets = {url_key(row["loc"]) for row in records}
        # サイトマップ・一覧の取得や解析が未完了なら、まだ見えていない記事がある。
        # その日時/順序は推測できないので、前回位置を維持する。
        discovery = self.expected_discovery | {key for key in self.scheduled if self.roots.get(key, key) not in targets}
        if self.discovery_failed or discovery - self.parsed or discovery & self.failed:
            return deepcopy(self.previous)

        incomplete = targets - self.saved
        incomplete.update(targets & self.failed)
        # 記事の2ページ目以降で失敗した場合も、その記事の lastmod/URL を進めない。
        for key, root in self.roots.items():
            if root in targets and (key not in self.saved or key in self.failed):
                incomplete.add(root)

        if "latest_lastmod" in candidate:
            return self._sitemap_point(records, candidate, incomplete)
        return self._url_point(spider, candidate, incomplete)

    def _sitemap_point(self, records: list[dict], candidate: dict, incomplete: set[str]) -> dict:
        """最も古い未完了 lastmod より前の、完了した日時まで候補位置を進める。

        records は対象記事、candidate は変更可能な候補位置のコピー、incomplete は
        未完了の元記事の照合用 URL 集合。同一日時の記事は全件完了するまで進めない。
        例として 10:00・10:10 が成功、10:05 が失敗なら 10:00 を返す。

        全件完了なら候補を採用し、日時不明や前回より進める日時がない場合は前回位置を返す。
        """
        # 同一時刻の全記事を一組として扱う。一件でも未保存なら、その時刻より前まで進める。
        dated = [(url_key(row["loc"]), utc_date(row.get("lastmod"))) for row in records]
        if any(date is None for _, date in dated):
            return deepcopy(self.previous)
        failed_dates = [date for key, date in dated if key in incomplete and date is not None]
        if not failed_dates:
            return candidate
        boundary = min(failed_dates)
        completed_dates = [date for _, date in dated if date is not None and date < boundary]
        if not completed_dates:
            return deepcopy(self.previous)
        safe_date = max(completed_dates)
        old_date = utc_date(self.previous.get("latest_lastmod"))
        if old_date is not None and safe_date <= old_date:
            return deepcopy(self.previous)
        candidate["latest_lastmod"] = safe_date
        return candidate

    def _url_point(self, spider: Any, candidate: dict, incomplete: set[str]) -> dict:
        """一覧の順序から、次回の継続判定に使う URL 群（通常 10 件）を選ぶ。

        candidate は変更可能な候補位置のコピー、incomplete は未完了の照合用 URL 集合。
        記録した一覧をページ番号順に結合し、同じ URL は最初の出現位置を残す。
        一覧記録がなければ spider.all_urls_list を使う。いずれも新しい順を前提とする。

        全件完了なら先頭から check_count 件、未完了があればその最も古い記事より
        古い側から同数を再開の目印にする。複数起点、未完了記事の位置不明、目印不足は
        前回位置を維持する。ただし初回の全件完了時は規定件数未満も許容する。
        """
        # 現在の一覧スパイダーはそれぞれ単一の起点を持つ。複数起点なら全体の順序を推測しない。
        if len(candidate) != 1:
            return deepcopy(self.previous)
        source = next(iter(candidate))
        if "urls" not in candidate[source]:
            return deepcopy(self.previous)
        pages = self.listing_pages.get(source, {})
        rows = [row for page in sorted(pages) for row in pages[page]] if pages else spider.all_urls_list
        unique: dict[str, dict] = {}
        for row in rows:
            unique.setdefault(url_key(row["loc"]), row)
        ordered = list(unique.values())
        if not incomplete:
            # 10件未満では既存の十分な目印を失わない。初回は取得できた件数だけを保存する。
            if self.previous and len(ordered) < spider.url_continued.check_count:
                return deepcopy(self.previous)
            candidate[source]["urls"] = deepcopy(ordered[: spider.url_continued.check_count])
            return candidate
        failed_positions = [i for i, row in enumerate(ordered) if url_key(row["loc"]) in incomplete]
        if {url_key(ordered[i]["loc"]) for i in failed_positions} != incomplete:
            return deepcopy(self.previous)
        # 一覧は新しい→古い順。最も古い未取得記事より後ろの10件を再開の目印にする。
        # 成功した最新10件を単純に選ぶと、その途中の未取得記事を飛ばすため不可。
        count = spider.url_continued.check_count
        anchors = ordered[max(failed_positions) + 1 :][:count]
        if len(anchors) < count:
            return deepcopy(self.previous)
        candidate[source]["urls"] = deepcopy(anchors)
        return candidate


class CrawlProgressMiddleware:
    """Scrapy の通知と解析結果を、スパイダーが持つ CrawlProgress に橋渡しする。

    各サイトに共通する要求登録・パイプライン通過・失敗の通知を接続し、解析完了も記録する。
    DB 更新や再試行は行わず、終了時の安全な再開位置の判定に必要な事実を集める。
    """

    crawler: Crawler

    @classmethod
    def from_crawler(cls, crawler):
        """ミドルウェアを生成し、要求・保存成功・解析や保存の失敗のシグナルを接続する。"""
        instance = cls()
        instance.crawler = crawler
        crawler.signals.connect(instance.request_scheduled, signal=signals.request_scheduled)
        crawler.signals.connect(instance.item_scraped, signal=signals.item_scraped)
        crawler.signals.connect(instance.failed, signal=signals.spider_error)
        crawler.signals.connect(instance.failed, signal=signals.item_error)
        crawler.signals.connect(instance.failed, signal=signals.item_dropped)
        return instance

    def request_scheduled(self, request, spider):
        """request_scheduled 通知を受け、終了時に未完了要求を検出できるよう登録する。"""
        spider._crawl_progress.scheduled_request(request)

    def item_scraped(self, response, spider):
        """全パイプライン通過の通知を保存済みとして反映する。応答不明なら記録しない。"""
        if response is not None:
            spider._crawl_progress.saved_response(response)

    def failed(self, response, spider):
        """spider_error・item_error・item_dropped を、安全な位置の判定に使う失敗として渡す。"""
        spider._crawl_progress.failed_response(response)

    async def process_spider_output(self, response, result):
        """解析結果の要求・アイテムをそのまま渡し、反復完了後に解析済みを記録する。

        反復が中断した場合は完了記録に到達しない。解析完了とパイプラインでの保存成功は
        別に記録するため、ここで結果を取り出しただけでは記事の保存済み判定にはならない。
        """
        # ジェネレーターの最後まで解析できたときだけ完了。途中の例外では完了にしない。
        async for item in result:
            yield item
        progress = getattr(self.crawler.spider, "_crawl_progress", None)
        if progress is not None:
            progress.parsed_response(response)
