from datetime import datetime
from typing import Any, Final
from urllib.parse import urlparse

from news_crawl.settings import TIMEZONE
from pydantic import BaseModel, Field, ValidationInfo, field_validator

#####################################################################################
# 定数 (news_crawlの引数)
# ※クラス内で定義したかったが、その場合クラス内で参照できなかった。
#   次善の策としてモジュール定数側で定義。
#####################################################################################


class NewsCrawlInputConst:
    """NewsCrawlInput用のコンスタント定義クラス"""

    DEBUG: Final[str] = "debug"
    CRAWL_POINT_NON_UPDATE: Final[str] = "crawl_point_non_update"
    CRAWLING_START_TIME: Final[str] = "crawling_start_time"
    LASTMOD_TERM_MINUTES_FROM: Final[str] = "lastmod_term_minutes_from"
    LASTMOD_TERM_MINUTES_TO: Final[str] = "lastmod_term_minutes_to"
    PAGE_SPAN_FROM: Final[str] = "page_span_from"
    PAGE_SPAN_TO: Final[str] = "page_span_to"
    CONTINUED: Final[str] = "continued"
    DIRECT_CRAWL_URLS: Final[str] = "direct_crawl_urls"
    URL_PATTERN: Final[str] = "url_pattern"


class NewsCrawlInput(BaseModel):
    """
    News Crawlに対する引数用モデル
    引数のチェック及びデータモデルとしての機能を提供する。
    """

    # News Crawlerの動作モードに関する引数
    debug: bool = Field(False, title="デバックモードフラグ")
    crawl_point_non_update: bool = Field(False, title="クロールポイント更新なしフラグ")

    # クロール開始となる基準時間。指定がなかった場合、現在時刻とする。
    crawling_start_time: datetime = Field(datetime.now().astimezone(TIMEZONE), title="クロール開始時間")

    # クロール対象・範囲を指定する任意引数
    lastmod_term_minutes_from: int | None = Field(None, title="最終更新期間(分)From")
    lastmod_term_minutes_to: int | None = Field(None, title="最終更新期間(分)To")
    page_span_from: int | None = Field(None, title="ページ範囲")
    page_span_to: int | None = Field(None, title="ページ範囲", validate_default=True)
    continued: bool | None = Field(None, title="続きから再開")
    direct_crawl_urls: list[str] | None = Field(None, title="直接クロールするURLリスト")
    url_pattern: str | None = Field(None, title="URLパターンによる絞り込み")

    def __init__(self, **data: Any):
        super().__init__(**data)

    """
    クラス変数側の定義順にチェックされる。
    info.data には先に検証済みのフィールドが入るため、定義順に注意する。
    省略されたデフォルト値も検証する場合は Field(validate_default=True) を指定する。
    型変換前に検証する場合は field_validator(..., mode="before") を使う。
    """

    ##################################
    # 単項目チェック
    ##################################continued
    @field_validator(NewsCrawlInputConst.DIRECT_CRAWL_URLS)
    @classmethod
    def start_time_check(cls, value: list[str]) -> list[str] | None:
        if value:
            for url in value:
                parsed_url = urlparse(url)
                assert len(parsed_url.scheme) > 0, (
                    f"引数エラー({NewsCrawlInputConst.DIRECT_CRAWL_URLS}): URLとして解析できませんでした {url}"
                )
        return value

    @field_validator(NewsCrawlInputConst.LASTMOD_TERM_MINUTES_TO)
    @classmethod
    def lastmod_term_minutes_to_check(cls, value: int | None, info: ValidationInfo) -> int | None:
        if value and info.data[NewsCrawlInputConst.LASTMOD_TERM_MINUTES_FROM]:
            assert value <= info.data[NewsCrawlInputConst.LASTMOD_TERM_MINUTES_FROM], (
                f"引数エラー : {NewsCrawlInputConst.LASTMOD_TERM_MINUTES_FROM} と "
                f"{NewsCrawlInputConst.LASTMOD_TERM_MINUTES_TO} は、from > toで指定してください。"
                f"from({info.data[NewsCrawlInputConst.LASTMOD_TERM_MINUTES_FROM]}) : to({value})）"
            )
        return value

    @field_validator(NewsCrawlInputConst.PAGE_SPAN_TO)
    @classmethod
    def page_span_to_check(cls, value: int | None, info: ValidationInfo) -> int | None:
        assert (info.data[NewsCrawlInputConst.PAGE_SPAN_FROM] and value) or (
            not info.data[NewsCrawlInputConst.PAGE_SPAN_FROM] and not value
        ), (
            f"引数エラー : {NewsCrawlInputConst.PAGE_SPAN_FROM} と "
            f"{NewsCrawlInputConst.PAGE_SPAN_TO} は同時に指定してください。"
        )

        if value and info.data[NewsCrawlInputConst.PAGE_SPAN_FROM]:
            assert value >= info.data[NewsCrawlInputConst.PAGE_SPAN_FROM], (
                f"引数エラー : {NewsCrawlInputConst.PAGE_SPAN_FROM}と"
                f"{NewsCrawlInputConst.PAGE_SPAN_TO}はfrom ≦ toで指定してください。"
                f"from({info.data[NewsCrawlInputConst.PAGE_SPAN_FROM]}) : to({value})）"
            )

        return value

    ###################################
    #
    ###################################


if __name__ == "__main__":
    params = {
        "crawling_start_time": datetime(2022, 10, 1, 0, 0, 10),
        "debug": True,
        "crawl_point_non_update": False,
        "lastmod_term_minutes_from": 60,
        "lastmod_term_minutes_to": 0,
        "page_span_from": 2,
        "page_span_to": 3,
        "continued": False,
        "direct_crawl_urls": ["https://yahoo.co.jp"],
        "url_pattern": "topic",
        "aaaaa": "bbbbb",  # 関係無い項目は無視される。
        # CONST_CRAWLING_START_TIME='jko;jkl;jkl;'
    }
    a = NewsCrawlInput(**params)

    print(a.debug)
    print(a.crawl_point_non_update)
    print(a.lastmod_term_minutes_from)
    print(a.lastmod_term_minutes_to)
    print(a.page_span_from)
    print(a.page_span_to)
    print(a.continued)
    print(a.direct_crawl_urls)
    print(a.url_pattern)
    if a.crawling_start_time:
        aa: datetime = a.crawling_start_time
        print(aa)

    print("=====")

    b = NewsCrawlInput(
        debug=True,
        crawl_point_non_update=False,
        lastmod_term_minutes_from=60,
        lastmod_term_minutes_to=0,
        page_span_from=1,
        page_span_to=3,
        continued=False,
        # direct_crawl_urls = ['https://~','http://aaa~'],
        url_pattern="topic",
    )

    print(b.debug)
    print(b.crawl_point_non_update)
    print(b.lastmod_term_minutes_from)
    print(b.lastmod_term_minutes_to)
    print(b.page_span_from)
    print(b.page_span_to)
    print(b.continued)
    print(b.direct_crawl_urls)
    print(b.url_pattern)

    if b.crawling_start_time:
        bb: datetime = b.crawling_start_time
        print(bb)

    print(b.__dict__)  # クラス変数一括取得
