import os
import sys

sys.path.append(os.getcwd())
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel

path = os.getcwd()


class UrlsContinuedSkipCheck:
    """
    前回の続きからクロールさせるためのチェックを行う。
    """

    crawl_point_save: dict = {}
    last_time_urls: list = []
    continued: bool | None
    skip_flg: bool = False
    check_count: int = 10

    def __init__(self, crawl_point: dict, base_url: str, continued: bool | None) -> None:
        """
        前回の続きの指定がある場合、前回のクロールポイントの5件のurlをクラス変数へ保存する。
        """
        # 途中完了で保存した10件も、通常のクロールポイントと同じように照合する。
        # クラス属性のリストを変更すると他のインスタンスへ影響するため、毎回初期化する。
        self.last_time_urls = []
        self.skip_flg = False
        self.continued = continued
        if self.continued:
            if base_url in crawl_point:
                self.last_time_urls = [_[ControllerModel.LOC] for _ in crawl_point[base_url][ControllerModel.URLS]]
        self.remaining_threshold = len(self.last_time_urls) // 2
        self.has_checkpoint = bool(self.last_time_urls)

    def skip_check(self, url: str) -> bool:
        """
        前回の続きの指定がある場合、前回のクロールポイントの5件のurlまで読み込みが完了しているか判定を行う。
        ・前回のクロールポイントまで読み込みが完了していた場合、引数のurlをスキップ対象（True）とする。
        ・上記以外の場合、引数のurlをスキップ対象外（False）とする。
        ・ただし、前回の続きの指定がない場合、常にスキップ対象外（False）を返す。
        ※前回のクロールポイントには10件のurlがあるが、url取得中に更新されトップページへ移動している可能性がある。
          無限ループに陥らないように5/10件で完了とさせる。
        """
        if self.continued and self.has_checkpoint:
            if len(self.last_time_urls) <= self.remaining_threshold:
                self.skip_flg = True

            if url in self.last_time_urls:
                self.last_time_urls.remove(url)

        return self.skip_flg
