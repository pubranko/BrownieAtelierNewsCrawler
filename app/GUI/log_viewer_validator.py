import re

from dateutil import parser
from pydantic import BaseModel, ValidationInfo, field_validator
from shared.settings import TIMEZONE


class LogViewerValidator(BaseModel):
    date_from: str = ""
    time_from: str = ""
    date_to: str = ""
    time_to: str = ""
    record_type: list = []
    log_level_value: int = 9

    """
    定義順にチェックされる。
    valuesにはチェック済みの値のみが入るため順序は重要。(単項目チェック、関連項目チェックの順で定義するのが良さそう。)
    """

    ################################
    # 単項目チェック
    ################################
    @field_validator("date_from", "date_to")
    @classmethod
    def date_check(cls, value: str) -> str:
        """date_fromのチェックを行う。正常の場合、半角数値に変換して返す。"""
        if value:
            assert value.isdecimal(), "数字以外が含まれている"
            # int(value)  #全て半角数値へ変換
            pattern = re.compile(r"[0-9]{4}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])")
            assert pattern.match(value), "yyyymmdd以外の入力不可"
            try:
                parser.parse(value)
            except ValueError as exc:
                raise ValueError("存在しない月日") from exc
            return value
        else:
            return value

    @field_validator("time_from", "time_to")
    @classmethod
    def time_check(cls, value: str) -> str:
        if value:
            assert value.isdecimal(), "数字以外が含まれている"
            # int(value)  #全て半角数値へ変換
            pattern = re.compile(r"([01][0-9]|2[0-3])[0-5][0-9][0-5][0-9]")
            assert pattern.match(value), "hhmmss以外の入力不可"
            return value
        else:
            return value

    @field_validator("record_type")
    @classmethod
    def record_type_check(cls, value: list) -> list:
        if value:
            assert isinstance(value, list), "リスト型以外がエラー"
        return value

    @field_validator("log_level_value")
    @classmethod
    def log_level_value_check(cls, value: int) -> int:
        if value:
            assert isinstance(value, int), "整数型以外がエラー"
        return value

    ###################################
    # 関連項目チェック
    ###################################
    @field_validator("time_from")
    @classmethod
    def time_from_check(cls, value: str, info: ValidationInfo) -> str:
        if value:
            assert info.data["date_from"], "時間を指定する場合、日付入力は必須です"
        return value

    @field_validator("time_to")
    @classmethod
    def time_to_check(cls, value: str, info: ValidationInfo) -> str:
        if value:
            assert info.data["date_to"], "時間を指定する場合、日付入力は必須です"
        return value

    #####################################
    # 型変換
    #####################################
    def datetime_from(self):
        return parser.parse(self.date_from + self.time_from).astimezone(TIMEZONE)

    def datetime_to(self):
        return parser.parse(self.date_to + self.time_to).astimezone(TIMEZONE)
