from datetime import datetime, date, time
from dateutil.relativedelta import relativedelta
from typing import Any, Optional, Tuple, Final, Literal
from pydantic import BaseModel
from shared.settings import TIMEZONE

CONST__START_TIME: Final[str] = "start_time"
CONST__BASE_DATE: Final[str] = "base_date"


class StatsInfoCollectInput(BaseModel):
    base_date: Optional[date] = None

    #####################
    # 定数
    #####################
    BASE_DATE: str = Literal[f"{CONST__BASE_DATE}"]
    """定数: base_date"""

    def __init__(self, **data: Any):
        """あとで"""
        super().__init__(**data)

    """
    定義順にチェックされる。
    valuesにはチェック済みの値のみが入るため順序は重要。(単項目チェック、関連項目チェックの順で定義するのが良さそう。)
    """
    ##################################
    # 単項目チェック、省略時の値設定
    ##################################

    ###################################
    # 関連項目チェック
    ###################################

    #####################################
    # カスタマイズデータ
    #####################################
    def base_date_get(self, start_time) -> Tuple[datetime, datetime]:
        """
        レポート期間(report_term)と基準日(base_date)を基に基準期間(base_date_from, base_date_to)を取得する。
        ※基準日(base_date)=基準期間to(base_date_to)となる。
        """
        if self.base_date:
            base_date_from = self.base_date
            base_date_from = datetime.combine(self.base_date, time.min, TIMEZONE)
        else:
            base_date_from = start_time.replace(
                hour=0, minute=0, second=0, microsecond=0
            ) - relativedelta(days=1)

        base_date_to = base_date_from + relativedelta(days=1)

        return (base_date_from, base_date_to)
