
from typing import Final
from prefect_lib.data_models.scraper_pattern_report_data import ScraperPatternReportData

class ScraperPatternReportConst:
    '''Regular observation controller update用定数クラス'''
    # 定数定義
    HEAD1: Final[str] = 'head1'
    HEAD2: Final[str] = 'head2'
    COL: Final[str] = 'col'
    EQUIVALENT_COLOR: Final[str] = 'equivalent_color'

    SCRAPER_PATTERN_ANALYSIS_COLUMNS_INFO: list = [
        # head1                     : 必須 : 見出し１行目
        # head2                     : 必須 : 見出し２行目
        # col                       : 必須 : データフレーム列名
        # digit_adjustment          : 任意 : 単位の調整。1000とした場合、'value/1000'となる。
        # number_format             : 任意 : 小数点以下の桁数。省略した場合'#,##0'
        # number_format             : 任意 : 小数点以下の桁数。省略した場合'#,##0'
        # equivalent_color          : 任意 : 上のセルと同値の場合の文字色。上と同値の場合、文字色を薄くするなどに使用する。
        # warning_value             : 任意 : ワーニングとする値を入れる配列
        # warning_value_over        : 任意 : 超過したらワーニングとする値
        # warning_background_color  : 任意 : ワーンングとなったセルの背景色
        #{'head1': '集計期間', 'head2': '', 'col': 'aggregate_base_term'},
        {HEAD1: 'スクレイパー情報', HEAD2: 'ドメイン', COL: ScraperPatternReportData.DOMAIN,
            EQUIVALENT_COLOR: 'bbbbbb'},
        {HEAD1: '', HEAD2: 'アイテム', COL: ScraperPatternReportData.SCRAPE_ITEMS,
            EQUIVALENT_COLOR: 'bbbbbb'},
        {HEAD1: '', HEAD2: '優先順位', COL: ScraperPatternReportData.PRIORITY},
        {HEAD1: '', HEAD2: 'パターン', COL: ScraperPatternReportData.PATTERN},
        {HEAD1: '', HEAD2: '使用回数', COL: ScraperPatternReportData.COUNT_OF_USE},
    ]



