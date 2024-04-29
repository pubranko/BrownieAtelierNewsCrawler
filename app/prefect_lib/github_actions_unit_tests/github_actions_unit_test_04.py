# カレントディレクトリをpythonpathに追加
import os
import sys
current_directory = os.environ.get('PWD')
if current_directory:
    sys.path.append(current_directory)

# <8>
# スクレイピング使用パターンレポート
#   scraper_pattern_report_flow.py
from datetime import datetime, timedelta
from prefect_lib.flows.scraper_pattern_report_flow import \
    scraper_pattern_report_flow
from prefect_lib.data_models.scraper_pattern_report_input import \
    ScraperPatternReportConst
from shared.settings import TIMEZONE

# 基準日を翌日にずらしてから一週間分の情報を収集
scraper_pattern_report_flow(
    report_term=ScraperPatternReportConst.REPORT_TERM__WEEKLY,   
    base_date=datetime.now().astimezone(TIMEZONE) + timedelta(days=1),
)

# <9>
# Scrapy統計情報集計
#   stats_info_collect_flow.py
# from datetime import date
from prefect_lib.flows.stats_info_collect_flow import stats_info_collect_flow
# from shared.settings import TIMEZONE

#基準日(0:00:00)〜翌日(0:00:00)までの期間が対象となる。
# stats_info_collect_flow(base_date=date(2023, 7, 1))
# 日を跨いだ場合に備えて2日分を収集
_ = datetime.now().astimezone(TIMEZONE)
stats_info_collect_flow(base_date=_.date())
_ = _ - timedelta(days=1)
stats_info_collect_flow(base_date=_.date())

# <10>
# Scrapy統計情報レポート
#   stats_analysis_report_flow.py
# from datetime import date
from prefect_lib.data_models.stats_analysis_report_input import \
    StatsAnalysisReportConst
from prefect_lib.flows.stats_analysis_report_flow import \
    stats_analysis_report_flow

# 基準日(0:00:00)〜翌日(0:00:00)までの期間が対象となる。
# 一週間分を1日ごとに集計した結果を取得する。
_ = datetime.now().astimezone(TIMEZONE)
stats_analysis_report_flow(
    report_term=StatsAnalysisReportConst.REPORT_TERM__WEEKLY,
    totalling_term=StatsAnalysisReportConst.TOTALLING_TERM__DAILY,
    base_date=_.date(),
)

# <11>
# 同期チェック
#   crawl_sync_check_flow.py
# from datetime import datetime
from prefect_lib.flows.crawl_sync_check_flow import crawl_sync_check_flow
from shared.settings import TIMEZONE

# 絞り込み用の引数がなければ全量チェック
crawl_sync_check_flow(
    # domain='sankei.com',
    # start_time_from=datetime(2023, 6, 1, 0, 0, 0, 000000).astimezone(TIMEZONE),
    # start_time_to=datetime(2023, 6, 30, 23, 59, 59, 999999).astimezone(TIMEZONE),
)
