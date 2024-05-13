
if __name__ == "__main__":

    # カレントディレクトリをpythonpathに追加
    import os
    import sys
    current_directory = os.environ.get('PWD')
    if current_directory:
        sys.path.append(current_directory)

    # <9>
    # Scrapy統計情報集計
    #   stats_info_collect_flow.py
    from datetime import datetime, timedelta
    from prefect_lib.flows.stats_info_collect_flow import stats_info_collect_flow
    from shared.settings import TIMEZONE

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

    # From(0:00:00)〜基準日(0:00:00)までの期間が対象となる。
    # 一週間分を1日ごとに集計した結果を取得する。
    # 本来は前日までのデータを取得するためのものであるため、基準日を＋1日にし当日分も対象となるよう調整。
    _ = datetime.now().astimezone(TIMEZONE) + timedelta(days=1)
    from logging import Logger
    import logging
    logger: Logger = logging.getLogger()
    logger.info(f"＝＝＝　確認！ {datetime.now().isoformat()}")
    logger.info(f"＝＝＝　確認！ {_}")
    stats_analysis_report_flow(
        report_term=StatsAnalysisReportConst.REPORT_TERM__WEEKLY,
        totalling_term=StatsAnalysisReportConst.TOTALLING_TERM__DAILY,
        base_date=_.date(),
    )
