def test_exec():

    # カレントディレクトリをpythonpathに追加
    import os
    import sys
    current_directory = os.environ.get('PWD')
    if current_directory:
        sys.path.append(current_directory)

    # <11>
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


    # <12>
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

if __name__ == "__main__":
    test_exec()
