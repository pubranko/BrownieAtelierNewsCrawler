
if __name__ == "__main__":

    # カレントディレクトリをpythonpathに追加
    import os
    import sys
    current_directory = os.environ.get('PWD')
    if current_directory:
        sys.path.append(current_directory)

    # <7>
    # 手動スクレイピング
    #   manual_scrapying_flow.py
    from datetime import datetime, timedelta
    from prefect_lib.flows.manual_scrapying_flow import manual_scrapying_flow
    from shared.settings import TIMEZONE

    manual_scrapying_flow(
        # domain='sankei_com_sitemap',
        target_start_time_from=datetime.now().astimezone(TIMEZONE) - timedelta(minutes=60),
        target_start_time_to=datetime.now().astimezone(TIMEZONE),
        # urls=None,
        following_processing_execution=True,
    )


    # <8>
    # 手動ニュースクリップマスター保存
    #   manual_news_clip_master_save_flow.py
    # from datetime import datetime
    from prefect_lib.flows.manual_news_clip_master_save_flow import \
        manual_news_clip_master_save_flow
    # from shared.settings import TIMEZONE

    manual_news_clip_master_save_flow(
        # domain='sankei_com_sitemap',
        target_start_time_from=datetime.now().astimezone(TIMEZONE) - timedelta(minutes=60),
        target_start_time_to=datetime.now().astimezone(TIMEZONE),
    )
