import os
import glob
from prefect_lib.flows.scraper_info_uploader_flow import scraper_info_by_domain_flow


scraper_info_by_domain_flow(
    scraper_info_by_domain_files=[
        # "nikkei_com.json",
        # "mainichi_jp.json",
        # "epochtimes_jp.json",
        # 'yomiuri_co_jp.json',
        # 'yomiuri_co_jp_e1.json',
        # 'yomiuri_co_jp_e2.json',
        # 'yomiuri_co_jp_e3.json',
        # 'yomiuri_co_jp_e4.json',
        # 'yomiuri_co_jp_e5.json',
        # 'yomiuri_co_jp_e6.json',
        # 'yomiuri_co_jp_e7.json',
        # 'yomiuri_co_jp.json',
    ],
)

# テスト時のログファイル削除漏れ防止用
for log_file in glob.glob("/tmp/prefect_log_*"):
    print(f"削除漏れlog_file削除: {log_file}")
    os.remove(log_file)
