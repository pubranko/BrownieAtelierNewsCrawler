# カレントディレクトリをpythonpathに追加
import os
import sys
from prefect.testing.utilities import prefect_test_harness
current_directory = os.environ.get('PWD')
if current_directory:
    sys.path.append(current_directory)

def test_exec():
    with prefect_test_harness():
        # <1>
        # 各ニュースサイト別に、スクレイピングの情報を登録する。
        #   scraper_info_uploader_flow.py
        from prefect_lib.flows.scraper_info_uploader_flow import \
            scraper_info_by_domain_flow
        scraper_info_by_domain_flow(scraper_info_by_domain_files=[],)


        # <2>
        # 定期観測用のスパイダーを登録する。
        #   regular_observation_controller_update_flow.py
        #   ３つを指定（a:産経,b:朝日,c:読売）
        from prefect_lib.flows.regular_observation_controller_update_const import \
            RegularObservationControllerUpdateConst
        from prefect_lib.flows.regular_observation_controller_update_flow import \
            regular_observation_controller_update_flow
        regular_observation_controller_update_flow(
            register_type=RegularObservationControllerUpdateConst.REGISTER_ADD,
            spiders_name=[
                "sankei_com_sitemap",
                "asahi_com_sitemap",
                "yomiuri_co_jp_sitemap",
            ],
        )

        # <3>
        # 初回定期観測
        #   first_observation_flow.py
        #   上記のa:産経,b:朝日,c:読売だけが実行されるはず
        from prefect_lib.flows.first_observation_flow import first_observation_flow
        first_observation_flow()

if __name__ == "__main__":
    test_exec()
