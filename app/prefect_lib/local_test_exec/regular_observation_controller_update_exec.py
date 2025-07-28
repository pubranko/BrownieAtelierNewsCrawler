# from prefect.testing.utilities import prefect_test_harness
from prefect_lib.flows.regular_observation_controller_update_const import \
    RegularObservationControllerUpdateConst
from prefect_lib.flows.regular_observation_controller_update_flow import \
    regular_observation_controller_update_flow

def test_exec():
    # with prefect_test_harness():

    regular_observation_controller_update_flow(
        register_type=RegularObservationControllerUpdateConst.REGISTER_ADD,
        # register_type=RegularObservationControllerUpdateConst.REGISTER_DELETE,
        spiders_name=[
            # "asahi_com_sitemap",
            # "epochtimes_jp_crawl",
            # "jp_reuters_com_sitemap",
            # "kyodo_co_jp_sitemap",
            # "mainichi_jp_crawl",
            # "nikkei_com_crawl",
            # "yomiuri_co_jp_sitemap",
            "sankei_com_sitemap",
        ],
    )

if __name__ == "__main__":
    test_exec()