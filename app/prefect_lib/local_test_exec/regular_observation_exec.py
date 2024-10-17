from prefect.testing.utilities import prefect_test_harness
from prefect_lib.flows.regular_observation_flow import regular_observation_flow


def test_exec():
    with prefect_test_harness():

        regular_observation_flow()

if __name__ == "__main__":
    test_exec()