from prefect_lib.flows.first_observation_flow import first_observation_flow
from prefect.testing.utilities import prefect_test_harness


def test_exec():
    with prefect_test_harness():
        first_observation_flow()

if __name__ == "__main__":
    test_exec()