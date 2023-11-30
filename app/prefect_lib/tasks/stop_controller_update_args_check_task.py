from prefect import task, get_run_logger
from pydantic_core import ValidationError
from prefect_lib.data_models.stop_controller_update_input import StopControllerUpdateInput


@task
def stop_controller_update_args_check_task(
    domain:str, command:str, destination:str
    ) -> StopControllerUpdateInput:
    '''
    ・入力（Flowの引数）のバリデーションチェック。
    ・戻り値: 入力データクラス
    '''
    logger = get_run_logger()   # PrefectLogAdapter
    logger.info(f'=== 引数 : domain= {domain},  command= {command}, destination= {destination}')

    # 入力パラメータのバリデーション
    try:
        stop_controller_update_input = StopControllerUpdateInput(
            domain=domain,
            command=command,
            destination=destination)
    except ValidationError as e:
        logger.error(f'=== バリデーションエラー: {e.errors()}')
        raise ValueError()

    return stop_controller_update_input
