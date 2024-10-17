from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from prefect import flow, get_run_logger
from prefect.futures import PrefectFuture
from prefect_lib.flows.init_flow import init_flow
from prefect_lib.tasks.end_task import end_task
from prefect_lib.tasks.init_task import init_task
from prefect_lib.tasks.scraper_info_by_domain_task import scraper_info_by_domain_task

"""
mongoDBのインポートを行う。
・pythonのlistをpickle.loadsで復元しインポートする。
・対象のコレクションを選択できる。
・対象の年月を指定できる。範囲を指定した場合、月ごとにエクスポートを行う。
"""


@flow(
    name="Scraper info uploader flow",
    flow_run_name="Scraper info uploader flow run",
)
# @common_flow
def scraper_info_by_domain_flow(scraper_info_by_domain_files: list = []):
    init_flow()

    # ロガー取得
    logger = get_run_logger()  # PrefectLogAdapter
    # 初期処理
    init_task_instance: PrefectFuture = init_task.submit()
    # 実行結果が返ってくるまで待機し、戻り値を保存。 
    #   ※タスクのステータスをresultを受け取る前に判定してもPendingとなる。インスタンスのステータスはリアルタイムで更新されているので注意。
    init_task_result = init_task_instance.result()

    if init_task_instance.state.is_completed():
        mongo: MongoModel = init_task_result

        try:
            scraper_info_by_domain_task(scraper_info_by_domain_files, mongo)
        except Exception as e:
            # 例外をキャッチしてログ出力等の処理を行う
            logger.error(f"=== {e}")
        finally:
            # 後続の処理を実行する
            end_task(mongo)
    else:
        logger.error(f"=== init_taskが正常に完了しなかったため、後続タスクの実行を中止しました。")
