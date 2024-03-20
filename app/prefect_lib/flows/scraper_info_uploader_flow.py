import glob
import json
import logging
import os
from logging import Logger
from typing import Any

from BrownieAtelierMongo.collection_models.mongo_model import MongoModel
from BrownieAtelierMongo.collection_models.scraper_info_by_domain_model import \
    ScraperInfoByDomainModel
from BrownieAtelierMongo.data_models.scraper_info_by_domain_data import \
    ScraperInfoByDomainConst
from prefect import flow, get_run_logger, task
from prefect.futures import PrefectFuture
from prefect.states import State
from prefect.task_runners import SequentialTaskRunner
# from prefect_lib.flows.common_flow import common_flow
from prefect_lib.flows.init_flow import init_flow
from prefect_lib.tasks.end_task import end_task
from prefect_lib.tasks.init_task import init_task
from pydantic import ValidationError
from shared.settings import DATA__SCRAPER_INFO_BY_DOMAIN_DIR

"""
mongoDBのインポートを行う。
・pythonのlistをpickle.loadsで復元しインポートする。
・対象のコレクションを選択できる。
・対象の年月を指定できる。範囲を指定した場合、月ごとにエクスポートを行う。
"""


@task
def scraper_info_by_domain_task(scraper_info_by_domain_files: list, mongo: MongoModel):
    logger = get_run_logger()  # PrefectLogAdapter
    logger.info(f"=== 引数: {scraper_info_by_domain_files}")

    scraper_info_by_domain_model = ScraperInfoByDomainModel(mongo)

    get_files: list = []
    if len(scraper_info_by_domain_files) == 0:
        path = os.path.join(DATA__SCRAPER_INFO_BY_DOMAIN_DIR, "*.json")
        get_files = glob.glob(path)
        if len(get_files) == 0:
            # raise ENDRUN(state=state.Failed())
            raise IOError(
                f"対象ディレクトリにファイルが見つかりませんでした。ディレクトリにファイルを格納してください。 (ディレクトリ= {DATA__SCRAPER_INFO_BY_DOMAIN_DIR})"
            )
        else:
            logger.info(f"=== ファイル指定なし → 全ファイル対象 : {get_files}")
    else:
        for file in scraper_info_by_domain_files:
            file_path = os.path.join(DATA__SCRAPER_INFO_BY_DOMAIN_DIR, file)
            if os.path.exists(file_path):
                raise IOError(
                    f"対象ディレクトリにファイルが見つかりませんでした。ファイル名に誤りがある可能性があります。 (ディレクトリ= {DATA__SCRAPER_INFO_BY_DOMAIN_DIR}, ファイル名= {file})"
                )
            get_files.append(file_path)

    for file_path in get_files:
        logger.info(f"=== ファイルチェック : {file_path}")
        with open(file_path, "r") as f:
            file = f.read()

        scraper_info: dict = json.loads(file)

        try:
            scraper_info_by_domain_model.data_check(scraper=scraper_info)
        except ValidationError as e:
            error_info: list = e.errors()
            logger.error(f'=== エラー({file_path}) : {error_info[0]["msg"]}')
        else:
            scraper_info_by_domain_model.update_one(
                filter={
                    ScraperInfoByDomainConst.DOMAIN: scraper_info[
                        ScraperInfoByDomainConst.DOMAIN
                    ]
                },
                record={"$set": scraper_info},
            )
            logger.info(f"=== 登録完了 : {file_path}")

        # 処理の終わったファイルオブジェクトを削除
        del file, scraper_info


@flow(
    name="Scraper info uploader flow",
    flow_run_name="Scraper info uploader flow run",
    task_runner=SequentialTaskRunner(),
)
# @common_flow
def scraper_info_by_domain_flow(scraper_info_by_domain_files: list = []):
    init_flow()

    # ロガー取得
    logger = get_run_logger()  # PrefectLogAdapter
    # 初期処理
    init_task_result: PrefectFuture = init_task.submit()

    any: Any = init_task_result.get_state()
    state: State = any
    if state.is_completed():
        mongo = init_task_result.result()

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
