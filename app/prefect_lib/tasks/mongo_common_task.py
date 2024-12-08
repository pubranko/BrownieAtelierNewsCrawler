import os
from datetime import date, datetime, time

from dateutil.relativedelta import relativedelta
from prefect import get_run_logger, task
from prefect_lib.flows import START_TIME
from shared.settings import DATA__BACKUP_BASE_DIR, TIMEZONE


@task
def mongo_common_task(
    prefix: str,  # export先のフォルダ名先頭に拡張した名前を付与する。
    suffix: str,  # export先のフォルダ名の末尾に拡張した名前を付与する。
    period_date_from: date,  # 月次エクスポートを行うデータの基準年月
    period_date_to: date,  # 月次エクスポートを行うデータの基準年月
) -> tuple[str, datetime, datetime]:
    """
    mongoDBのコレクションよりimport/exportを行うための前処理を実行する。
    ①import/export先のフォルダ名の生成  ex) prefix_yyyy-mm_yyyy-mm_suffix, prefix_yyyy-mm_yyyy-mm, yyyy-mm_yyyy-mm_suffix, yyyy-mm_yyyy-mm
    ②exportを行う範囲の日時を生成
    """
    logger = get_run_logger()  # PrefectLogAdapter
    logger.info(
        f"=== 引数 : prefix={prefix} suffix={suffix} period_date_from~to= {period_date_from} ~ {period_date_to}"
    )

    period_datetime_from: datetime = datetime.combine(
        period_date_from, time.min, TIMEZONE
    )
    period_datetime_to: datetime = datetime.combine(
        period_date_to, time.max, TIMEZONE
    )

    pre: str = ""
    if prefix:
        pre = prefix + "_"
    suf: str = ""
    if suffix:
        suf = "_" + suffix

    start_period_ymd: str = period_datetime_from.strftime("%Y-%m-%d")
    end_period_ymd: str = period_datetime_to.strftime("%Y-%m-%d")

    folder_name: str = f"{pre}{start_period_ymd}_{end_period_ymd}{suf}"
    dir_path = os.path.join(DATA__BACKUP_BASE_DIR, folder_name)

    logger.info(f"=== 結果 : {dir_path} {period_datetime_from} {period_datetime_to}")

    return dir_path, period_datetime_from, period_datetime_to


"""
保存するフォルダ内の形式

prefix_yyyy-mm_yyyy-mm_suffix
    timestamp
    collection
    collection
    ,,,,
"""
