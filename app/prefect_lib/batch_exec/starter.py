import os
import yaml
import datetime
import logging
from shared.settings import DATA__INFORMATION_ON_SCHEDULED_DIR
import importlib.util
import importlib
from typing import cast
from importlib.machinery import ModuleSpec

# prefectロガー配下の当ファイル名でloggerを作成
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(f"prefect.{__name__}")


def load_schedule_dict() -> dict:
    """ スケジュール情報を読み込み、辞書形式で返す
    Returns:
        dict: スケジュール情報の辞書
    """
    # YAMLファイルから辞書を取得
    schedule_path = os.path.join(DATA__INFORMATION_ON_SCHEDULED_DIR, "starter_flows.yml")
    with open(schedule_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    
    # スケジュール情報の読み込み
    schedule_dict: dict = load_schedule_dict()
    # 現在の時間を取得して、スケジュールに基づいたフロー情報＆パラメータを取得
    hour = datetime.datetime.now().hour
    flow_configs = schedule_dict.get(hour, schedule_dict.get("default", []))
    if not isinstance(flow_configs, list):
        flow_configs = [flow_configs]

    for config in flow_configs:
        flow_path = config.get("flow", "")
        params = config.get("params")
        if params is None:
            params = {}

        logger.info(f"実行対象フロー: {flow_path}")
        logger.info(f"パラメータ情報: {params}")

        # flow_pathは.pyファイルのみを想定
        flow_file = os.path.join(os.getcwd(), flow_path)
        module_name = os.path.splitext(os.path.basename(flow_file))[0]
        spec = importlib.util.spec_from_file_location(module_name, flow_file)
        spec = cast(ModuleSpec, spec)
        if spec is None or spec.loader is None:
            logger.warning(f"specまたはloaderがNoneです: {flow_path}")
            continue
        flow_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(flow_module)
        if hasattr(flow_module, "main"):
            flow_module.main(**params)
        elif hasattr(flow_module, "run"):
            flow_module.run(**params)
        else:
            logger.warning(f"{flow_path}にmainまたはrun関数がありません")


if __name__ == "__main__":
    main()