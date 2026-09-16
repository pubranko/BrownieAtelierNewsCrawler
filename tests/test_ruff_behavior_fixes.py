"""GUIや外部サービスを起動せず、lint修正に伴う動作を検証する。"""

import ast
import runpy
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from models.solr_news_clip_model import SolrNewsClip

ROOT = Path(__file__).resolve().parents[1]


def isolated_function(path, name, namespace):
    """副作用のあるモジュールから対象関数だけを読み込む。"""
    tree = ast.parse((ROOT / path).read_text())
    function = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == name)
    function.decorator_list = []
    module = ast.Module(body=[function], type_ignores=[])
    exec(compile(module, str(path), "exec"), namespace)
    return namespace[name]


class RuffBehaviorTests(unittest.TestCase):
    def test_solr_defaults_and_explicit_empty_sort(self):
        model = object.__new__(SolrNewsClip)
        model.solr = Mock()
        model.logger = Mock()
        model.search_query(["*:*"], sort={}, field=["title"], facet_field=["issuer"])
        self.assertEqual(model.solr.search.call_args.kwargs["sort"], "")
        self.assertEqual(model.solr.search.call_args.kwargs["fl"], "title")
        model.search_query(["*:*"])
        self.assertEqual(model.solr.search.call_args.kwargs["sort"], "response_time desc,")
        self.assertEqual(model.solr.search.call_args.kwargs["fl"], "")
        self.assertEqual(model.solr.search.call_args.kwargs["facet_field"], "")

    def test_flow_default_list_is_fresh_each_call(self):
        seen = []

        def upload(files, mongo):
            seen.append(files.copy())
            files.append("changed")

        task = Mock()
        task.submit.return_value.state.is_completed.return_value = True
        namespace = {
            "init_flow": Mock(),
            "get_run_logger": Mock(),
            "init_task": task,
            "scraper_info_by_domain_task": upload,
            "end_task": Mock(),
        }
        function = isolated_function(
            "app/prefect_lib/flows/scraper_info_uploader_flow.py", "scraper_info_by_domain_flow", namespace
        )
        function()
        function()
        self.assertEqual(seen, [[], []])

    def test_copy_callbacks_keep_their_own_record_value(self):
        tree = ast.parse((ROOT / "app/GUI/log_viewer.py").read_text())
        callback = next(
            node for node in ast.walk(tree) if isinstance(node, ast.Lambda) and "pyperclip.copy" in ast.unparse(node)
        )
        copier = Mock()
        namespace: dict[str, object] = {"pyperclip": SimpleNamespace(copy=copier)}
        callbacks = []
        for value in ("first", "second"):
            namespace["record_value"] = value
            callbacks.append(eval(compile(ast.Expression(callback), "callback", "eval"), namespace))
        for function in callbacks:
            function()
        self.assertEqual([call.args[0] for call in copier.call_args_list], ["first", "second"])

    def test_reactor_is_installed_before_it_is_imported(self):
        events = []
        reactor = object()
        installer = SimpleNamespace(install=lambda: events.append("install"))

        def import_module(name):
            events.append(name)
            return installer if name.endswith("asyncioreactor") else reactor

        with patch("importlib.import_module", side_effect=import_module), patch.dict(sys.modules):
            result = runpy.run_path(str(ROOT / "app/prefect_lib/reactor_setup.py"))
        self.assertEqual(events, ["twisted.internet.asyncioreactor", "install", "twisted.internet.reactor"])
        self.assertIs(result["reactor"], reactor)


if __name__ == "__main__":
    unittest.main()
