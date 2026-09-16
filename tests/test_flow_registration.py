"""フロー登録をモックし、登録内容と非同期処理の完了待ちを検証する。"""

import sys
import unittest
from contextlib import ExitStack
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, patch

import prefect.settings as prefect_settings
from prefect_lib.deployments.flows_register import main

# 修正前の登録対象・順序・名前・タグ。
EXPECTED = [
    (
        "prefect_lib.flows.manual_crawling_flow",
        "manual_crawling_flow",
        {"name": "manual-crawl-scrape", "tags": ["manual", "crawl-scrape"]},
    ),
    (
        "prefect_lib.flows.manual_scrapying_flow",
        "manual_scrapying_flow",
        {"name": "manual-crawl-scrape", "tags": ["manual", "crawl-scrape"]},
    ),
    (
        "prefect_lib.flows.manual_news_clip_master_save_flow",
        "manual_news_clip_master_save_flow",
        {"name": "manual-crawl-scrape", "tags": ["manual", "crawl-scrape"]},
    ),
    (
        "prefect_lib.flows.first_observation_flow",
        "first_observation_flow",
        {"name": "manual-crawl-scrape", "tags": ["manual", "crawl-scrape"]},
    ),
    (
        "prefect_lib.flows.regular_observation_flow",
        "regular_observation_flow",
        {"name": "auto-crawl-scrape", "tags": ["auto", "daily", "crawl-scrape"]},
    ),
    (
        "prefect_lib.flows.scraper_info_uploader_flow",
        "scraper_info_by_domain_flow",
        {"name": "register", "tags": ["manual", "register"]},
    ),
    (
        "prefect_lib.flows.regular_observation_controller_update_flow",
        "regular_observation_controller_update_flow",
        {"name": "register", "tags": ["manual", "register"]},
    ),
    (
        "prefect_lib.flows.stop_controller_update_flow",
        "stop_controller_update_flow",
        {"name": "register", "tags": ["manual", "register"]},
    ),
    (
        "prefect_lib.flows.crawl_sync_check_flow",
        "crawl_sync_check_flow",
        {"name": "check", "tags": ["manual", "check", "report"]},
    ),
    (
        "prefect_lib.flows.mongo_delete_selector_flow",
        "mongo_delete_selector_flow",
        {"name": "mongodb", "tags": ["manual", "mongodb"]},
    ),
    (
        "prefect_lib.flows.mongo_export_selector_flow",
        "mongo_export_selector_flow",
        {"name": "mongodb", "tags": ["manual", "mongodb"]},
    ),
    (
        "prefect_lib.flows.mongo_import_selector_flow",
        "mongo_import_selector_flow",
        {"name": "mongodb", "tags": ["manual", "mongodb"]},
    ),
    (
        "prefect_lib.flows.stats_info_collect_flow",
        "stats_info_collect_flow",
        {"name": "report", "tags": ["manual", "report"]},
    ),
    (
        "prefect_lib.flows.stats_analysis_report_flow",
        "stats_analysis_report_flow",
        {"name": "report", "tags": ["manual", "report"]},
    ),
    (
        "prefect_lib.flows.scraper_pattern_report_flow",
        "scraper_pattern_report_flow",
        {"name": "report", "tags": ["manual", "report"]},
    ),
    (
        "prefect_lib.flow_nets.morning_flow_net",
        "morning_flow_net",
        {"name": "daily-morning", "tags": ["daily", "morning", "net", "report", "mongodb"]},
    ),
]


class FlowRegistrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.stack = self.enterContext(ExitStack())
        self.flows = []
        self.deployments = []
        modules = {}
        for module_name, flow_name, _ in EXPECTED:
            deployment = SimpleNamespace(aapply=AsyncMock(return_value="deployment-id"))
            flow = SimpleNamespace(ato_deployment=AsyncMock(return_value=deployment))
            module = ModuleType(module_name)
            setattr(module, flow_name, flow)
            modules[module_name] = module
            self.flows.append(flow)
            self.deployments.append(deployment)
        self.stack.enter_context(patch.dict(sys.modules, modules))
        self.settings = SimpleNamespace(home="test-home", api=SimpleNamespace(url="http://example.test/api"))
        self.stack.enter_context(patch.object(prefect_settings, "get_current_settings", return_value=self.settings))
        self.stack.enter_context(patch("decouple.config", return_value="test-pool"))
        self.output = self.stack.enter_context(patch("builtins.print"))

    async def test_all_deployments_are_created_and_applied(self):
        await main()
        self.assertEqual(len(self.flows), 16)
        for flow, deployment, (_, _, options) in zip(self.flows, self.deployments, EXPECTED, strict=True):
            flow.ato_deployment.assert_awaited_once_with(**options, work_pool_name="test-pool")
            deployment.aapply.assert_awaited_once_with()

    async def test_missing_api_url_stops_before_registration(self):
        self.settings.api.url = None
        with self.assertRaisesRegex(ValueError, "PREFECT_API_URL"):
            await main()
        for flow in self.flows:
            flow.ato_deployment.assert_not_called()

    async def test_apply_failure_stops_following_deployments(self):
        self.deployments[0].aapply.side_effect = RuntimeError("registration failed")
        with self.assertRaisesRegex(RuntimeError, "registration failed"):
            await main()
        self.deployments[0].aapply.assert_awaited_once()
        self.flows[1].ato_deployment.assert_not_called()
        self.assertFalse(any("完了" in str(call) for call in self.output.call_args_list))


if __name__ == "__main__":
    unittest.main()
