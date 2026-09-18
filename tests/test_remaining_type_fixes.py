"""空の設定、スクレイプ条件、結合セル、pandas集計の回帰テスト。"""

import importlib
import unittest
from unittest.mock import Mock, mock_open, patch

import pandas as pd
from bs4 import BeautifulSoup
from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet
from prefect_lib.batch_exec import starter, test_starter
from prefect_lib.data_models.scraper_pattern_report_data import ScraperPatternReportData
from prefect_lib.data_models.scraper_pattern_report_excel import ScraperPatternReportExcel
from prefect_lib.data_models.stats_analysis_report_excel import StatsAnalysisReportExcel
from prefect_lib.data_models.stats_info_collect_data import StatsInfoCollectData


class SlackResponseTests(unittest.TestCase):
    def test_missing_message_fails_before_attachment_or_other_sends(self):
        module = importlib.import_module("BrownieAtelierNotice.test_slack_notice")
        with patch.object(module, "WebClient") as client_class, patch("builtins.open") as open_file:
            client = client_class.return_value
            client.chat_postMessage.return_value = {"message": None}
            with self.assertRaisesRegex(AssertionError, "message"):
                module.slack_notice_test()
            open_file.assert_not_called()
            client.files_upload_v2.assert_not_called()


class ScheduleTests(unittest.TestCase):
    def test_empty_yaml_is_empty_schedule(self):
        for module in (starter, test_starter):
            with self.subTest(module=module.__name__), patch("builtins.open", mock_open(read_data="")):
                self.assertEqual(module.load_schedule_dict(), {})

    def test_null_flow_does_not_import_or_execute(self):
        for module in (starter, test_starter):
            with (
                self.subTest(module=module.__name__),
                patch.object(module, "load_schedule_dict", return_value={"default": None}),
                patch.object(module.importlib.util, "spec_from_file_location") as load,
            ):
                module.main()
                load.assert_not_called()

    def test_invalid_yaml_and_flow_are_rejected(self):
        for module in (starter, test_starter):
            with patch("builtins.open", mock_open(read_data="- invalid")), self.assertRaises(ValueError):
                module.load_schedule_dict()
            with patch.object(module, "load_schedule_dict", return_value={"default": [42]}):
                with self.assertRaises(ValueError):
                    module.main()

    def test_valid_schedule_executes_with_parameters(self):
        for module in (starter, test_starter):
            flow = Mock()
            with (
                patch.object(
                    module, "load_schedule_dict", return_value={"default": {"flow": "flow.py", "params": {"n": 2}}}
                ),
                patch.object(module.importlib.util, "spec_from_file_location"),
                patch.object(module.importlib.util, "module_from_spec", return_value=flow),
            ):
                module.main()
                flow.main.assert_called_once_with(n=2)


class ScraperTests(unittest.TestCase):
    def test_integer_pattern_and_string_selector(self):
        cases = [
            ("title_scraper", "title", "<title>Title</title>", "title", "Title"),
            ("article_scraper", "p", "<p>First</p><p>Second</p>", "article", "First\nSecond"),
            ("publish_date_scraper", "meta", '<meta content="2026-09-15T10:00:00+09:00">', "publish_date", None),
        ]
        for name, selector, html, key, expected in cases:
            module = importlib.import_module(f"prefect_lib.scraper.{name}")
            params: list[dict[str, str | int]] = [{"pattern": 2, "css_selecter": selector}]
            with self.subTest(name=name):
                result, pattern = module.scraper(BeautifulSoup(html, "html.parser"), name, params)
                self.assertEqual(pattern, {name: 2})
                if expected is None:
                    self.assertEqual(result[key].isoformat(), "2026-09-15T10:00:00+09:00")
                else:
                    self.assertEqual(result[key], expected)
                with self.assertRaises(TypeError):
                    module.scraper(BeautifulSoup(html, "html.parser"), name, [{"pattern": 2, "css_selecter": 123}])


class ExcelTests(unittest.TestCase):
    def test_column_width_with_merged_first_row(self):
        report = object.__new__(ScraperPatternReportExcel)
        worksheet = Workbook().active
        assert isinstance(worksheet, Worksheet)
        report.worksheet = worksheet
        report.worksheet.merge_cells("A1:B1")
        report.worksheet["A1"] = "Heading"
        report.worksheet["B3"] = "Detail"
        data = object.__new__(ScraperPatternReportData)
        data.result_df = pd.DataFrame(columns=[x[report.COL] for x in report.SCRAPER_PATTERN_ANALYSIS_COLUMNS_INFO])
        report.scraper_pattern_report_body(data)
        self.assertEqual(report.worksheet.column_dimensions["B"].width, 11)

        stats = object.__new__(StatsAnalysisReportExcel)
        stats.worksheet_1 = report.worksheet
        stats.stats_analysis_report_body(pd.DataFrame(columns=["aggregate_base_term"]), pd.Series(dtype=str))
        self.assertAlmostEqual(stats.worksheet_1.column_dimensions["B"].width, 8.07)


class AggregationTests(unittest.TestCase):
    def test_aggregations_preserve_values_rounding_and_groups(self):
        model = StatsInfoCollectData()
        frame = pd.DataFrame({"spider_name": ["a", "a", "b"], "value": [1.111, 2.222, 4.0]})
        results = {key: pd.DataFrame() for key in ("sum", "mean", "min", "max")}
        model.aggregate_result_set(frame, ["spider_name"], "2026-09-15", results)
        for operation, expected in {"sum": 3.33, "mean": 1.67, "min": 1.11, "max": 2.22}.items():
            self.assertEqual(results[operation].iloc[0]["value"], expected)
            self.assertEqual(results[operation].iloc[1]["value"], 4.0)
            self.assertEqual(results[operation].iloc[0][model.AGGREGATE_BASE_TERM], "2026-09-15")
        empty = frame.iloc[:0]
        assert isinstance(empty, pd.DataFrame)
        model.aggregate_result_set(empty, ["spider_name"], "2026-09-16", results)
        self.assertTrue(all(len(result) == 2 for result in results.values()))

    def test_full_analysis_includes_empty_period(self):
        from datetime import datetime

        model = StatsInfoCollectData()
        common = {"start_time": ["2026-09-14 15:00", "2026-09-15 14:59"], "spider_name": ["a", "a"]}
        model.robots_df = pd.DataFrame({**common, model.ROBOTS_RESPONSE_STATUS: [200, 200], model.COUNT: [1, 3]})
        model.downloader_df = pd.DataFrame(
            {**common, model.DOWNLOADER_RESPONSE_STATUS: [200, 200], model.COUNT: [2, 4]}
        )
        model.spider_df = pd.DataFrame({**common, "value": [2.0, 4.0]})
        result = model.stats_analysis_exec(
            [
                (datetime(2026, 9, 15), datetime(2026, 9, 15, 23, 59, 59)),
                (datetime(2026, 9, 16), datetime(2026, 9, 16, 23, 59, 59)),
            ]
        )
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]["value"], 6.0)
        self.assertEqual(result.iloc[0]["value_mean"], 3.0)
        self.assertEqual(result.iloc[0]["value_min"], 2.0)
        self.assertEqual(result.iloc[0]["value_max"], 4.0)
        self.assertEqual(result.iloc[1]["value"], "")

    def test_datetime_index_preserves_japan_time_and_inclusive_slice(self):
        model = StatsInfoCollectData()
        frame = pd.DataFrame(
            {"start_time": ["2026-09-14 15:00", "2026-09-15 14:59", "2026-09-15 15:00"], "value": [1, 2, 3]}
        )
        indexed = model.date_time_set_index("start_time", frame)
        assert isinstance(indexed.index, pd.DatetimeIndex)
        self.assertEqual(str(indexed.index.tz), "Asia/Tokyo")
        selected = indexed["2026-09-15 00:00:00":"2026-09-15 23:59:59"]
        assert isinstance(selected, pd.DataFrame)
        values = selected["value"]
        assert isinstance(values, pd.Series)
        self.assertEqual(values.tolist(), [1, 2])


if __name__ == "__main__":
    unittest.main()
