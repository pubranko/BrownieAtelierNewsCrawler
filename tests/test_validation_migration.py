"""Pydantic V2移行後の省略値・関連項目・入力エラーを検証する。"""

import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

from BrownieAtelierMongo.data_models.scraper_info_by_domain_data import ScraperInfoByDomainData
from GUI.log_viewer_validator import LogViewerValidator
from news_crawl.news_crawl_input import NewsCrawlInput
from prefect_lib.data_models.scraper_pattern_report_input import ScraperPatternReportInput
from prefect_lib.data_models.stop_controller_update_input import StopControllerUpdateInput
from pydantic import ValidationError


class ValidationMigrationTests(unittest.TestCase):
    def test_page_range_default_and_valid_pair(self):
        self.assertIsNone(NewsCrawlInput().page_span_to)
        model = NewsCrawlInput(page_span_from=2, page_span_to=4)
        self.assertEqual((model.page_span_from, model.page_span_to), (2, 4))
        for fields in ({"page_span_from": 2}, {"page_span_to": 4}, {"page_span_from": 4, "page_span_to": 2}):
            with self.subTest(fields=fields), self.assertRaises(ValidationError):
                NewsCrawlInput(**fields)

    def test_lastmod_and_direct_urls(self):
        model = NewsCrawlInput(lastmod_term_minutes_from=60, lastmod_term_minutes_to=10)
        self.assertEqual(model.lastmod_term_minutes_to, 10)
        with self.assertRaises(ValidationError):
            NewsCrawlInput(lastmod_term_minutes_from=10, lastmod_term_minutes_to=60)
        self.assertEqual(
            NewsCrawlInput(direct_crawl_urls=["https://example.test"]).direct_crawl_urls, ["https://example.test"]
        )
        with self.assertRaises(ValidationError):
            NewsCrawlInput(direct_crawl_urls=["not-a-url"])

    def test_gui_date_time_and_integer_level(self):
        model = LogViewerValidator(date_from="20260916", time_from="123456", log_level_value=20)
        self.assertEqual(model.model_dump()["time_from"], "123456")
        self.assertEqual(model.log_level_value, 20)
        for fields in (
            {"date_from": "20260230"},
            {"time_from": "120000"},
            {"time_to": "120000"},
            {"date_to": "20260916", "time_to": "250000"},
            {"log_level_value": "bad"},
        ):
            with self.subTest(fields=fields), self.assertRaises(ValidationError):
                LogViewerValidator.model_validate(fields)

    def test_report_terms(self):
        flows = SimpleNamespace(START_TIME=datetime(2026, 9, 16))
        with patch.dict("sys.modules", {"prefect_lib.flows": flows}):
            from prefect_lib.data_models.stats_analysis_report_input import StatsAnalysisReportInput

        for term in ("daily", "weekly", "monthly", "yearly"):
            model = ScraperPatternReportInput(start_time=datetime(2026, 9, 16), report_term=term)
            self.assertEqual(model.report_term, term)
            stats = StatsAnalysisReportInput(report_term=term, totalling_term=term)
            self.assertEqual(stats.totalling_term, term)
        with self.assertRaises(ValidationError):
            ScraperPatternReportInput(start_time=datetime(2026, 9, 16), report_term="bad")
        with self.assertRaises(ValidationError):
            StatsAnalysisReportInput(report_term="daily", totalling_term="bad")

    def test_stop_controller_choices(self):
        for command in ("add", "delete"):
            for destination in ("crawling", "scrapying"):
                result = StopControllerUpdateInput(domain="example.test", command=command, destination=destination)
                self.assertEqual(result.command, command)
        for command, destination in (("bad", "crawling"), ("add", "bad")):
            with self.assertRaises(ValidationError):
                StopControllerUpdateInput(domain="example.test", command=command, destination=destination)

    def test_scraper_validators_both_run(self):
        for data in ({}, {"domain": 123}, {"domain": "example.test"}, {"domain": "example.test", "scrape_items": {}}):
            with self.subTest(data=data), self.assertRaises(ValidationError):
                ScraperInfoByDomainData(scraper=data)
        data = {
            "domain": "example.test",
            "scrape_items": {
                "title_scraper": [{"pattern": 1, "css_selecter": "title", "priority": 1, "register_date": "2026-09-16"}]
            },
        }
        with patch(
            "BrownieAtelierMongo.data_models.scraper_info_by_domain_data.glob.glob", return_value=["title_scraper.py"]
        ):
            result = ScraperInfoByDomainData(scraper=data)
            self.assertEqual(result.scraper, data)


if __name__ == "__main__":
    unittest.main()
