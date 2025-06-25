import glob
import os
from prefect_lib.flows.scraper_info_uploader_flow import \
    scraper_info_by_domain_flow


def main():
    scraper_info_by_domain_flow(
        scraper_info_by_domain_files=[],
    )

if __name__ == "__main__":
    main()