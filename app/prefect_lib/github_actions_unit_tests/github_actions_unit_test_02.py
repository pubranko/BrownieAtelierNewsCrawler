
if __name__ == "__main__":

    # カレントディレクトリをpythonpathに追加
    import os
    import sys
    current_directory = os.environ.get('PWD')
    if current_directory:
        sys.path.append(current_directory)

    # <4>
    # 手動クローリング
    #   manual_crawling_flow.py
    #   その他を全て指定 (a:産経, b:朝日, c:読売, d:エポック, e:ロイター, f:共同, g:毎日, h:日経)
    from prefect_lib.flows.manual_crawling_flow import manual_crawling_flow
    manual_crawling_flow(
        spider_names=[
            "sankei_com_sitemap",
            "asahi_com_sitemap",
            "yomiuri_co_jp_sitemap",
            "epochtimes_jp_crawl",
            "jp_reuters_com_sitemap",
            "kyodo_co_jp_sitemap",
            "mainichi_jp_crawl",
            "nikkei_com_crawl",
        ],
        spider_kwargs=dict(
            debug=True,
            page_span_from=2,
            page_span_to=2,
            lastmod_term_minutes_from=60,
            lastmod_term_minutes_to=0,
        ),
        following_processing_execution=True  # 後続処理実行(scrapying,news_clip_masterへの登録,solrへの登録)
    )
