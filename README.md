# Brawnie Atlier
## 目次
- [Brawnie Atlier](#brawnie-atlier)
  - [目次](#目次)
  - [システム概要](#システム概要)
  - [当資料の基準日](#当資料の基準日)
  - [主要技術](#主要技術)
  - [システム概要図](#システム概要図)
  - [システム概要図のアクション番号について](#システム概要図のアクション番号について)
  - [プロジェクトで使用されるGitリポジトリ](#プロジェクトで使用されるgitリポジトリ)
  - [プロジェクトで作成・使用するdockerリポジトリ一覧](#プロジェクトで作成使用するdockerリポジトリ一覧)
  - [現在実装済みのFlowの一覧](#現在実装済みのflowの一覧)
  - [定期観測の処理の流れ](#定期観測の処理の流れ)
    - [主要機能である定期観測(regular\_observation\_flow.py)の処理の流れの紹介](#主要機能である定期観測regular_observation_flowpyの処理の流れの紹介)

## システム概要
- 現在、以下のような機能の構築を目指して作成中です。
1. 各種ニュースサイトから情報を収集する。
2. ソーシャルメディアから情報を収集する。
3. 公共機関等が公開しているビックデータを収集する。
4. 収集した情報を全文検索システムに入力し、様々な検索を行えるWEBサービスを提供する。
5. 収集した情報をマイクロソフトのCopilotに分析させ、様々なレポートを生成させWEB上で参照できるサービスを提供する（構想中）。

## 当資料の基準日
2023/11/11時点の情報となります。

## 主要技術
1. 言語(python3.9～)
2. フレームワーク(Prefect2, Scrapy)
3. DB(MongoDB, Apache Solr)
4. Docker, Docker Compose
5. Azure Container Instances
6. Azure Function (Httpトリガー、Timerトリガー、BLOBトリガー)
7. Azure File Storage

## システム概要図
<!-- ![システム概要図](static/システム概要図.jpg) -->
<a href="static/システム概要図.jpg" class="lightbox">
  <img src="static/システム概要図.jpg" width="800" alt="画像の説明">
</a>

## システム概要図のアクション番号について


| №    | デプロイ<br>アクション | 保守運用<br>アクション | 処理実行<br>アクション | アクション内容|
| :--: | :-----: | :-----: | :-----: | :------------------------------------------------------ |
| A1   | 〇      |         |         | 自作したFlowを、Prefect社が運用しているPrefectCloudに登録する。|
|      |         | 〇      |         | PrefectCloudの環境設定、登録した各種Flowを実行、実行結果の参照を行う。|
| A2   | 〇      |         |         | 自作したブラウニー工房系のソースをGitHubにPush（※１）。<br>※１：対象Gitリポジトリーは「BrownieAtelier、BrownieAtelierMongo、BrownieAtelierNotice、BrownieAtelierStorage、BrownieAtelierController」。|
|      | 〇      |         |         | 「ブラウニー工房アプリ」イメージをローカルに作成する際、上記のリポジトリーよりGitクローンを行う（※１）。<br>※１：対象Gitリポジトリーは「BrownieAtelier、BrownieAtelierMongo、BrownieAtelierNotice、BrownieAtelierStorage」。|
| A3   | 〇      |         |         | №A2で作成ローカル上のDockerイメージ「ブラウニー工房アプリ」をDockerHubへPush（※１）する。<br>※１：対象Dockerイメージ名「mikuras/brownie_atelier_app:0.13」|
| A4   | 〇      |         |         | 自作したブラウニー工房コントローラー（※１）のソースをAzureFunctionへデプロイする。<br>※１：対象Gitリポジトリーは「BrownieAtelierController」|
|      |         | 〇      |         | AzureFunctionのHTTPトリガーを使用し、２つのコンテナー（ACI）の作成・削除・起動・停止を行う。|
| A5   | 〇      |         |         | ・暗号化情報ファイル（CA証明書、自己証明書、秘密鍵）を配置<br>・ダイレクトクロール機能を使用する場合、URLのリストファイルを配置<br>・ログインが必要なサイトの場合、そのログイン情報ファイルを配置<br>・ドメイン別スクレイピング情報ファイルを配置|
| A6   |         |         | 〇      | ・タイマー指定があるFlowに対して実行を行うようPrefectAgentに対して通信を行う。<br>・アクション番号A1で手動実行の指示を受けたFlowに対して実行を行うようPrefectAgentに対して通信を行う。|
| A7   |         |         | 〇      | ブラウニー工房Appコンテナー（brownie-atelier-app）を作成・起動する際、DockerHubに登録してあるイメージを取得する。|
| A8   |         |         | ◎      | 【Timerトリガー】<br>ニュースサイトに対する定期観測を行うため、定期的に各コンテナーを起動させる。<br>※現在の計画では、6時、9時、12時、15時、18時、21時、24時に定期観測を行う予定。<br>　ただし朝一のみ不要データの削除なども行うため、若干早めに起動させることを検討中。|
|      |         |         | ◎      | 【BLOBトリガー】<br>ニュースサイトの定期観測が完了した場合、BLOBファイルの再作成が行われる。当BLOBトリガーがそれを検知した場合、コンテナーの停止を行う。|
|      |         |         | 〇      | 【HTTPトリガー】<br>ローカル端末より受けた指示に基づき各コンテナーの作成、起動、停止、削除を行う。|
| A9   |         |         | ◎      | 各ニュースサイトに対してリクエストを行い、ニュース記事を取得する。|
| A10  |         |         | ◎      | 取得したニュースサイトからのレスポンスをMongoDBへ保存する。その際、ログ、各種統計情報も合わせて保存する。|
| A11  |         |         | 〇      | デバックモードでニュースクローラー（Scrapy）を動かした際、参照したサイトマップや巡回先のURLを保存する。|
|      |         |         | 〇      | MongoDBのデータをエクスポートする。またエクスポートしたファイルをインポートする際にも使用する。|
|      |         |         | 〇      | URLを直接指定してクロール（ニュースサイトの巡回）をさせたい場合、そのＵＲＬのリストを保存する。|
|      |         |         | 〇      | 当プロジェクトでは同一ドメインへの多重クロールを禁止している。<br>それを実現する手段としてクロール先のドメイン名でファイルを作成し排他制御をかける。<br>仮に同一ドメインへのクロールが発生しても、ドメイン名のファイルを作成することができなかった場合はクロールを中止する仕組みとなっている。 |
|      |         |         | 〇      | 各ニュースサイトのスクレイピング情報ファイルを読み込む際に使用する。<br>※読み込まれたデータはMongoDBへ保存し、実際のスクレイピング作業時はMongoDBの値を使用する。|
| A12  |         |         | 〇      | MongoDBの暗号化情報ファイル（CA証明書、自己証明書、秘密鍵）を参照する。|
| A13  |         |         | 〇      | MongoDBのデータはコンテナー内に保存できないため、実際のデータはAzureFileStorageに保存する。|
| A14  |         |         | ◎      | ニュースサイトの定期観測が完了した場合、BLOBファイルを再作成する。<br>※当BLOBトリガーがそれを検知した場合、コンテナーの停止を行う。|
| A15  |         |         | ◎      | BLOBファイルの更新有無を監視している。|
| A16  |         |         | ◎      | 各Flowでエラーが発生した場合、メールを送信する。|
| A17  |         |         | ◎      | ローカル端末にメールを送信する。|

## プロジェクトで使用されるGitリポジトリ

| №   | リポジトリ名             | メイン | サブ | 概要                                                                                                                                                                                                                                   | gitリポジトリー                                          |
| :-- | :----------------------- | :--: | :--: | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------- |
| 1   | BrownieAtelier<br>ブラウニーアトリエ           | 〇     |      | ブラウニー工房の主要ブランチ。<br>各種処理の実行には、フレームワーク：Prefect2のFlowを使用。<br>クローリングには、フレームワーク：Scrapyを使用。<br>スクレイピングには、上記フレームワークを使用せずbeautiful soupライブラリーを使用。 | https://github.com/pubranko/BrownieAtelier.git|
| 2   | BrownieAtelierController<br>ブラウニーアトリエコントローラー | 〇     |      | AzureFunctionにより、ブラウニー工房を格納しているコンテナ、MongoDBコンテナの起動・停止を行う。| https://github.com/pubranko/BrownieAtelierController.git |
| 3   | BrownieAtelierMongo<br>ブラウニーアトリエモンゴDB      |        | 〇   | Gitサブモジュール。MongoDBコンテナへの接続、参照、更新を行う専用モジュール。| https://github.com/pubranko/BrownieAtelierMongo.git      |
| 4   | BrownieAtelierNotice<br>ブラウニーアトリエ通知     |        | 〇   | Gitサブモジュール。メールによる送信機能。エラー発生時の通知用モジュール。| https://github.com/pubranko/BrownieAtelierNotice.git     |
| 5   | BrownieAtelierStorage<br>ブラウニーアトリエストレージ    |        | 〇   | Gitサブモジュール。AzureStoregeへの接続、参照、更新を行う専用モジュール。| https://github.com/pubranko/BrownieAtelierStorage.git    |

## プロジェクトで作成・使用するdockerリポジトリ一覧
| №   | イメージ名                       | コンテナー名        | イメージリポジトリ                          |                                                       |
| :-- | :------------------------------- | :------------------ | :------------------------------------------ | :---------------------------------------------------- |
| 1   | mikuras/brownie_atelier_app:0.13 | Brownie-atelier-app | https://hub.docker.com/repositories/mikuras | ブラウニー工房のアプリをUbuntu20.04へ格納したイメージ<br>※プライベートリポジトリ |
| 2   | mongo:5.0.10-focal               | mongo-azure-db      | https://hub.docker.com/_/mongo              | MongoDB公式イメージ                                   |
|     |                                  |                     |                                             |                                                       |

## 現在実装済みのFlowの一覧

| №          | Flow一覧                                      | 処理概要|
| :--------- | :-------------------------------------------- | :---------|
|            | ＜各種登録系＞                                 ||
| Register-1 | regular_observation_controller_update_flow.py | 定期観測用のスパイダーを登録する。<br>定期観測に使用しないスパイダーはここでは登録しない。<br>登録先MongoDBコレクション(controller)|
| Register-2 | scraper_info_uploader_flow.py                 | 各ニュースサイト別に、スクレイピングの情報を登録する。|
| Register-3 | stop_controller_update_flow.py                | 各ニュースサイト別に、定期観測クローリングのON/OFF、スクレイピングのON/OFF指定を登録する。|
|            | <ニュースクロール・スクレイピング系＞            ||
| Crawl-1    | first_observation_flow.py                     | 定期観測対象のスパイダーでまだ一度も定期観測していないスパイダーのみ実行する。|
| Crawl-2    | regular_observation_flow.py                   | 定期観測対象のスパイダーを実行する。<br>※対象のスパイダーは、上述「Register-1」で登録する。|
| Crawl-3    | manual_crawling_flow.py                       | 手動でクローリングを行う際、必要な引数を与えて実行します。|
| Crawl-4    | manual_scrapying_flow.py                      | 手動でスクレイピングを行う際、必要な引数を与えて実行します。|
| Crawl-5    | manual_news_clip_master_save_flow.py          | 手動でニュースクリップマスターへ保存を行う際、必要な引数を与えて実行します。|
|            | ＜MongoDB更新系＞                              ||
| Mongo-1    | mongo_delete_selector_flow.py                 | mongodbの各種コレクションに対して、指定したデータを削除する。|
| Mongo-2    | mongo_export_selector_flow.py                 | mongodbの各種コレクションに対して、指定したデータをエクスポートする。|
| Mongo-3    | mongo_import_selector_flow.py                 | mongodbの各種コレクションに対して、指定したデータをインポートする。|
|            | ＜レポート系＞                                 ||
| Report-1   | scraper_pattern_report_flow.py                | 各ニュースサイト別のスクレイピング結果より、使われたパターンの統計情報をExcelで作成しメールにて送信する。|
| Report-2   | stats_info_collect_flow.py                    | 各ニュースサイトをクローリングした際、フレームワーク：Scrapyでは統計情報を出力している。<br>その統計をMongoDBに保存させているため、情報を扱いやすいように加工したデータを日付別に保存する。|
| Report-3   | stats_analysis_report_flow.py                 | 各ニュースサイトをクローリングした際、フレームワーク：Scrapyでは統計情報を出力している。<br>その統計をMongoDBに保存させているため、それを使いレポートをExcelで作成しメールにて送信する。|
|            | ＜チェック系＞                                 ||
| Check-1    | crawl_sync_check_flow.py                      | クロール対象となったURLとクローラーレスポンス（crawler_response）の同期が取れているかチェック。<br>クローラーレスポンス（crawler_response）とニュースクリップマスター（news_clip_master）の同期が取れているかチェック。 |

## 定期観測の処理の流れ
### 主要機能である定期観測(regular\_observation\_flow.py)の処理の流れの紹介
<!-- ![定期観測の処理の流れ](static/定期観測の処理の流れ.jpg) -->

<a href="static/定期観測の処理の流れ.jpg">
  <img src="static/定期観測の処理の流れ.jpg" width="800" alt="定期観測の処理の流れ">
</a>

