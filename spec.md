## 1. 概要
本ドキュメントは、Microsoft Entra ID（旧Azure AD）から出力されるサインインログを、分析および長期保存を目的として PostgreSQL 18 データベースへ集約するバッチ処理の仕様を定義する。
1日数万件を超える大規模データを効率的に処理するため、PostgreSQLの「COPYプロトコル」と「宣言的パーティショニング」を活用する。また、将来の仕様変更やテスト要件に柔軟に対応できるよう、**クリーンアーキテクチャ**に基づいた設計を採用する。

## 2. 技術スタック
* **実行環境**: Python 3.10+
* **データベース**: PostgreSQL 18
* **主要ライブラリ**:
    * `msal`: Microsoft 認証 (OAuth 2.0 Client Credentials Flow)
    * `psycopg` (Version 3): PostgreSQL 18対応、高速COPYプロトコル利用
    * `requests`: Graph API HTTPリクエスト
* **認証方式**: Azure サービスプリンシパル（クライアントシークレット）

## 3. プロジェクト構成（クリーンアーキテクチャ）
依存関係が「外側（インフラ・外部API）」から「内側（ビジネスロジック）」へ向かうように層（レイヤー）を分割し、保守性とテスト容易性を高める。

### 3.1 ディレクトリ構成
```text
entra_signin_etl/
├── .env.example                # 環境変数のサンプル（テナントID, DB接続情報など）
├── requirements.txt            # 依存ライブラリ
├── main.py                     # エントリポイント（各クラスを生成し、処理を開始する）
├── tests/                      # テストコードディレクトリ
└── src/
    ├── domain/                 # 【第1層: エンティティ】
    │   ├── __init__.py
    │   ├── models.py           # SignInLog, BatchStatus などのデータクラス
    │   └── exceptions.py       # ドメイン固有の例外クラス
    │
    ├── application/            # 【第2層: ユースケース】
    │   ├── __init__.py
    │   ├── etl_use_case.py     # 抽出(E)→変換(T)→ロード(L)のフロー制御
    │   └── interfaces/         # 外部連携用のインターフェース（抽象クラス）
    │       ├── api_port.py     # Graph APIクライアントのインターフェース
    │       └── db_port.py      # DBリポジトリのインターフェース
    │
    ├── adapters/               # 【第3層: インターフェースアダプター】
    │   ├── __init__.py
    │   ├── graph_api_client.py # api_portの実装 (requestsを使用)
    │   └── postgres_repo.py    # db_portの実装 (psycopg3を使用、COPY/UPSERT実行)
    │
    └── infrastructure/         # 【第4層: フレームワークとドライバ】
        ├── __init__.py
        ├── config.py           # 環境変数の読み込みと設定管理
        ├── database.py         # DBコネクションプールの管理
        ├── auth.py             # MSALを用いた認証トークン取得ロジック
        └── logger.py           # ログ出力の設定
```

### 3.2 各レイヤーの役割
1. domain (エンティティ層): 外部ライブラリに依存しない純粋なPythonコード。APIから取得したデータを保持するモデルや、データ型変換（JSONからUUID/datetime等への変換）のコアビジネスルールをカプセル化する。
1. application (ユースケース層): バッチ処理の進行（APIから取得→変換→DBへ保存）を制御する。具体的なSQLやHTTPリクエストは記述せず、interfaces に定義した抽象クラスを呼び出す。
1. adapters (アダプター層): ユースケースの指示を受け、実際に外部システム（Graph APIやPostgreSQL）と通信する。ページネーション処理やDBの高速ロード（COPY）ロジックはここに実装する。
1. infrastructure (インフラストラクチャ層): データベースの接続管理、環境変数読み込み、MSALによるアクセストークン取得など、システムを動かすための土台を提供する。

## 4. データベース設計

### 4.1 親テーブル: `entra_signin_logs`
`created_at` をパーティションキーとし、月単位でレンジ分割を行う。

```sql
-- 親テーブル
CREATE TABLE entra_signin_logs (
    id UUID NOT NULL,                        -- Entra IDのGUID
    created_at TIMESTAMPTZ NOT NULL,         -- ログ生成日時(UTC)
    user_principal_name VARCHAR(255),        -- ユーザUPN
    display_name VARCHAR(255),               -- ユーザ名
    app_display_name VARCHAR(255),           -- アプリ名
    ip_address INET,                         -- 接続元IP（IPv4/v6両対応）
    status_success BOOLEAN,                  -- 成功/失敗フラグ
    failure_reason TEXT,                     -- 失敗理由（エラーコード等）
    raw_data JSONB,                          -- レスポンス全データ
    fetched_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, -- 取り込み日時
    
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- インデックス
CREATE INDEX idx_signin_created_at ON entra_signin_logs (created_at);
CREATE INDEX idx_signin_upn ON entra_signin_logs (user_principal_name);
CREATE INDEX idx_signin_raw_data ON entra_signin_logs USING GIN (raw_data);

-- デフォルトパーティション（範囲外データの一時受け用）
CREATE TABLE entra_signin_logs_default PARTITION OF entra_signin_logs DEFAULT;
```

### 4.2 管理用テーブル: batch_status

```sql
CREATE TABLE batch_status (
    process_name VARCHAR(50) PRIMARY KEY,
    last_scanned_at TIMESTAMPTZ NOT NULL
);
```

## 5. 処理フロー詳細

### 5.1 抽出 (Extract)
1. 前回実行時刻の取得: batch_status より last_scanned_at を取得。
1. APIリクエスト: Microsoft Graph API (/auditLogs/signIns) を呼び出す。
    * フィルタ: createdDateTime gt {last_scanned_at}
    * 並び替え: createdDateTime asc
    * 反映遅延を考慮し、前回取得時刻を5分程度ラップさせて取得する。
1. ページネーション: @odata.nextLink が存在する限り、全件を走査する。

### 5.2 変換 (Transform)
* APIレスポンス（JSON）を、DBカラムに対応するPythonのタプルまたは辞書形式に変換。
* id は文字列から UUID 型へ、createdDateTime は ISO8601形式から UTC の datetime オブジェクトへ変換。

### 5.3 高速ロード (Load) - 一括登録ロジック
大量件数（数万件〜）を登録するため、INSERT ではなく COPY プロトコルを使用する。

1. ワークテーブルの作成:
    * CREATE TEMP TABLE tmp_logs (LIKE entra_signin_logs INCLUDING ALL) ON COMMIT DROP;
1. 一括データ転送:
    * psycopg.Cursor.copy() を用い、Python上の全データを tmp_logs へストリーム転送。
1. 重複排除マージ:
```sql
INSERT INTO entra_signin_logs 
SELECT * FROM tmp_logs 
ON CONFLICT (id, created_at) DO NOTHING;
```

## 6. 運用管理

### 6.1 パーティションの自動メンテナンス
* バッチ処理の冒頭で、「翌月分のパーティション」の有無を確認。
* 存在しない場合は CREATE TABLE entra_signin_logs_y2026m04 PARTITION OF entra_signin_logs FOR VALUES FROM (...) TO (...); を自動発行する。

### 6.2 エラー処理
* APIレート制限: HTTP 429 エラー時、ヘッダの Retry-After 秒数に従い待機。
* リトライロジック: ネットワークエラー等の不時停止時、最大3回まで再試行を行う。
* トランザクション: データのロードから batch_status の更新までを一連のトランザクションとして管理し、失敗時はロールバックする。

## 7. セキュリティ
* 認証情報の秘匿化: 環境変数、またはシークレット管理サービス（Azure Key Vault等）を利用。
* DBアクセス: 最小権限の原則に基づき、本テーブルへの DML(SELECT/INSERT) およびパーティション作成に必要な DDL(CREATE) 権限のみを付与。