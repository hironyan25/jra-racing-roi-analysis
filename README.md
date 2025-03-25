# JRA競馬データ分析：回収率向上プロジェクト

## プロジェクト概要

このプロジェクトは、JRA（日本中央競馬会）のレースデータを分析し、単なる的中率ではなく**回収率（ROI）向上**に特化した予測モデルを構築することを目的としています。一般的な予測モデルでは見落とされがちな「人気以上に好走する条件」を数値化することで、馬券の期待値を高める特徴量を開発しています。

## 特徴量と分析手法

本プロジェクトでは、以下の主要な特徴量に焦点を当てて分析しています：

1. **種牡馬×馬場適性ROI**：特定の種牡馬の産駒が特定の馬場条件で示す回収率
2. **騎手のコース別平均配当**：騎手が特定コースで勝った際の平均配当（穴をあける騎手の特定）
3. **上がりタイム順位と回収率**：レース終盤の上がりタイムと回収率の関係
4. **前走ペース偏差（展開不利指標）**：前走での不利な展開を数値化
5. **馬のコース実績ROI**：馬の特定コースでの回収率実績
6. **スタミナ指数**：長距離レースにおけるスタミナ評価指標

## データベース構造

JVDデータベース（JRA-VAN Data Lab）の以下のテーブルを活用しています：

- **jvd_ra**: レース基本情報（競馬場、日付、距離、馬場状態など）
- **jvd_se**: 出走馬情報（着順、タイム、騎手、馬番など）
- **jvd_hr**: レース払戻情報（単勝、複勝など各種配当）
- **jvd_um**: 馬基本情報（血統など）
- **jvd_hn**: 血統・繁殖馬情報

## ディレクトリ構造

```
jra-racing-roi-analysis/
├── config/                   # 設定ファイル
│   ├── config.py             # 基本設定
│   └── db_config.py          # データベース接続設定
├── data/                     # データ処理
│   ├── db_connector.py       # データベース接続
│   ├── data_loader.py        # データロード処理
│   └── query_repository.py   # SQLクエリ集
├── preprocessing/            # 前処理
│   ├── cleaning.py           # データクリーニング
│   └── transformation.py     # データ変換
├── features/                 # 特徴量エンジニアリング
│   ├── sire_track_features.py  # 種牡馬×馬場適性
│   ├── jockey_features.py      # 騎手関連特徴量
│   ├── pace_features.py        # ペース・展開関連特徴量
│   ├── course_features.py      # コース適性特徴量
│   ├── time_features.py        # タイム関連特徴量
│   └── feature_store.py        # 特徴量保存・管理
├── models/                   # モデル
│   ├── base_model.py         # モデル基底クラス
│   ├── roi_optimizer.py      # 回収率最適化ロジック
│   └── evaluation.py         # モデル評価・検証
├── visualization/            # 可視化
│   ├── visualizer.py         # 基本可視化関数
│   └── dashboard.py          # ダッシュボード
├── notebooks/                # Jupyter notebooks
│   ├── exploratory_analysis.ipynb  # 探索的データ分析
│   └── feature_validation.ipynb    # 特徴量検証
├── scripts/                  # 実行スクリプト
│   ├── train_model.py        # モデルトレーニング
│   └── predict.py            # 予測実行
├── tests/                    # テスト
├── .gitignore                # git除外設定
├── requirements.txt          # 依存パッケージ一覧
└── setup.py                  # パッケージ設定
```

## インストールと実行方法

### 1. 環境のセットアップ

```bash
# リポジトリのクローン
git clone https://github.com/hironyan25/jra-racing-roi-analysis.git
cd jra-racing-roi-analysis

# 仮想環境の作成と有効化
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 必要パッケージのインストール
pip install -r requirements.txt
```

### 2. 設定

`config/db_config.py` にデータベース接続情報を設定してください。

### 3. 実行

```bash
# モデルのトレーニング
python scripts/train_model.py

# 予測の実行
python scripts/predict.py
```

## 主要な分析結果

1. **種牡馬×馬場適性ROI**：エーシントップ産駒はダート良馬場で勝率23.53%、回収率959.41%という驚異的な数値を示しています。

2. **騎手のコース別平均配当**：川合達彦騎手は京都中距離で回収率1069.27%という驚異的な数値を記録しています。

3. **展開不利指標**：前走で展開不利→大敗した馬は次走で平均9.35倍の配当となり、回収率64.07%を実現しています。

4. **上がりタイム順位**：上がり1位の馬は平均36.16%という高い勝率と337.19%という驚異的な回収率を示しています。

## ライセンス

MIT

## 貢献者

本プロジェクトへの貢献を歓迎します。Issue報告や機能追加の提案など、お気軽にお寄せください。
