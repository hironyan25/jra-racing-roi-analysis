#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
騎手関連の特徴量を生成するモジュール

主な機能:
- 騎手のコース別成績と回収率の計算
- 騎手の距離適性評価
- 騎手の馬場状態別成績評価
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from logging import getLogger

logger = getLogger(__name__)

class JockeyFeatureGenerator:
    """騎手関連の特徴量を生成するクラス"""
    
    def __init__(self, db_engine):
        """
        Parameters
        ----------
        db_engine : sqlalchemy.engine.Engine
            データベース接続エンジン
        """
        self.engine = db_engine
        
    def get_jockey_course_roi(self, since_year=2000, min_races=20):
        """
        騎手別・コース別の回収率を取得する
        
        Parameters
        ----------
        since_year : int, default=2000
            何年以降のデータを使用するか
        min_races : int, default=20
            最低レース数の閾値
            
        Returns
        -------
        pd.DataFrame
            騎手別・コース別の回収率データフレーム
        """
        query = f"""
        WITH jockey_course_results AS (
            -- 騎手ごとの競馬場・トラック種別・距離別の成績
            SELECT 
                s.kishu_code AS jockey_id,
                TRIM(s.kishumei_ryakusho) AS jockey_name,
                r.keibajo_code,
                CASE 
                    WHEN r.keibajo_code = '01' THEN '札幌'
                    WHEN r.keibajo_code = '02' THEN '函館'
                    WHEN r.keibajo_code = '03' THEN '福島'
                    WHEN r.keibajo_code = '04' THEN '新潟'
                    WHEN r.keibajo_code = '05' THEN '東京'
                    WHEN r.keibajo_code = '06' THEN '中山'
                    WHEN r.keibajo_code = '07' THEN '中京'
                    WHEN r.keibajo_code = '08' THEN '京都'
                    WHEN r.keibajo_code = '09' THEN '阪神'
                    WHEN r.keibajo_code = '10' THEN '小倉'
                    ELSE r.keibajo_code
                END AS course_name,
                CASE 
                    WHEN SUBSTRING(r.track_code, 1, 1) = '1' THEN '芝' 
                    WHEN SUBSTRING(r.track_code, 1, 1) = '2' THEN 'ダート'
                    ELSE 'その他'
                END AS track_type,
                CASE 
                    WHEN CAST(r.kyori AS INTEGER) <= 1400 THEN '短距離'
                    WHEN CAST(r.kyori AS INTEGER) <= 2000 THEN '中距離'
                    ELSE '長距離'
                END AS distance_category,
                s.kakutei_chakujun AS finish_pos,
                CAST(s.tansho_odds AS NUMERIC) / 10.0 AS win_odds,
                CAST(s.tansho_ninkijun AS INTEGER) AS popularity
            FROM jvd_se s
            JOIN jvd_ra r ON s.kaisai_nen = r.kaisai_nen 
                          AND s.kaisai_tsukihi = r.kaisai_tsukihi
                          AND s.keibajo_code = r.keibajo_code
                          AND s.race_bango = r.race_bango
            WHERE s.kakutei_chakujun ~ '^[0-9]+$'
            AND s.kakutei_chakujun NOT IN ('00', '99')
            AND r.kaisai_nen >= '{since_year}'
            AND s.tansho_odds IS NOT NULL
        )
        SELECT 
            jockey_id,
            jockey_name,
            course_name,
            track_type,
            distance_category,
            COUNT(*) AS total_races,
            COUNT(*) FILTER (WHERE finish_pos = '01') AS wins,
            COUNT(*) FILTER (WHERE finish_pos IN ('01', '02', '03')) AS top3_finishes,
            ROUND(COUNT(*) FILTER (WHERE finish_pos = '01')::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS win_rate,
            ROUND(COUNT(*) FILTER (WHERE finish_pos IN ('01', '02', '03'))::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS top3_rate,
            ROUND(SUM(CASE WHEN finish_pos = '01' THEN win_odds ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS roi_percentage,
            ROUND(AVG(CASE WHEN finish_pos = '01' THEN win_odds ELSE NULL END), 2) AS avg_win_odds,
            ROUND(AVG(popularity), 2) AS avg_popularity,
            COUNT(*) FILTER (WHERE popularity >= 4 AND popularity <= 8 AND finish_pos = '01') AS middle_odds_wins,
            COUNT(*) FILTER (WHERE popularity >= 9 AND finish_pos = '01') AS longshot_wins
        FROM jockey_course_results
        GROUP BY jockey_id, jockey_name, course_name, track_type, distance_category
        HAVING COUNT(*) >= {min_races}  -- 最低レース数
        ORDER BY roi_percentage DESC
        """
        
        try:
            logger.info("騎手別・コース別ROIデータを取得中...")
            df = pd.read_sql(query, self.engine)
            logger.info(f"{len(df)}行のデータを取得しました")
            return df
        except Exception as e:
            logger.error(f"騎手別・コース別ROIデータ取得エラー: {e}")
            return pd.DataFrame()
            
    def get_jockey_popularity_roi(self, since_year=2000, min_races=20):
        """
        騎手の人気別回収率を取得する
        
        Parameters
        ----------
        since_year : int, default=2000
            何年以降のデータを使用するか
        min_races : int, default=20
            最低レース数の閾値
            
        Returns
        -------
        pd.DataFrame
            騎手の人気別回収率データフレーム
        """
        query = f"""
        WITH jockey_popularity_results AS (
            -- 騎手ごとの人気別成績
            SELECT 
                s.kishu_code AS jockey_id,
                TRIM(s.kishumei_ryakusho) AS jockey_name,
                CASE 
                    WHEN CAST(s.tansho_ninkijun AS INTEGER) BETWEEN 1 AND 3 THEN '人気（1-3位）'
                    WHEN CAST(s.tansho_ninkijun AS INTEGER) BETWEEN 4 AND 8 THEN '中穴（4-8位）'
                    ELSE '大穴（9位-）'
                END AS popularity_category,
                s.kakutei_chakujun AS finish_pos,
                CAST(s.tansho_odds AS NUMERIC) / 10.0 AS win_odds
            FROM jvd_se s
            JOIN jvd_ra r ON s.kaisai_nen = r.kaisai_nen 
                          AND s.kaisai_tsukihi = r.kaisai_tsukihi
                          AND s.keibajo_code = r.keibajo_code
                          AND s.race_bango = r.race_bango
            WHERE s.kakutei_chakujun ~ '^[0-9]+$'
            AND s.kakutei_chakujun NOT IN ('00', '99')
            AND r.kaisai_nen >= '{since_year}'
            AND s.tansho_odds IS NOT NULL
            AND s.tansho_ninkijun IS NOT NULL
        )
        SELECT 
            jockey_id,
            jockey_name,
            popularity_category,
            COUNT(*) AS total_races,
            COUNT(*) FILTER (WHERE finish_pos = '01') AS wins,
            COUNT(*) FILTER (WHERE finish_pos IN ('01', '02', '03')) AS top3_finishes,
            ROUND(COUNT(*) FILTER (WHERE finish_pos = '01')::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS win_rate,
            ROUND(COUNT(*) FILTER (WHERE finish_pos IN ('01', '02', '03'))::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS top3_rate,
            ROUND(SUM(CASE WHEN finish_pos = '01' THEN win_odds ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS roi_percentage,
            ROUND(AVG(CASE WHEN finish_pos = '01' THEN win_odds ELSE NULL END), 2) AS avg_win_odds
        FROM jockey_popularity_results
        GROUP BY jockey_id, jockey_name, popularity_category
        HAVING COUNT(*) >= {min_races}  -- 最低レース数
        ORDER BY popularity_category, roi_percentage DESC
        """
        
        try:
            logger.info("騎手の人気別ROIデータを取得中...")
            df = pd.read_sql(query, self.engine)
            logger.info(f"{len(df)}行のデータを取得しました")
            return df
        except Exception as e:
            logger.error(f"騎手の人気別ROIデータ取得エラー: {e}")
            return pd.DataFrame()
            
    def get_jockey_surface_condition_roi(self, since_year=2000, min_races=20):
        """
        騎手の馬場状態別回収率を取得する
        
        Parameters
        ----------
        since_year : int, default=2000
            何年以降のデータを使用するか
        min_races : int, default=20
            最低レース数の閾値
            
        Returns
        -------
        pd.DataFrame
            騎手の馬場状態別回収率データフレーム
        """
        query = f"""
        WITH jockey_surface_results AS (
            -- 騎手ごとのトラック種別・馬場状態別の成績
            SELECT 
                s.kishu_code AS jockey_id,
                TRIM(s.kishumei_ryakusho) AS jockey_name,
                CASE 
                    WHEN SUBSTRING(r.track_code, 1, 1) = '1' THEN '芝' 
                    WHEN SUBSTRING(r.track_code, 1, 1) = '2' THEN 'ダート'
                    ELSE 'その他'
                END AS track_type,
                CASE 
                    WHEN SUBSTRING(r.track_code, 1, 1) = '1' AND r.babajotai_code_shiba = '1' THEN '良'
                    WHEN SUBSTRING(r.track_code, 1, 1) = '1' AND r.babajotai_code_shiba = '2' THEN '稍重'
                    WHEN SUBSTRING(r.track_code, 1, 1) = '1' AND r.babajotai_code_shiba = '3' THEN '重'
                    WHEN SUBSTRING(r.track_code, 1, 1) = '1' AND r.babajotai_code_shiba = '4' THEN '不良'
                    WHEN SUBSTRING(r.track_code, 1, 1) = '2' AND r.babajotai_code_dirt = '1' THEN '良'
                    WHEN SUBSTRING(r.track_code, 1, 1) = '2' AND r.babajotai_code_dirt = '2' THEN '稍重'
                    WHEN SUBSTRING(r.track_code, 1, 1) = '2' AND r.babajotai_code_dirt = '3' THEN '重'
                    WHEN SUBSTRING(r.track_code, 1, 1) = '2' AND r.babajotai_code_dirt = '4' THEN '不良'
                    ELSE '不明'
                END AS surface_condition,
                s.kakutei_chakujun AS finish_pos,
                CAST(s.tansho_odds AS NUMERIC) / 10.0 AS win_odds,
                CAST(s.tansho_ninkijun AS INTEGER) AS popularity
            FROM jvd_se s
            JOIN jvd_ra r ON s.kaisai_nen = r.kaisai_nen 
                          AND s.kaisai_tsukihi = r.kaisai_tsukihi
                          AND s.keibajo_code = r.keibajo_code
                          AND s.race_bango = r.race_bango
            WHERE s.kakutei_chakujun ~ '^[0-9]+$'
            AND s.kakutei_chakujun NOT IN ('00', '99')
            AND r.kaisai_nen >= '{since_year}'
            AND s.tansho_odds IS NOT NULL
        )
        SELECT 
            jockey_id,
            jockey_name,
            track_type,
            surface_condition,
            COUNT(*) AS total_races,
            COUNT(*) FILTER (WHERE finish_pos = '01') AS wins,
            COUNT(*) FILTER (WHERE finish_pos IN ('01', '02', '03')) AS top3_finishes,
            ROUND(COUNT(*) FILTER (WHERE finish_pos = '01')::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS win_rate,
            ROUND(COUNT(*) FILTER (WHERE finish_pos IN ('01', '02', '03'))::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS top3_rate,
            ROUND(SUM(CASE WHEN finish_pos = '01' THEN win_odds ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS roi_percentage,
            ROUND(AVG(CASE WHEN finish_pos = '01' THEN win_odds ELSE NULL END), 2) AS avg_win_odds,
            ROUND(AVG(popularity), 2) AS avg_popularity,
            COUNT(*) FILTER (WHERE popularity >= 4 AND finish_pos = '01') AS non_favorite_wins
        FROM jockey_surface_results
        GROUP BY jockey_id, jockey_name, track_type, surface_condition
        HAVING COUNT(*) >= {min_races}  -- 最低レース数
        ORDER BY roi_percentage DESC
        """
        
        try:
            logger.info("騎手の馬場状態別ROIデータを取得中...")
            df = pd.read_sql(query, self.engine)
            logger.info(f"{len(df)}行のデータを取得しました")
            return df
        except Exception as e:
            logger.error(f"騎手の馬場状態別ROIデータ取得エラー: {e}")
            return pd.DataFrame()
    
    def get_jockey_course_feature(self, jockey_id, course_name, track_type, distance_category):
        """
        特定の騎手・コース条件での特徴量を取得する
        
        Parameters
        ----------
        jockey_id : str
            騎手コード
        course_name : str
            競馬場名 ('東京', '阪神', etc.)
        track_type : str
            トラック種別 ('芝', 'ダート', 'その他')
        distance_category : str
            距離カテゴリ ('短距離', '中距離', '長距離')
            
        Returns
        -------
        dict
            特徴量辞書
        """
        # 競馬場コードへの変換
        course_code_map = {
            '札幌': '01', '函館': '02', '福島': '03', '新潟': '04', '東京': '05',
            '中山': '06', '中京': '07', '京都': '08', '阪神': '09', '小倉': '10'
        }
        course_code = course_code_map.get(course_name)
        
        if not course_code:
            logger.warning(f"不明な競馬場名: {course_name}")
            course_code = course_name  # そのまま使用
        
        query = f"""
        WITH jockey_course_stats AS (
            -- 特定の騎手・コース条件での成績
            SELECT 
                s.kishu_code AS jockey_id,
                TRIM(s.kishumei_ryakusho) AS jockey_name,
                r.keibajo_code,
                CASE 
                    WHEN r.keibajo_code = '01' THEN '札幌'
                    WHEN r.keibajo_code = '02' THEN '函館'
                    WHEN r.keibajo_code = '03' THEN '福島'
                    WHEN r.keibajo_code = '04' THEN '新潟'
                    WHEN r.keibajo_code = '05' THEN '東京'
                    WHEN r.keibajo_code = '06' THEN '中山'
                    WHEN r.keibajo_code = '07' THEN '中京'
                    WHEN r.keibajo_code = '08' THEN '京都'
                    WHEN r.keibajo_code = '09' THEN '阪神'
                    WHEN r.keibajo_code = '10' THEN '小倉'
                    ELSE r.keibajo_code
                END AS course_name,
                CASE 
                    WHEN SUBSTRING(r.track_code, 1, 1) = '1' THEN '芝' 
                    WHEN SUBSTRING(r.track_code, 1, 1) = '2' THEN 'ダート'
                    ELSE 'その他'
                END AS track_type,
                CASE 
                    WHEN CAST(r.kyori AS INTEGER) <= 1400 THEN '短距離'
                    WHEN CAST(r.kyori AS INTEGER) <= 2000 THEN '中距離'
                    ELSE '長距離'
                END AS distance_category,
                s.kakutei_chakujun AS finish_pos,
                CAST(s.tansho_odds AS NUMERIC) / 10.0 AS win_odds,
                CAST(s.tansho_ninkijun AS INTEGER) AS popularity
            FROM jvd_se s
            JOIN jvd_ra r ON s.kaisai_nen = r.kaisai_nen 
                          AND s.kaisai_tsukihi = r.kaisai_tsukihi
                          AND s.keibajo_code = r.keibajo_code
                          AND s.race_bango = r.race_bango
            WHERE s.kakutei_chakujun ~ '^[0-9]+$'
            AND s.kakutei_chakujun NOT IN ('00', '99')
            AND s.kishu_code = '{jockey_id}'
            AND r.keibajo_code = '{course_code}'
            AND CASE 
                    WHEN SUBSTRING(r.track_code, 1, 1) = '1' THEN '芝' 
                    WHEN SUBSTRING(r.track_code, 1, 1) = '2' THEN 'ダート'
                    ELSE 'その他'
                END = '{track_type}'
            AND CASE 
                    WHEN CAST(r.kyori AS INTEGER) <= 1400 THEN '短距離'
                    WHEN CAST(r.kyori AS INTEGER) <= 2000 THEN '中距離'
                    ELSE '長距離'
                END = '{distance_category}'
            AND s.tansho_odds IS NOT NULL
        ),
        jockey_all_stats AS (
            -- 同じ騎手の全体成績（比較用）
            SELECT 
                s.kishu_code AS jockey_id,
                TRIM(s.kishumei_ryakusho) AS jockey_name,
                s.kakutei_chakujun AS finish_pos,
                CAST(s.tansho_odds AS NUMERIC) / 10.0 AS win_odds
            FROM jvd_se s
            WHERE s.kakutei_chakujun ~ '^[0-9]+$'
            AND s.kakutei_chakujun NOT IN ('00', '99')
            AND s.kishu_code = '{jockey_id}'
            AND s.tansho_odds IS NOT NULL
        ),
        aggregate_stats AS (
            -- コース条件での集計結果
            SELECT 
                jockey_id,
                jockey_name,
                COUNT(*) AS total_races,
                COUNT(*) FILTER (WHERE finish_pos = '01') AS wins,
                COUNT(*) FILTER (WHERE finish_pos IN ('01', '02', '03')) AS top3_finishes,
                ROUND(COUNT(*) FILTER (WHERE finish_pos = '01')::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS win_rate,
                ROUND(COUNT(*) FILTER (WHERE finish_pos IN ('01', '02', '03'))::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS top3_rate,
                ROUND(SUM(CASE WHEN finish_pos = '01' THEN win_odds ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS roi_percentage,
                ROUND(AVG(CASE WHEN finish_pos = '01' THEN win_odds ELSE NULL END), 2) AS avg_win_odds,
                ROUND(AVG(popularity), 2) AS avg_popularity,
                COUNT(*) FILTER (WHERE popularity >= 4 AND popularity <= 8 AND finish_pos = '01') AS middle_odds_wins,
                COUNT(*) FILTER (WHERE popularity >= 9 AND finish_pos = '01') AS longshot_wins
            FROM jockey_course_stats
            GROUP BY jockey_id, jockey_name
        ),
        all_aggregate_stats AS (
            -- 全体の集計結果
            SELECT 
                jockey_id,
                jockey_name,
                COUNT(*) AS total_races,
                COUNT(*) FILTER (WHERE finish_pos = '01') AS wins,
                ROUND(COUNT(*) FILTER (WHERE finish_pos = '01')::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS win_rate,
                ROUND(SUM(CASE WHEN finish_pos = '01' THEN win_odds ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS roi_percentage
            FROM jockey_all_stats
            GROUP BY jockey_id, jockey_name
        )
        SELECT 
            a.jockey_id,
            a.jockey_name,
            '{course_name}' AS course_name,
            '{track_type}' AS track_type,
            '{distance_category}' AS distance_category,
            a.total_races,
            a.wins,
            a.top3_finishes,
            a.win_rate,
            a.top3_rate,
            a.roi_percentage,
            a.avg_win_odds,
            a.avg_popularity,
            a.middle_odds_wins,
            a.longshot_wins,
            all_a.total_races AS all_total_races,
            all_a.win_rate AS all_win_rate,
            all_a.roi_percentage AS all_roi_percentage,
            -- コース適性指数（コース別回収率 ÷ 全体回収率）
            ROUND(a.roi_percentage / NULLIF(all_a.roi_percentage, 0), 2) AS course_aptitude_index,
            -- 勝率比（コース別勝率 ÷ 全体勝率）
            ROUND(a.win_rate / NULLIF(all_a.win_rate, 0), 2) AS win_rate_ratio
        FROM aggregate_stats a
        JOIN all_aggregate_stats all_a ON a.jockey_id = all_a.jockey_id
        """
        
        try:
            logger.info(f"騎手ID {jockey_id} のコース特徴量データを取得中...")
            df = pd.read_sql(query, self.engine)
            
            if df.empty:
                logger.warning(f"騎手ID {jockey_id} のコース特徴量データがありません")
                # 空の結果を辞書形式で返す
                return {
                    'jockey_id': jockey_id,
                    'jockey_name': '',
                    'course_name': course_name,
                    'track_type': track_type,
                    'distance_category': distance_category,
                    'total_races': 0,
                    'wins': 0,
                    'win_rate': 0.0,
                    'roi_percentage': 0.0,
                    'avg_win_odds': 0.0,
                    'course_aptitude_index': 0.0,
                    'win_rate_ratio': 0.0
                }
            
            # 最初の行を辞書として返す
            result = df.iloc[0].to_dict()
            logger.info(f"騎手 {result['jockey_name']} のコース特徴量データを取得しました")
            return result
            
        except Exception as e:
            logger.error(f"騎手コース特徴量取得エラー: {e}")
            return {
                'jockey_id': jockey_id,
                'jockey_name': '',
                'course_name': course_name,
                'track_type': track_type,
                'distance_category': distance_category,
                'total_races': 0,
                'wins': 0,
                'win_rate': 0.0,
                'roi_percentage': 0.0,
                'avg_win_odds': 0.0,
                'course_aptitude_index': 0.0,
                'win_rate_ratio': 0.0
            }
