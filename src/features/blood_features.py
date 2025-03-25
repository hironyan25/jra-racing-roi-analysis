#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
血統関連の特徴量を生成するモジュール

主な機能:
- 種牡馬×馬場適性ROI特徴量の計算
- 母父による特徴量
- 3代血統からの特徴量抽出
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from logging import getLogger

logger = getLogger(__name__)

class BloodFeatureGenerator:
    """血統関連の特徴量を生成するクラス"""
    
    def __init__(self, db_engine):
        """
        Parameters
        ----------
        db_engine : sqlalchemy.engine.Engine
            データベース接続エンジン
        """
        self.engine = db_engine
        
    def get_sire_track_condition_roi(self, since_year=2000, min_races=20):
        """
        種牡馬×馬場条件ごとの回収率を計算する
        
        Parameters
        ----------
        since_year : int, default=2000
            何年以降のデータを使用するか
        min_races : int, default=20
            最低レース数の閾値
            
        Returns
        -------
        pd.DataFrame
            種牡馬×馬場条件ごとの回収率データフレーム
        """
        query = f"""
        WITH sire_track_results AS (
            -- 父馬ごとのトラック種別・馬場状態別の成績
            SELECT 
                u.ketto_joho_01a AS sire_id,  -- 父馬ID
                TRIM(u.ketto_joho_01b) AS sire_name,  -- 父馬名
                CASE 
                    WHEN SUBSTRING(r.track_code, 1, 1) = '1' THEN '芝' 
                    WHEN SUBSTRING(r.track_code, 1, 1) = '2' THEN 'ダート'
                    ELSE 'その他'
                END AS track_type,  -- トラック種別
                CASE 
                    WHEN SUBSTRING(r.track_code, 1, 1) = '1' THEN r.babajotai_code_shiba
                    WHEN SUBSTRING(r.track_code, 1, 1) = '2' THEN r.babajotai_code_dirt
                    ELSE '0'
                END AS condition_code,  -- 馬場状態コード
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
                END AS track_condition,  -- 馬場状態
                s.kakutei_chakujun AS finish_pos,  -- 着順
                CAST(s.tansho_odds AS NUMERIC) / 10.0 AS win_odds,  -- 単勝オッズ
                CAST(s.tansho_ninkijun AS INTEGER) AS popularity  -- 人気順
            FROM jvd_se s
            JOIN jvd_ra r ON s.kaisai_nen = r.kaisai_nen 
                          AND s.kaisai_tsukihi = r.kaisai_tsukihi
                          AND s.keibajo_code = r.keibajo_code
                          AND s.race_bango = r.race_bango
            JOIN jvd_um u ON s.ketto_toroku_bango = u.ketto_toroku_bango
            WHERE s.kakutei_chakujun ~ '^[0-9]+$'
            AND s.kakutei_chakujun NOT IN ('00', '99')
            AND r.kaisai_nen >= '{since_year}'
            AND u.ketto_joho_01a != '0000000000'  -- 父馬不明を除外
            AND s.tansho_odds IS NOT NULL
        )
        SELECT 
            sire_id,
            sire_name,
            track_type,
            track_condition,
            COUNT(*) AS total_races,
            COUNT(*) FILTER (WHERE finish_pos = '01') AS wins,
            ROUND(COUNT(*) FILTER (WHERE finish_pos = '01')::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS win_rate,
            ROUND(SUM(CASE WHEN finish_pos = '01' THEN win_odds ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS roi_percentage,
            ROUND(AVG(CASE WHEN finish_pos = '01' THEN win_odds ELSE NULL END), 2) AS avg_win_odds,
            ROUND(AVG(popularity), 2) AS avg_popularity,
            COUNT(*) FILTER (WHERE popularity >= 9 AND finish_pos = '01') AS longshot_wins
        FROM sire_track_results
        GROUP BY sire_id, sire_name, track_type, track_condition
        HAVING COUNT(*) >= {min_races}  -- 最低レース数
        ORDER BY roi_percentage DESC
        """
        
        try:
            logger.info("種牡馬×馬場条件ROIデータを取得中...")
            df = pd.read_sql(query, self.engine)
            logger.info(f"{len(df)}行のデータを取得しました")
            return df
        except Exception as e:
            logger.error(f"種牡馬×馬場条件ROIデータ取得エラー: {e}")
            return pd.DataFrame()
            
    def get_horse_sire_track_roi_feature(self, horse_id, race_track_type, race_condition):
        """
        特定の馬の父馬×レースのトラック・馬場条件に対するROI特徴量を取得
        
        Parameters
        ----------
        horse_id : str
            血統登録番号
        race_track_type : str
            レースのトラック種別 ('芝'/'ダート'/'その他')
        race_condition : str
            レースの馬場状態 ('良'/'稍重'/'重'/'不良')
            
        Returns
        -------
        dict
            ROI特徴量を含む辞書
        """
        query = f"""
        SELECT 
            u.ketto_joho_01a AS sire_id,
            TRIM(u.ketto_joho_01b) AS sire_name
        FROM jvd_um u
        WHERE u.ketto_toroku_bango = '{horse_id}'
        """
        
        try:
            sire_info = pd.read_sql(query, self.engine)
            if sire_info.empty:
                logger.warning(f"馬ID {horse_id} の父馬情報が見つかりません")
                return {
                    'sire_name': None,
                    'sire_track_roi': 0.0,
                    'sire_track_win_rate': 0.0,
                    'sire_track_races': 0,
                    'sire_track_roi_rank': 0
                }
            
            sire_id = sire_info.iloc[0]['sire_id']
            sire_name = sire_info.iloc[0]['sire_name']
            
            # 父馬の特定トラック・馬場状態でのROIを取得
            roi_query = f"""
            WITH sire_track_results AS (
                -- 父馬ごとのトラック種別・馬場状態別の成績
                SELECT 
                    u.ketto_joho_01a AS sire_id,
                    TRIM(u.ketto_joho_01b) AS sire_name,
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
                    END AS track_condition,
                    s.kakutei_chakujun AS finish_pos,
                    CAST(s.tansho_odds AS NUMERIC) / 10.0 AS win_odds
                FROM jvd_se s
                JOIN jvd_ra r ON s.kaisai_nen = r.kaisai_nen 
                              AND s.kaisai_tsukihi = r.kaisai_tsukihi
                              AND s.keibajo_code = r.keibajo_code
                              AND s.race_bango = r.race_bango
                JOIN jvd_um u ON s.ketto_toroku_bango = u.ketto_toroku_bango
                WHERE s.kakutei_chakujun ~ '^[0-9]+$'
                AND s.kakutei_chakujun NOT IN ('00', '99')
                AND u.ketto_joho_01a = '{sire_id}'
                AND s.tansho_odds IS NOT NULL
            )
            SELECT 
                COUNT(*) AS total_races,
                COUNT(*) FILTER (WHERE finish_pos = '01') AS wins,
                ROUND(COUNT(*) FILTER (WHERE finish_pos = '01')::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS win_rate,
                ROUND(SUM(CASE WHEN finish_pos = '01' THEN win_odds ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS roi_percentage
            FROM sire_track_results
            WHERE track_type = '{race_track_type}' AND track_condition = '{race_condition}'
            """
            
            roi_info = pd.read_sql(roi_query, self.engine)
            
            if roi_info.empty or roi_info.iloc[0]['total_races'] is None:
                logger.info(f"父馬 {sire_name} の {race_track_type}・{race_condition} での出走データがありません")
                return {
                    'sire_name': sire_name,
                    'sire_track_roi': 0.0,
                    'sire_track_win_rate': 0.0,
                    'sire_track_races': 0,
                    'sire_track_roi_rank': 0
                }
            
            # 全体でのランク取得
            rank_query = f"""
            WITH sire_ranking AS (
                WITH sire_track_results AS (
                    SELECT 
                        u.ketto_joho_01a AS sire_id,
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
                        END AS track_condition,
                        s.kakutei_chakujun AS finish_pos,
                        CAST(s.tansho_odds AS NUMERIC) / 10.0 AS win_odds
                    FROM jvd_se s
                    JOIN jvd_ra r ON s.kaisai_nen = r.kaisai_nen 
                                  AND s.kaisai_tsukihi = r.kaisai_tsukihi
                                  AND s.keibajo_code = r.keibajo_code
                                  AND s.race_bango = r.race_bango
                    JOIN jvd_um u ON s.ketto_toroku_bango = u.ketto_toroku_bango
                    WHERE s.kakutei_chakujun ~ '^[0-9]+$'
                    AND s.kakutei_chakujun NOT IN ('00', '99')
                    AND s.tansho_odds IS NOT NULL
                )
                SELECT 
                    sire_id,
                    track_type,
                    track_condition,
                    COUNT(*) AS total_races,
                    ROUND(SUM(CASE WHEN finish_pos = '01' THEN win_odds ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 2) AS roi_percentage,
                    ROW_NUMBER() OVER (
                        PARTITION BY track_type, track_condition 
                        ORDER BY SUM(CASE WHEN finish_pos = '01' THEN win_odds ELSE 0 END) / NULLIF(COUNT(*), 0) DESC
                    ) AS roi_rank
                FROM sire_track_results
                WHERE track_type = '{race_track_type}' AND track_condition = '{race_condition}'
                GROUP BY sire_id, track_type, track_condition
                HAVING COUNT(*) >= 20  -- 最低レース数
            )
            SELECT roi_rank
            FROM sire_ranking
            WHERE sire_id = '{sire_id}'
            """
            
            try:
                rank_info = pd.read_sql(rank_query, self.engine)
                roi_rank = rank_info.iloc[0]['roi_rank'] if not rank_info.empty else 0
            except Exception as e:
                logger.warning(f"ROIランク取得エラー: {e}")
                roi_rank = 0
            
            # 結果を返す
            return {
                'sire_name': sire_name,
                'sire_track_roi': roi_info.iloc[0]['roi_percentage'],
                'sire_track_win_rate': roi_info.iloc[0]['win_rate'],
                'sire_track_races': roi_info.iloc[0]['total_races'],
                'sire_track_roi_rank': roi_rank
            }
            
        except Exception as e:
            logger.error(f"種牡馬×馬場条件ROI特徴量取得エラー: {e}")
            return {
                'sire_name': None,
                'sire_track_roi': 0.0,
                'sire_track_win_rate': 0.0,
                'sire_track_races': 0,
                'sire_track_roi_rank': 0
            }
            
    def get_pedigree_tree(self, horse_id, depth=3):
        """
        馬の血統ツリーを取得する
        
        Parameters
        ----------
        horse_id : str
            血統登録番号
        depth : int, default=3
            遡る世代数（1=父母、2=祖父母、3=曾祖父母）
            
        Returns
        -------
        dict
            血統ツリー情報
        """
        if depth > 3:
            logger.warning(f"深さは最大3までサポートしています。depth={depth}は3に調整されます。")
            depth = 3
            
        query = f"""
        WITH RECURSIVE pedigree AS (
            -- 基点となる馬
            SELECT 
                u.ketto_toroku_bango AS horse_id,
                TRIM(u.bamei) AS horse_name,
                u.ketto_joho_01a AS sire_id,
                TRIM(u.ketto_joho_01b) AS sire_name,
                u.ketto_joho_02a AS dam_id,
                TRIM(u.ketto_joho_02b) AS dam_name,
                1 AS generation,
                '1' AS position
            FROM jvd_um u
            WHERE u.ketto_toroku_bango = '{horse_id}'
            
            UNION ALL
            
            -- 父方
            SELECT 
                p.sire_id AS horse_id,
                p.sire_name AS horse_name,
                s.ketto_joho_01a AS sire_id,
                TRIM(s.ketto_joho_01b) AS sire_name,
                s.ketto_joho_02a AS dam_id,
                TRIM(s.ketto_joho_02b) AS dam_name,
                p.generation + 1 AS generation,
                p.position || '1' AS position
            FROM pedigree p
            LEFT JOIN jvd_um s ON p.sire_id = s.ketto_toroku_bango
            WHERE p.generation < {depth} AND p.sire_id != '0000000000' AND p.sire_id IS NOT NULL
            
            UNION ALL
            
            -- 母方
            SELECT 
                p.dam_id AS horse_id,
                p.dam_name AS horse_name,
                d.ketto_joho_01a AS sire_id,
                TRIM(d.ketto_joho_01b) AS sire_name,
                d.ketto_joho_02a AS dam_id,
                TRIM(d.ketto_joho_02b) AS dam_name,
                p.generation + 1 AS generation,
                p.position || '2' AS position
            FROM pedigree p
            LEFT JOIN jvd_um d ON p.dam_id = d.ketto_toroku_bango
            WHERE p.generation < {depth} AND p.dam_id != '0000000000' AND p.dam_id IS NOT NULL
        )
        SELECT 
            horse_id,
            horse_name,
            sire_id,
            sire_name,
            dam_id,
            dam_name,
            generation,
            position
        FROM pedigree
        ORDER BY generation, position
        """
        
        try:
            df = pd.read_sql(query, self.engine)
            
            if df.empty:
                logger.warning(f"馬ID {horse_id} の血統情報が見つかりません")
                return {}
                
            # 結果を階層構造に変換
            tree = {}
            
            for _, row in df.iterrows():
                if row['generation'] == 1:  # 本馬
                    tree = {
                        'id': row['horse_id'],
                        'name': row['horse_name'],
                        'sire': {'id': row['sire_id'], 'name': row['sire_name']},
                        'dam': {'id': row['dam_id'], 'name': row['dam_name']}
                    }
                else:
                    # positionをパースして親を見つける
                    pos = row['position']
                    parent_pos = pos[:-1]
                    is_sire = pos[-1] == '1'
                    
                    # 親ノードを探す
                    current = tree
                    for c in parent_pos[1:]:
                        if c == '1':
                            current = current['sire']
                        else:
                            current = current['dam']
                    
                    # 当該ノードを追加
                    node = {
                        'id': row['horse_id'],
                        'name': row['horse_name']
                    }
                    
                    if row['generation'] < depth:
                        node['sire'] = {'id': row['sire_id'], 'name': row['sire_name']}
                        node['dam'] = {'id': row['dam_id'], 'name': row['dam_name']}
                    
                    if is_sire:
                        current['sire'] = node
                    else:
                        current['dam'] = node
            
            return tree
            
        except Exception as e:
            logger.error(f"血統ツリー取得エラー: {e}")
            return {}
