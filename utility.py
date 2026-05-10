
import os, json, joblib, numpy as np, pandas as pd, shap,re
from typing import Dict, Tuple, List
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, average_precision_score, f1_score, brier_score_loss, precision_recall_curve,roc_curve, classification_report
import tensorflow as tf
from keras import layers
import keras
import matplotlib.pyplot as plt
import xgboost as xgb
from sklearn.model_selection import train_test_split,StratifiedKFold
from sklearn.feature_selection import RFECV
from sklearn.ensemble import RandomForestClassifier
from boruta import BorutaPy
#from  Hyperkalemia import SEQ_HOURS

#SEQ_HOURS = Hyperkalemia.SEQ_HOURS

SEQ_HOURS = int(os.getenv("SEQ_HOURS", "72"))     # 特徵序列長度（0–24h）
LBL_FROM  = int(os.getenv("LBL_FROM_H", "24"))    # 標籤視窗起點（相對 t0 小時）
LBL_TO    = int(os.getenv("LBL_TO_H", "72"))      # 標籤視窗終點
K_CUTOFF  = float(os.getenv("K_CUTOFF", "5.5"))   # 高血鉀閾值
RANDOM_STATE = int(os.getenv("RANDOM_STATE", "42"))
TEST_SIZE = float(os.getenv("TEST_SIZE", "0.2"))
VAL_SIZE  = float(os.getenv("VAL_SIZE",  "0.2"))



FEATURE_MAP = {

    # ===== Vital =====
    'HEART RATE':'hr',
    'RESPIRATORY RATE':'rr',
    'SPO2':'spo2',
    'TEMPERATURE CELSIUS':'temp',
    'NON INVASIVE BLOOD PRESSURE SYSTOLIC':'sbp',
    'NON INVASIVE BLOOD PRESSURE DIASTOLIC':'dbp',
    'MEAN ARTERIAL PRESSURE (NIBP)':'map',

    # ===== Electrolyte =====
    'SODIUM':'sodium',
    #'SODIUM, WHOLE BLOOD':'sodium',

    'CHLORIDE':'chloride', #氯離子有很多種測量方式，且與血鉀的關聯性不明顯，暫不納入（可後續再評估）
    'Chloride, Whole Blood':'Chloride, Whole Blood',

    # ⚠️ potassium 已排除（正確）
    # 'POTASSIUM':'potassium',

    # ===== Renal =====
    'CREATININE':'creatinine',
    'UREA NITROGEN':'bun',
    #'EGFR':'egfr',
    #'Estimated GFR (MDRD equation)':'egfr',

    # ===== Acid-base（重要🔥）=====
    'PH':'ph',
    #'PH, VENOUS':'ph',
    'FREE CALCIUM':'free_calcium',

    'PCO2':'pco2',
    #'PCO2, VENOUS':'pco2',

    'CALCULATED TOTAL CO2':'hco3',
    'CALCIUM, TOTAL':'calcium_total',
    'BICARBONATE':'hco3',

    # ===== Metabolic =====
    'GLUCOSE':'glucose',
    'WHITE BLOOD CELLS':'white_blood_cells',

    # ===== Nutrition / protein =====
    'ALBUMIN':'albumin',
    'ALBUMIN, URINE':'albumin_urine',
    'TOTAL PROTEIN, URINE':'protein_urine',
    'PROTEIN':'Protein',

    # ===== Hematology =====
    'HEMOGLOBIN':'hemoglobin',
    'TRANSFERRIN':'transferrin',
    'LACTATE':'lactate',
    'MAGNESIUM':'magnesium',

    # ===== Lipid =====
    'CHOLESTEROL, TOTAL':'cholesterol',
    'CHOLESTEROL, LDL, CALCULATED':'ldl',
    'CHOLESTEROL, HDL':'hdl',
    'TRIGLYCERIDES':'triglycerides',

    # ===== Other labs =====
    'PHOSPHATE':'phosphate',
    'URIC ACID':'uric_acid',
    '% HEMOGLOBIN A1C':'hba1c',

    # ===== Urine =====
    'URINE_OUTPUT':'urine',

    # ===== Drugs =====
    'RAASI':'drug_raasi',
    'K_SPARING':'drug_k_sparing',
    'NSAID':'drug_nsaid',
    'HEPARIN':'drug_heparin',
    'LOOP':'drug_loop',
    'THIAZIDE':'drug_thiazide'
}



def to_hourly(df_long: pd.DataFrame, t0_df: pd.DataFrame) -> pd.DataFrame:
    """
    將不規則時間序列對齊到固定小時格，便於 pivot 成 N×T×F（每個病人每小時每個特徵一個值）。
    因為使用 tail(1)，排序很重要：charttime 要由早到晚，否則會取到錯誤的那一筆。
    簡單輸出範例列（意義）：

    stay_id=1001, time_index=0, label=HR, value=88 → 第 0 小時心跳 88
    stay_id=1001, time_index=1, label=SODIUM, value=137 → 第 1 小時血鈉 137
    若要改成按半小時或其它窗寬，只需改 offset 的分箱/round 邏輯即可。
    """
    if df_long.empty:
        return pd.DataFrame(columns=["stay_id","time_index","label","value"])
    df = df_long.merge(t0_df[["stay_id","t0"]], on="stay_id", how="left")

    df["offset_h"] = (pd.to_datetime(df["charttime"]) - pd.to_datetime(df["t0"])).dt.total_seconds()/3600.0
    #df["offset_h"] = (pd.to_datetime(df["charttime"]) - pd.to_datetime(df["t0"])).dt.total_seconds()/7200.0
    df = df[(df["offset_h"] >= 0) & (df["offset_h"] < SEQ_HOURS)].copy()
   # df["time_index"] = df["offset_h"].round().astype(int)
    df["time_index"] = np.floor(df["offset_h"]).astype(int)
    df.sort_values(["stay_id","label","charttime"], inplace=True)
    df = df.groupby(["stay_id","label","time_index"], as_index=False).tail(1)#同一小時若多筆只留最新值
    return df[["stay_id","label","time_index","valuenum"]].rename(columns={"valuenum":"value"})

def to_hourly_drug(df_drug: pd.DataFrame, stays: pd.DataFrame) -> pd.DataFrame:
    """
    將 prescriptions → 每小時藥物 exposure (0/1 指標)
    """
    if df_drug.empty:
        return pd.DataFrame(columns=["stay_id", "time_index", "label", "value"])

    df = df_drug.merge(stays[["stay_id", "t0"]], on="stay_id", how="left")

    df["offset_h"] = (pd.to_datetime(df["starttime"]) - pd.to_datetime(df["t0"])).dt.total_seconds()/3600.0
    df = df[(df["offset_h"] >= 0) & (df["offset_h"] < SEQ_HOURS)].copy()

    #df["time_index"] = df["offset_h"].round().astype(int)
    df["time_index"] = np.floor(df["offset_h"]).astype(int)

    # 每一小時每一類藥物取 1（有給藥就算 1）
    df["value"] = 1

    return df[["stay_id", "time_index", "label", "value"]]

def xgb_feature_importance(CSV_PATH,LAB_NAME):
    df = pd.read_csv(CSV_PATH)   # 你的CSV資料

    # === 2. Pivot：將各項檢驗轉為欄位 ===
    df_pivot = (
        df.pivot_table(index='stay_id', columns='label', values='valuenum', aggfunc='mean')
        .reset_index()
    )

    # === 3. 建立高血鉀標籤 ===
    if 'POTASSIUM' not in df_pivot.columns:
        raise ValueError("❗ 找不到 'POTASSIUM' 欄位，請確認資料中是否有血鉀 (K) 檢驗")
    #血鉀建立標籤（>5.5 → 高血鉀=1）
    df_pivot['target_hyperk'] = np.where(df_pivot['POTASSIUM'] > 5.5, 1, 0)
    df_pivot = df_pivot.dropna(subset=['target_hyperk'])

    # === 4. 特徵與標籤 ===
    X = df_pivot.drop(columns=['stay_id', 'target_hyperk'])
    y = df_pivot['target_hyperk'].astype(int)


    # 5) 關鍵：排除任何「疑似血鉀」的特徵欄位，避免資料洩漏
    #    規則：名稱含 potassium / k / k+（大小寫不敏感）
    leak_regex = re.compile(r'(potassium|^k$|\bk\+$|\bk\b|serum[_\s-]*k)', flags=re.IGNORECASE)
    leak_cols = [c for c in X.columns if leak_regex.search(str(c))]
    if leak_cols:
        print("🔒 移除疑似洩漏的血鉀相關欄位：", leak_cols)
        X = X.drop(columns=leak_cols)

    # 缺值填補
    X = X.fillna(X.median())

    # === 5. 訓練/測試切分 ===
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42, stratify=y
    )

    # === 6. 建立 XGBoost 模型 ===
    model = xgb.XGBClassifier(
        n_estimators=500,
        learning_rate=0.05,
        max_depth=5,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        eval_metric='auc'
    )

    model.fit(X_train, y_train)

    # === 7. 評估 ===
    y_pred = model.predict(X_test)
    y_pred_prob = model.predict_proba(X_test)[:, 1]

    auc = roc_auc_score(y_test, y_pred_prob)
    print(f"\n✅ ROC-AUC = {auc:.4f}")
    print(classification_report(y_test, y_pred, digits=3))

    # === 8. 特徵重要度 ===
    importance = pd.DataFrame({
        'feature': X.columns,
        'importance': model.feature_importances_
    }).sort_values(by='importance', ascending=False)

    print("\n📊 Top 10 重要特徵：")
    print(importance.head(10))

    # === 9. 可視化 ===
    plt.figure(figsize=(8,6))
    xgb.plot_importance(model, max_num_features=15, importance_type='gain')
    plt.title("XGBoost Feature Importance (Gain)")
    plt.show()

    # === 10. SHAP 解釋 ===
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_test)

    shap.summary_plot(shap_values, X_test, plot_type="bar", max_display=15)
    shap.summary_plot(shap_values, X_test, max_display=15)

    # === 11. 儲存結果 ===
    importance.to_csv("xgboost_feature_importance.csv", index=False)
    print("\n📁 已輸出：xgboost_feature_importance.csv")

def xgb_feature_importance2(CSV_PATH, LAB_NAME="POTASSIUM"):

    df = pd.read_csv(CSV_PATH)

    # === 1. Pivot：將各項檢驗轉為欄位 ===
    df_pivot = (
        df.pivot_table(
            index="stay_id",
            columns="label",
            values="valuenum",
            aggfunc="mean"
        )
        .reset_index()
    )

    # === 2. 建立 target ===
    if LAB_NAME not in df_pivot.columns:
        raise ValueError(f"❗ 找不到 '{LAB_NAME}' 欄位，請確認資料中是否有此檢驗")

    df_pivot["target_hyperk"] = np.where(df_pivot[LAB_NAME] > 5.5, 1, 0)

    # === 3. 特徵與標籤 ===
    X = df_pivot.drop(columns=["stay_id", "target_hyperk"])
    y = df_pivot["target_hyperk"].astype(int)

    # === 4. 移除血鉀相關欄位，避免資料洩漏 ===
    leak_regex = re.compile(
        r"(potassium|^k$|\bk\+$|\bk\b|serum[_\s-]*k)",
        flags=re.IGNORECASE
    )

    leak_cols = [c for c in X.columns if leak_regex.search(str(c))]

    if leak_cols:
        print("🔒 移除疑似洩漏的血鉀相關欄位：", leak_cols)
        X = X.drop(columns=leak_cols)

    # === 5. 缺值填補 ===
    X = X.apply(pd.to_numeric, errors="coerce")
    X = X.fillna(X.median())

    # === 6. Train / Test split ===
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.3,
        random_state=42,
        stratify=y
    )

    # === 7. XGBoost model ===
    model = xgb.XGBClassifier(
        n_estimators=500,
        learning_rate=0.05,
        max_depth=5,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        eval_metric="auc",
        n_jobs=-1
    )

    model.fit(X_train, y_train)

    # === 8. 評估 ===
    y_pred = model.predict(X_test)
    y_pred_prob = model.predict_proba(X_test)[:, 1]

    auc = roc_auc_score(y_test, y_pred_prob)

    print(f"\n✅ ROC-AUC = {auc:.4f}")
    print(classification_report(y_test, y_pred, digits=3))

    # === 9. XGBoost Gain Importance ===
    booster = model.get_booster()
    gain_dict = booster.get_score(importance_type="gain")

    importance = (
        pd.DataFrame({
            "feature": list(gain_dict.keys()),
            "importance_gain": list(gain_dict.values())
        })
        .sort_values("importance_gain", ascending=False)
        .reset_index(drop=True)
    )

    print("\n📊 Top 15 重要特徵：")
    print(importance.head(15))

    importance.to_csv("xgboost_feature_importance.csv", index=False, encoding="utf-8-sig")

    # === 10. XGBoost importance plot ===
    plt.figure(figsize=(8, 6))
    xgb.plot_importance(
        model,
        max_num_features=15,
        importance_type="gain"
    )
    plt.title("XGBoost Feature Importance (Gain)")
    plt.tight_layout()
    plt.savefig("xgb_feature_importance_gain.png", dpi=300, bbox_inches="tight")
    plt.show()

    # === 11. SHAP 解釋 ===
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_test)

    # SHAP bar plot
    shap.summary_plot(
        shap_values,
        X_test,
        plot_type="bar",
        max_display=15,
        show=False
    )
    plt.tight_layout()
    plt.savefig("shap_summary_bar.png", dpi=300, bbox_inches="tight")
    plt.show()

    # SHAP beeswarm plot
    shap.summary_plot(
        shap_values,
        X_test,
        max_display=15,
        show=False
    )
    plt.tight_layout()
    plt.savefig("shap_summary_beeswarm.png", dpi=300, bbox_inches="tight")
    plt.show()

    print("\n📁 已輸出：")
    print(" - xgboost_feature_importance.csv")
    print(" - xgb_feature_importance_gain.png")
    print(" - shap_summary_bar.png")
    print(" - shap_summary_beeswarm.png")

    return model, importance

def rfecv_feature_selection(CSV_PATH,LAB_NAME):#RFECV
    df = pd.read_csv(CSV_PATH)   # 你的CSV檔案名稱

    # === 2. Pivot，每位病人各檢驗平均值 ===
    df_pivot = (
        df.pivot_table(index='stay_id', columns='label', values='valuenum', aggfunc='mean')
        .reset_index()
    )

    # === 3. 定義高血鉀標籤 ===
    # 若資料中有 POTASSIUM 欄位，設定 K > 5.5 為高血鉀
    if 'POTASSIUM' not in df_pivot.columns:
        raise ValueError("❗ 資料中沒有 'POTASSIUM' 欄位，無法建立高血鉀標籤。請確認 CSV 是否包含血鉀 (K) 檢驗。")

    df_pivot['target_hyperk'] = np.where(df_pivot['POTASSIUM'] > 5.5, 1, 0)
    df_pivot = df_pivot.dropna(subset=['target_hyperk'])

    # === 4. 特徵矩陣 X、標籤 y ===
    X = df_pivot.drop(columns=['stay_id', 'target_hyperk'])
    y = df_pivot['target_hyperk'].astype(int)

    # 5) 關鍵：排除任何「疑似血鉀」的特徵欄位，避免資料洩漏
    #    規則：名稱含 potassium / k / k+（大小寫不敏感）
    leak_regex = re.compile(r'(potassium|^k$|\bk\+$|\bk\b|serum[_\s-]*k)', flags=re.IGNORECASE)
    leak_cols = [c for c in X.columns if leak_regex.search(str(c))]
    if leak_cols:
        print("🔒 移除疑似洩漏的血鉀相關欄位：", leak_cols)
        X = X.drop(columns=leak_cols)



    # 填補缺值
    X = X.fillna(X.median())

    # === 5. RFECV 特徵選擇 ===
    rf = RandomForestClassifier(
        n_estimators=200,
        random_state=42,
        class_weight='balanced'
    )

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    rfecv = RFECV(
        estimator=rf,
        step=1,
        cv=cv,
        scoring='roc_auc',
        n_jobs=-1
    )

    rfecv.fit(X, y)

    # === 6. 結果輸出 ===
    feature_ranks = pd.DataFrame({
        'feature': X.columns,
        'ranking': rfecv.ranking_,
        'selected': rfecv.support_
    }).sort_values(by='ranking')

    print("\n✅ RFECV 選出的重要特徵：")
    print(feature_ranks[feature_ranks['selected'] == True])

    mean_test_scores = np.mean(rfecv.cv_results_['mean_test_score'], axis=0) \
    if isinstance(rfecv.cv_results_['mean_test_score'], list) else rfecv.cv_results_['mean_test_score']




    # === 7. 視覺化 CV 結果 ===
    plt.figure(figsize=(8, 5))
    plt.title('RFECV Cross-Validation Score')
    plt.xlabel('Number of features selected')
    plt.ylabel('Cross-validation ROC-AUC')
    #plt.plot(range(1, len(rfecv.cv_results_) + 1), rfecv.cv_results_, marker='o')
    plt.plot(range(1, len(mean_test_scores) + 1), mean_test_scores, marker='o')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # === 8. 儲存結果 ===
    feature_ranks.to_csv("rfecv_feature_ranking.csv", index=False)
    print("\n📁 已輸出：rfecv_feature_ranking.csv")
    return 0

def boruta_feature_selection(CSV_PATH,LAB_NAME):#Boruta
    #LAB_NAME:要預測的檢驗名稱
    """
    使用 Boruta 方法從實驗室檢驗數據中選擇重要特徵，以預測高血鉀（K > 5.5 mEq/L）。
    輸入資料假設為 CSV 格式，包含欄位：stay_id, charttime, label, valuenum。
    """
    # === 1. 讀取原始資料 ===
    df = pd.read_csv(CSV_PATH)   # 你的CSV，例如 stay_id, charttime, label, valuenum

    # === 2. 整理資料 ===
    # 將每個 stay_id 各檢驗項目轉為欄位（Pivot）
    df_pivot = (
        df.pivot_table(index='stay_id', columns='label', values='valuenum', aggfunc='mean')
        .reset_index()
    )

    # === 3. 建立標籤變數 y ===
    # 定義高血鉀（K > 5.5 mEq/L）為 1，否則 0
    df_pivot['target_hyperk'] = np.where(df_pivot[LAB_NAME] > 5.5, 1, 0)

    # 若部分資料沒有血鉀值，先移除
    df_pivot = df_pivot.dropna(subset=['target_hyperk'])

    # === 4. 特徵矩陣與標籤 ===
    X = df_pivot.drop(columns=['stay_id', 'target_hyperk'])
    y = df_pivot['target_hyperk'].astype(int)

    leak_regex = re.compile(r'(potassium|^k$|\bk\+$|\bk\b|serum[_\s-]*k)', flags=re.IGNORECASE)
    leak_cols = [c for c in X.columns if leak_regex.search(str(c))]
    if leak_cols:
        print("🔒 移除疑似洩漏的血鉀相關欄位：", leak_cols)
        X = X.drop(columns=leak_cols)


    # 處理缺值
    X = X.fillna(X.median())

    # === 5. 建立隨機森林 + Boruta 特徵選擇器 ===
    rf = RandomForestClassifier(
        n_jobs=-1,
        class_weight='balanced',
        max_depth=7,
        random_state=42
    )

    boruta_selector = BorutaPy(
        rf,
        n_estimators='auto',
        verbose=2,
        random_state=42
    )

    boruta_selector.fit(X.values, y.values)

    # === 6. 結果輸出 ===
    feature_ranks = pd.DataFrame({
        'feature': X.columns,
        'rank': boruta_selector.ranking_,
        'selected': boruta_selector.support_
    }).sort_values(by='rank')

    print("\n✅ Boruta 選出的重要特徵：")
    print(feature_ranks[feature_ranks['selected'] == True])

    # === 7. 儲存結果 ===
    feature_ranks.to_csv("boruta_feature_ranking.csv", index=False)
    print("\n📁 已輸出：boruta_feature_ranking.csv")