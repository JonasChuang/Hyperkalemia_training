import matplotlib.pyplot as plt
from utility import (FEATURE_MAP, to_hourly, to_hourly_drug,to_hourly_urine,SEQ_HOURS,LBL_FROM,LBL_TO,K_CUTOFF,RANDOM_STATE
                     ,xgb_feature_importance,rfecv_feature_selection,boruta_feature_selection,xgb_feature_importance2,plot_eval_compare,loss_plot)

import os, json, joblib, numpy as np, pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, average_precision_score, f1_score, brier_score_loss, precision_recall_curve,roc_curve
import tensorflow as tf
from keras import layers
import keras

from plt_shap import  eval_block,eval_block2, plot_eval_radar,plot_eval_metrics,bootstrap_PLT
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from sklearn.calibration import CalibratedClassifierCV,calibration_curve
import statsmodels.api as sm
from build_tensor import build_tensor as build_tensor2, add_urine_features
#from tool import sqlalchmy_raw_sql
from Hyperkalemia import run_logistic_regression,run_logistic_regression_latest,run_xgboost
OUTDIR = os.getenv("OUTDIR", "./artifacts_hk_seq")

QUICK_TEST = os.getenv("QUICK_TEST", "false").lower() == "true"

np.random.seed(RANDOM_STATE)
tf.random.set_seed(RANDOM_STATE)
"""
    to_hourly()
        ↓
    build_wide()
        ↓
    add_urine_features()
    add_gcs_features()
        ↓
    ──────────────
    SPLIT
    ──────────────
    ↓     ↓     ↓
    Train  Val   Test
    ↓
    Train median
    ↓     ↓     ↓
    補 Train/Val/Test
        ↓
    to_tensor()
        ↓
    Scaler.fit(Train)
            ↓
    GRU / LSTM

"""

def focal_loss(gamma=2., alpha=0.25):#不平衡處理
    """
    意思是把損失函數 換成 Focal Loss，並設定兩個超參數：gamma=2.0、alpha=0.25
    「讓模型忽略容易的樣本，把注意力放在少數、困難的正樣本」，在不平衡資料（像 CKD 死亡、敗血症、低鈉）特別有效

    1. Focal Loss 的背景

    在 類別嚴重不平衡（例如敗血症、低鈉、腎衰竭發生率只有 5%）的情況下，傳統 Binary Cross-Entropy (BCE) 會被大量負樣本主導，導致模型傾向預測「全是陰性」。

    Focal Loss 是由 Lin et al., 2017 (RetinaNet) 提出的，用來解決分類不平衡。

    FL(pt)=−α(1−pt)γ log(pt)
    
    實際效果:

    在醫療數據上，使用 focal loss 常會，AUC 提升一點點或差不多；AUPRC 提升顯著（因為更關注正樣本）；

    收斂速度比 BCE 慢，但泛化到 test set 更穩。

    與 Binary Cross-Entropy 比較

    BCE（不平衡時）：

    假設資料 95% 陰性，模型就算永遠預測 0，也能得到很低的 loss。

    Focal Loss：

    會降低「容易分對的負樣本」在 loss 中的比重。

    會加強「難分的正樣本」的影響。

    適合醫療預測（罕見事件）。
    
    """
    def loss(y_true, y_pred):
        y_true = tf.cast(y_true, tf.float32)
        eps = 1e-7
        y_pred = tf.clip_by_value(y_pred, eps, 1.0 - eps)
        pt = tf.where(tf.equal(y_true, 1), y_pred, 1 - y_pred)
        w = tf.where(tf.equal(y_true, 1), alpha, 1 - alpha)
        return -tf.reduce_mean(w * tf.pow(1 - pt, gamma) * tf.math.log(pt))
    return loss

def build_gru(input_shape) -> keras.Model:
    inp = keras.Input(shape=input_shape, name="seq")
    x = layers.Masking(mask_value=0.0)(inp)
    x = layers.GRU(64, return_sequences=False)(x)#找特徵
  
    x = layers.Dropout(0.3)(x)#斷神經元
    out = layers.Dense(1, activation="sigmoid")(x)#訓練
    model = keras.Model(inp, out)
    model.compile(optimizer=keras.optimizers.Adam(1e-3),
                  #loss="binary_crossentropy",
                  loss=focal_loss(gamma=2.0, alpha=0.25),#不平衡處理
                  metrics=[keras.metrics.AUC(name="auc"), keras.metrics.AUC(curve="PR", name="auprc")])
    return model

def build_lstm(input_shape) -> keras.Model:
    inp = keras.Input(shape=input_shape, name="seq")
    x = layers.Masking(mask_value=0.0)(inp)
    x = layers.LSTM(64, return_sequences=False)(x)#找特徵
    x = layers.Dropout(0.3)(x)#斷神經元
    out = layers.Dense(1, activation="sigmoid")(x)#訓練
    model = keras.Model(inp, out)
    model.compile(optimizer=keras.optimizers.Adam(1e-3),
                  loss="binary_crossentropy",
                  metrics=[keras.metrics.AUC(name="auc"), keras.metrics.AUC(curve="PR", name="auprc")])
    return model

def main(flg):

    os.makedirs(OUTDIR, exist_ok=True)

    df_char = pd.read_csv(f"./{flg}/fetch_chartevents.csv")
    df_lab  = pd.read_csv(f"./{flg}/fetch_labevents.csv")
    df_ur   = pd.read_csv(f"./{flg}/URINE.csv")
    ydf     = pd.read_csv(f"./{flg}/POTASSIUM.csv")
    stays   = pd.read_csv(f"./{flg}/adm.csv")
    df_drug = pd.read_csv(f"./{flg}/高血鉀用藥.csv")

    # 避免 potassium label leakage
    df_lab = df_lab[
        ~df_lab["label"].isin([
            "POTASSIUM",
            "Potassium",
            "Potassium, Whole Blood"
        ])
    ]

    # ========================================
    # 1. Hourly data
    # ========================================

    char_h = to_hourly(df_char, stays)
    lab_h  = to_hourly(df_lab, stays)
    ur_h   = to_hourly_urine(df_ur, stays)
    drug_h = to_hourly_drug(df_drug, stays)

    # ========================================
    # 2. 建立 wide，但先不要 median
    # ========================================

    wide, feat_cols = build_wide(
        stays=stays,
        hourly_frames=[
            char_h,
            lab_h,
            ur_h,
            drug_h
        ],
        feature_builders=[
            add_urine_features
        ],
        seq_hours=SEQ_HOURS
    )

    # ========================================
    # 3. 先建立 y
    # ========================================

    sample_df = (
        stays[["stay_id"]]
        .drop_duplicates()
        .merge(
            ydf[["stay_id", "y_hk"]],
            on="stay_id",
            how="left"
        )
    )

    sample_df["y_hk"] = (
        sample_df["y_hk"]
        .fillna(0)
        .astype(int)
    )

    # ========================================
    # 4. Train / Val / Test split
    # ========================================

    rng = np.random.default_rng(RANDOM_STATE)

    idx = np.arange(len(sample_df))
    rng.shuffle(idx)

    TRAIN_P = float(os.getenv("TRAIN_P", "0.6"))

    remaining = 1.0 - TRAIN_P
    VAL_P = remaining / 2
    TEST_P = remaining / 2

    n_test = max(int(len(idx) * TEST_P), 1)
    n_val  = max(int(len(idx) * VAL_P), 1)

    te_idx = idx[:n_test]
    va_idx = idx[n_test:n_test + n_val]
    tr_idx = idx[n_test + n_val:]

    train_ids = sample_df.iloc[tr_idx]["stay_id"].values
    val_ids   = sample_df.iloc[va_idx]["stay_id"].values
    test_ids  = sample_df.iloc[te_idx]["stay_id"].values

    # ========================================
    # 5. split wide
    # ========================================

    train_wide = wide[
        wide["stay_id"].isin(train_ids)
    ].copy()

    val_wide = wide[
        wide["stay_id"].isin(val_ids)
    ].copy()

    test_wide = wide[
        wide["stay_id"].isin(test_ids)
    ].copy()

    # ========================================
    # 6. LOCF
    # 每個病人自己的過去值，安全
    # ========================================

    for df in [train_wide, val_wide, test_wide]:

        df[feat_cols] = (
            df.groupby("stay_id")[feat_cols]
            .ffill()
        )

    # ========================================
    # 7. median ONLY FROM TRAIN
    # ========================================

    train_median = train_wide[feat_cols].median()

    train_wide[feat_cols] = (
        train_wide[feat_cols]
        .fillna(train_median)
    )

    val_wide[feat_cols] = (
        val_wide[feat_cols]
        .fillna(train_median)
    )

    test_wide[feat_cols] = (
        test_wide[feat_cols]
        .fillna(train_median)
    )

    joblib.dump(
        train_median,
        os.path.join(OUTDIR, "median.joblib")
    )

    # ========================================
    # 8. to tensor
    # ========================================

    Xtr, train_sid = to_tensor(
        train_wide,
        feat_cols,
        SEQ_HOURS
    )

    Xva, val_sid = to_tensor(
        val_wide,
        feat_cols,
        SEQ_HOURS
    )

    Xte, test_sid = to_tensor(
        test_wide,
        feat_cols,
        SEQ_HOURS
    )

    # ========================================
    # 9. 依 tensor stay_id 取得 y
    # ========================================

    y_map = sample_df.set_index("stay_id")["y_hk"]

    ytr = train_sid["stay_id"].map(y_map).values
    yva = val_sid["stay_id"].map(y_map).values
    yte = test_sid["stay_id"].map(y_map).values
    #Train 高血鉀率VAL   高血鉀率TEST  高血鉀率

    print(
    f"TRAIN: N={len(ytr)}, "
    f"HK={ytr.sum()}, "
    f"rate={ytr.mean():.3f}"
    )

    print(
        f"VAL:   N={len(yva)}, "
        f"HK={yva.sum()}, "
        f"rate={yva.mean():.3f}"
    )

    print(
        f"TEST:  N={len(yte)}, "
        f"HK={yte.sum()}, "
        f"rate={yte.mean():.3f}"
    )


    # ========================================
    # 10. StandardScaler
    # ONLY FIT TRAIN
    #這段是在做 特徵標準化（Standardization），而且寫法是為了配合你的 GRU/LSTM 三維資料 (病人, 時間, 特徵)，同時避免資料洩漏。

    # 核心公式是：

    # $$ z = \frac{x-\mu}{\sigma} $$

    # 也就是每個特徵都減掉 Train 的平均值，再除以 Train 的標準差。

    # 例如你的特徵尺度可能差非常大：
    # ========================================

    Ntr, T, F = Xtr.shape
    Nva = Xva.shape[0]
    Nte = Xte.shape[0]

    Xtr_2d = Xtr.reshape(-1, F)
    Xva_2d = Xva.reshape(-1, F)
    Xte_2d = Xte.reshape(-1, F)

    scaler = StandardScaler()

    scaler.fit(Xtr_2d)

    Xtr = scaler.transform(
        Xtr_2d
    ).reshape(Ntr, T, F)

    Xva = scaler.transform(
        Xva_2d
    ).reshape(Nva, T, F)

    Xte = scaler.transform(
        Xte_2d
    ).reshape(Nte, T, F)

    joblib.dump(
        scaler,
        os.path.join(OUTDIR, "scaler.joblib")
    )

    # ========================================
    # 11. model
    # ========================================

    print("==> 訓練 GRU")

    model = build_gru(  (T, F)  )
    #model = build_lstm((X.shape[1], X.shape[2]))
    # 類別權重（不平衡）
    pos = max(ytr.sum(), 1); neg = max(len(ytr)-pos, 1)
    cw = {0: 0.5*len(ytr)/neg, 1: 0.5*len(ytr)/pos}
    cbs = [
        # EarlyStopping(...)
        # monitor="val_auprc"：監控驗證集的 AUPRC。
        # mode="max"：AUPRC 越大越好。
        # patience=12：如果連續 12 個 epoch 都沒變好，就提早停止訓練。
        # restore_best_weights=True：停止後把模型權重回復到「驗證表現最好」那一輪，避免最後幾輪過擬合。
        # ReduceLROnPlateau(...)
        # 一樣監控 val_auprc，而且越大越好。
        # patience=5：如果 5 個 epoch 沒進步，就把學習率調小。
        # factor=0.5：學習率乘以 0.5（減半）。
        # min_lr=1e-5：學習率最低不會小於 
        
        keras.callbacks.EarlyStopping(monitor="val_auprc", mode="max", patience=12, restore_best_weights=True),
        keras.callbacks.ReduceLROnPlateau(monitor="val_auprc", mode="max", patience=5, factor=0.5, min_lr=1e-5),
    ]
    history = model.fit(Xtr, ytr, validation_data=(Xva, yva),
                epochs=80,#epoch（訓練週期）是 深度學習訓練裡的一個基本單位
                batch_size=128, #batch_size=128 時，大概也就 1–2k 個樣本，每次參數更新時，送進模型的樣本數量。它是一個「訓練速度 vs 模型效果」之間的平衡點
                class_weight=cw,
                callbacks=cbs,
                verbose=2)
    loss_plot(history) #生成LOSS 圖 

    
    va_prob = model.predict(Xva, batch_size=256).ravel()
    te_prob = model.predict(Xte, batch_size=256).ravel()# 模型預測
    #model.predict: 驗證集每個樣本的 預測機率(0~1)

    print("==> 評估")
    #val_metrics = eval_block(yva, va_prob, "VAL")
    
    #test_metrics = eval_block(  yte,  te_prob, "TEST")

    val_metrics = eval_block2(
    yva,
    va_prob,
    "VAL"
    )

    val_thr = val_metrics["threshold"]

    test_metrics = eval_block2(
        yte,
        te_prob,
        "TEST",
        fixed_thr=val_thr
    )

    
  
  

    #plot_eval_metrics(val_metrics, tag="VAL")#驗證集
    #plot_eval_metrics(test_metrics, tag="TEST")#測試集
    plot_eval_compare(val_metrics, test_metrics)

    #plot_eval_radar([val_metrics, test_metrics], ["VAL","TEST"])
    print("==> Logistic Regression baseline")

    val_lr, test_lr, va_prob_lr, te_prob_lr = run_logistic_regression(
        Xtr, Xva, Xte,
        ytr, yva, yte
    )

    plot_eval_radar(
        [val_metrics, test_metrics, val_lr, test_lr],
        ["VAL_GRU", "TEST_GRU", "VAL_LR", "TEST_LR"]
    )
    print("==> Logistic Regression latest-data baseline")

    val_lr_latest, test_lr_latest, va_prob_lr_latest, te_prob_lr_latest = run_logistic_regression_latest(
        Xtr, Xva, Xte,
        ytr, yva, yte
    )
    plot_eval_radar(
        [val_metrics, test_metrics, val_lr, test_lr, val_lr_latest, test_lr_latest],
        ["VAL_GRU", "TEST_GRU", "VAL_LR", "TEST_LR", "VAL_LR_LATEST", "TEST_LR_LATEST"]
    )
    #logistic_odds_ratios(Xtr, ytr, feat_cols)#勝算比
        

    Xtr_xgb = Xtr.reshape(Xtr.shape[0], -1)
    Xva_xgb = Xva.reshape(Xva.shape[0], -1)
    Xte_xgb = Xte.reshape(Xte.shape[0], -1)

    val_xgb, test_xgb, va_prob_xgb, te_prob_xgb, xgb_model = run_xgboost(
        Xtr, Xva, Xte,
        ytr, yva, yte
    )
    cal_model = CalibratedClassifierCV(xgb_model, method='isotonic', cv=5)
    cal_model.fit(Xtr_xgb, ytr)
    # predict
    va_prob_cal = cal_model.predict_proba(Xva_xgb)[:, 1]
    te_prob_cal = cal_model.predict_proba(Xte_xgb)[:, 1]

    # 評估
    val_cal = eval_block(yva, va_prob_cal, "VAL_XGB_CAL")
    test_cal = eval_block(yte, te_prob_cal, "TEST_XGB_CAL")

        

    
    bootstrap_PLT(yte,te_prob)
    print("train hk_rate:", ytr.mean())
    print("val hk_rate:", yva.mean())
    print("test hk_rate:", yte.mean())


    #shap_show(model,Xtr,Xte,feat_cols)#不能用

    print("==> 輸出")
    mdir = OUTDIR
    model.save(os.path.join(mdir, "gru_hk_model.keras"))
    with open(os.path.join(mdir, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump({"val":val_metrics, "test":test_metrics, "features":feat_cols,
                    "seq_hours": SEQ_HOURS, "label_window":[LBL_FROM, LBL_TO],
                    "k_cutoff": K_CUTOFF}, f, ensure_ascii=False, indent=2)
    np.save(os.path.join(mdir, "test_prob.npy"), te_prob)#模型預測值
    np.save(os.path.join(mdir, "test_y.npy"),   yte)#這是測試集的 真實標籤 (ground truth)，通常是 0 或 1
    # 保存 X_train 和 X_test 用於 SHAP 分析
    np.save(os.path.join(mdir, "X_train.npy"), Xtr)  # shape (N_train, T, F)
    np.save(os.path.join(mdir, "X_test.npy"), Xte)   # shape (N_test, T, F)
    # 保存特徵名稱
    np.save(os.path.join(mdir, "feature_names.npy"), np.array(feat_cols))  # shape (F,)
    # te_prob:
    # 測試集每個樣本的 預測機率 (通常是 sigmoid 輸出的值，介於 0~1)。
    # 存成 test_prob.npy，方便後續做 ROC、PR、Calibration、SHAP 分析。

    # yte:
    # 測試集的 真實標籤 (ground truth)，通常是 0/1。
    # 存成 test_y.npy，對應 te_prob，用來計算 AUC、AUPRC、F1 等評估指標。

    print("完成，Artifacts 於：", os.path.abspath(mdir))
      
  

def build_wide(
    stays,
    hourly_frames,
    feature_builders=None,
    static_features=None,
    seq_hours=24
):

    frames = [
        x for x in hourly_frames
        if x is not None and not x.empty
    ]

    if not frames:
        raise RuntimeError(
            "No hourly features extracted."
        )

    long = pd.concat(
        frames,
        ignore_index=True
    )

    long["value"] = pd.to_numeric(
        long["value"],
        errors="coerce"
    )

    long["feature"] = (
        long["label"]
        .map(FEATURE_MAP)
    )

    long = long.dropna(
        subset=["feature"]
    )

    wide = (
        long
        .pivot_table(
            index=[
                "stay_id",
                "time_index"
            ],
            columns="feature",
            values="value",
            aggfunc="last"
        )
        .reset_index()
    )

    # ========================================
    # 完整 N × T grid
    # ========================================

    grid = (
        stays[["stay_id"]]
        .drop_duplicates()
        .assign(key=1)
        .merge(
            pd.DataFrame({
                "time_index":
                    np.arange(seq_hours),

                "key": 1
            }),
            on="key"
        )
        .drop(columns="key")
    )

    wide = (
        grid
        .merge(
            wide,
            on=[
                "stay_id",
                "time_index"
            ],
            how="left"
        )
        .sort_values([
            "stay_id",
            "time_index"
        ])
        .reset_index(drop=True)
    )

    # ========================================
    # Derived features
    # ========================================

    if feature_builders:

        for builder in feature_builders:
            wide = builder(wide)

    # ========================================
    # Static features
    # ========================================

    if static_features:

        static_df = (
            stays[
                ["stay_id"] +
                static_features
            ]
            .drop_duplicates("stay_id")
        )

        wide = wide.merge(
            static_df,
            on="stay_id",
            how="left"
        )

    feat_cols = [
        c for c in wide.columns
        if c not in [
            "stay_id",
            "time_index"
        ]
    ]

    wide[feat_cols] = (
        wide[feat_cols]
        .apply(
            pd.to_numeric,
            errors="coerce"
        )
    )

    return wide, feat_cols

def to_tensor(wide, feat_cols, seq_hours):

    wide = (
        wide
        .sort_values(["stay_id", "time_index"])
        .copy()
    )

    stay_ids = (
        wide["stay_id"]
        .drop_duplicates()
        .to_numpy()
    )

    N = len(stay_ids)
    T = seq_hours
    F = len(feat_cols)

    X = np.empty(
        (N, T, F),
        dtype=np.float32
    )

    groups = wide.groupby("stay_id")

    for i, sid in enumerate(stay_ids):

        block = (
            groups
            .get_group(sid)
            .sort_values("time_index")[feat_cols]
            .to_numpy(dtype=np.float32)
        )

        if len(block) != T:
            raise ValueError(
                f"stay_id={sid}: "
                f"expected {T} rows, got {len(block)}"
            )

        X[i] = block

    return X, pd.DataFrame({
        "stay_id": stay_ids
    })

if __name__ == "__main__":
   
    #main("POTA_TRAN_DATA20261014")#POTA_20251016、SODIUM_20251016
    main("real_data")
    #xgb_feature_importance2("POTA_TRAN_DATA20260429/fetch_labevents.csv", "POTASSIUM")
    #rfecv_feature_selection("POTA_TRAN_DATA20260429/fetch_labevents.csv", "POTASSIUM")
    #boruta_feature_selection("POTA_TRAN_DATA20260429/fetch_labevents.csv", "POTASSIUM")
    
    print("OK")
    