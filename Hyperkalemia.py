"""
高血鉀預測 模型建立

0–24h 每小時序列抽取（排除 K）→ GRU 訓練
MIMIC-IV｜0–24h 每小時序列（排除 K）→ GRU → 預測 24–48h 高血鉀
- 資料來源：chartevents（vitals）、labevents（labs，不含Potassium）、outputevents（尿量）
- 對齊基準：ICU intime（可改成入院 admittime，請見註解）
- 標籤：在 24–48h 內是否首次出現 K >= K_CUTOFF（預設 5.5 mmol/L）
- 模型：GRU(64) + Dropout，early stopping 以 val AUPRC 監控
- 指標：AUC, AUPRC, F1@PR最佳閾值, Brier
輸出：./artifacts_hk_seq/ 下存模型與指標
"""
from utility import FEATURE_MAP, to_hourly, to_hourly_drug,SEQ_HOURS,LBL_FROM,LBL_TO,K_CUTOFF,RANDOM_STATE

import os, json, joblib, numpy as np, pandas as pd, shap
from typing import Dict, Tuple, List
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, average_precision_score, f1_score, brier_score_loss, precision_recall_curve,roc_curve
import tensorflow as tf
from keras import layers
import keras
import matplotlib.pyplot as plt
#from tool import sqlalchmy_raw_sql

OUTDIR = os.getenv("OUTDIR", "./artifacts_hk_seq")

QUICK_TEST = os.getenv("QUICK_TEST", "false").lower() == "true"

np.random.seed(RANDOM_STATE)
tf.random.set_seed(RANDOM_STATE)


def build_tensor2(char_h: pd.DataFrame, lab_h: pd.DataFrame, ur_h: pd.DataFrame,drug_h: pd.DataFrame, stays: pd.DataFrame
                  ) -> Tuple[np.ndarray, List[str], pd.DataFrame]:
    """
    將 long format（char/lab/output 每筆事件一列）轉為三維張量 (N × T × F)
    並處理：label→feature 映射、時間網格補齊、缺值處理、標準化。
    """

    # 1) 合併來源
    frames = [x for x in [char_h, lab_h, ur_h,drug_h] if x is not None and not x.empty]
    if not frames:
        raise RuntimeError("No hourly features extracted.")
    long = pd.concat(frames, ignore_index=True)

    # 2) 安全數值轉換（避免 '103'、'nan'、'' 汙染）
    #    如果來源欄叫 'value'，確保它是數字；無法轉的直接變 NaN
    long["value"] = pd.to_numeric(long["value"], errors="coerce")

    # 3) label → 統一的 feature 名稱；丟掉 map 不到的
    long["feature"] = long["label"].map(FEATURE_MAP)
    long = long.dropna(subset=["feature"])

    # 4) 長轉寬（同一小時多筆取最後一筆）
    wide = (long
            .pivot_table(index=["stay_id","time_index"],
                         columns="feature", values="value", aggfunc="last")
            .reset_index())

    # 5) 補滿 0..SEQ_HOURS-1 的時間格
    grid = (stays[["stay_id"]].drop_duplicates().assign(key=1)
            .merge(pd.DataFrame({"time_index": np.arange(SEQ_HOURS), "key": 1}), on="key")
            .drop("key", axis=1))
    wide = (grid.merge(wide, on=["stay_id","time_index"], how="left")
                 .sort_values(["stay_id","time_index"]))

    # 6) 缺值處理：先 per-stay 前向填補，再用整體中位數補
    feat_cols = [c for c in wide.columns if c not in ["stay_id","time_index"]]

    # 再保險一次：把可能殘留的字串數字轉成 float
    wide[feat_cols] = wide[feat_cols].apply(pd.to_numeric, errors="coerce")

    def _ff(g):#就是 LOCF（向前補值）
        g[feat_cols] = g[feat_cols].ffill()
        return g
    wide = wide.groupby("stay_id", as_index=False).apply(_ff).reset_index(drop=True)

    # 刪除「整欄都是 NaN」的特徵（避免 median 出問題）
    all_nan_cols = [c for c in feat_cols if wide[c].isna().all()]
    if all_nan_cols:
        wide = wide.drop(columns=all_nan_cols)
        feat_cols = [c for c in feat_cols if c not in all_nan_cols]

    # 這時再算中位數並補
    med = wide[feat_cols].median(numeric_only=True)
    wide[feat_cols] = wide[feat_cols].fillna(med)


    wide[feat_cols] = wide[feat_cols].astype("float32")


    # 8) 組 N×T×F 張量
    order = wide[["stay_id"]].drop_duplicates().values.ravel()
    N, T, F = len(order), SEQ_HOURS, len(feat_cols)
    X = np.zeros((N, T, F), dtype=np.float32)
    for i, sid in enumerate(order):
        blk = (wide[wide["stay_id"] == sid]
               .sort_values("time_index")[feat_cols]
               .values)
        X[i, :, :] = blk[:T]

    return X, feat_cols, pd.DataFrame({"stay_id": order})

def build_tensor3(char_h, lab_h, ur_h, drug_h, stays):

    frames = [x for x in [char_h, lab_h, ur_h, drug_h] if x is not None and not x.empty]
    if not frames:
        raise RuntimeError("No hourly features extracted.")

    long = pd.concat(frames, ignore_index=True)
    long["value"] = pd.to_numeric(long["value"], errors="coerce")

    long["feature"] = long["label"].map(FEATURE_MAP)
    long = long.dropna(subset=["feature"])

    wide = (
        long.pivot_table(
            index=["stay_id", "time_index"],
            columns="feature",
            values="value",
            aggfunc="last"
        )
        .reset_index()
    )

    grid = (
        stays[["stay_id"]]
        .drop_duplicates()
        .assign(key=1)
        .merge(
            pd.DataFrame({
                "time_index": np.arange(SEQ_HOURS),
                "key": 1
            }),
            on="key"
        )
        .drop("key", axis=1)
    )

    wide = (
        grid.merge(wide, on=["stay_id", "time_index"], how="left")
        .sort_values(["stay_id", "time_index"])
    )

    # ===== 加入年齡 =====
    age_df = stays[["stay_id", "age"]].drop_duplicates()
    age_df["age"] = pd.to_numeric(age_df["age"], errors="coerce")

    wide = wide.merge(age_df, on="stay_id", how="left")

    feat_cols = [c for c in wide.columns if c not in ["stay_id", "time_index"]]

    wide[feat_cols] = wide[feat_cols].apply(pd.to_numeric, errors="coerce")

    def _ff(g):
        g[feat_cols] = g[feat_cols].ffill()
        return g

    wide = wide.groupby("stay_id", as_index=False).apply(_ff).reset_index(drop=True)

    all_nan_cols = [c for c in feat_cols if wide[c].isna().all()]
    if all_nan_cols:
        wide = wide.drop(columns=all_nan_cols)
        feat_cols = [c for c in feat_cols if c not in all_nan_cols]

    med = wide[feat_cols].median(numeric_only=True)
    wide[feat_cols] = wide[feat_cols].fillna(med)

    wide[feat_cols] = wide[feat_cols].astype("float32")

    order = wide[["stay_id"]].drop_duplicates().values.ravel()
    N, T, F = len(order), SEQ_HOURS, len(feat_cols)

    X = np.zeros((N, T, F), dtype=np.float32)

    for i, sid in enumerate(order):
        blk = (
            wide[wide["stay_id"] == sid]
            .sort_values("time_index")[feat_cols]
            .values
        )
        X[i, :, :] = blk[:T]

    return X, feat_cols, pd.DataFrame({"stay_id": order})
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

def build_lstm2(input_shape) -> keras.Model:
    inp = keras.Input(shape=input_shape, name="seq")
    x = layers.Masking(mask_value=0.0)(inp)
    x = layers.LSTM(64, return_sequences=False)(x)
    x = layers.Dropout(0.3)(x)
    out = layers.Dense(1, activation="sigmoid")(x)
    model = keras.Model(inp, out)
    model.compile(
        optimizer=keras.optimizers.Adam(1e-3),
        # 原本：loss="binary_crossentropy",
        loss=focal_loss(gamma=2.0, alpha=0.25),   # ← 改用 focal loss
        metrics=[keras.metrics.AUC(name="auc"), keras.metrics.AUC(curve="PR", name="auprc")]
    )
    return model

def best_pr_thr(y_true, prob):
    p, r, thr = precision_recall_curve(y_true, prob)
    f1 = 2 * p * r / (p + r + 1e-9)
    ix = np.argmax(f1)
    return thr[ix]

def eval_block(y, p, tag):
    #用最佳 threshold 評估模型在某個資料集上的表現，然後把機率轉成 0/1 預測
    thr = best_pr_thr(y, p)
    yhat = (p >= thr).astype(int)
    res = {
        "threshold": float(thr),
        "AUC": float(roc_auc_score(y, p)),
        "AUPRC": float(average_precision_score(y, p)),
        "F1": float(f1_score(y, yhat)),
        "Brier": float(brier_score_loss(y, p))#Brier score（校準程度)
    }
    print(f"\n[{tag}] thr={res['threshold']:.3f}  AUC={res['AUC']:.4f}  AUPRC={res['AUPRC']:.4f}  F1={res['F1']:.4f}  Brier={res['Brier']:.4f}")

    #列印ROC 曲線， 保留所有臨界點（不要丟掉中間點）
    fpr, tpr, _ = roc_curve(y, p, drop_intermediate=False)
    plt.figure(figsize=(6,6))
    
    plt.plot(fpr, tpr, label=f"{tag} ROC (AUC={res['AUC']:.3f})")
    plt.plot([0,1],[0,1],'k--')  # 參考線
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title(f"ROC Curve - {tag}")
    plt.legend(loc="lower right")
    plt.grid(True)
    plt.show()

    return res

def plot_eval_radar(res_list, tags):#雷達圖
    metrics = ["AUC","AUPRC","F1","Brier"]
    N = len(metrics)
    angles = np.linspace(0, 2*np.pi, N, endpoint=False).tolist()
    angles += angles[:1]  # close circle

    plt.figure(figsize=(6,6))
    ax = plt.subplot(111, polar=True)

    for res, tag in zip(res_list, tags):
        values = [res[m] for m in metrics]
        values += values[:1]
        ax.plot(angles, values, marker="o", label=tag)
        ax.fill(angles, values, alpha=0.25)

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(metrics)
    ax.set_ylim(0,1)
    plt.legend(loc="upper right")
    plt.title("Model Evaluation Metrics")
    plt.show()

def plot_eval_metrics(res, tag="Validation"):#長條圖
    metrics = ["AUC","AUPRC","F1","Brier"]
    values = [res[m] for m in metrics]

    plt.figure(figsize=(6,4))
    bars = plt.bar(metrics, values, color=["skyblue","lightgreen","orange","lightcoral"])
    plt.title(f"{tag} Metrics (thr={res['threshold']:.3f})")
    plt.ylim(0,1)
    plt.ylabel("Score")

    for bar, val in zip(bars, values):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height()+0.01,
                 f"{val:.3f}", ha="center", va="bottom")

    plt.show()


def shap_show(model,Xtr,Xte,feat_cols):

    bg_n = min(128, len(Xtr))
    X_bg = Xtr[np.random.default_rng(42).choice(len(Xtr), size=bg_n, replace=False)]

    # 取一批要解釋的樣本（這裡用測試集前 64 筆示範）
    k = min(64, len(Xte))
    X_explain = Xte[:k]

    # 建立 explainer（新式 API 優先；不行就退回 DeepExplainer）
    try:
        explainer = shap.Explainer(model, X_bg)   # 需 shap 新版
        expl = explainer(X_explain)               # shap.Explanation
        sv = expl.values                          # (k, T, F)
        base = expl.base_values                   # (k,) 或標量
    except Exception as Error:
        print(str(Error))
        explainer = shap.DeepExplainer(model, X_bg)   # 舊版 API
        sv_list = explainer.shap_values(X_explain)    # list/ndarray
        sv = sv_list[0] if isinstance(sv_list, list) else sv_list  # (k, T, F)
        base = getattr(explainer, "expected_value", 0.0)
    print("SHAP values shape:", sv.shape) 

    # 各特徵的平均 |SHAP|（跨樣本與時間）
    imp_feat = np.mean(np.abs(sv), axis=(0,1))   # -> (F,)
    rank = np.argsort(imp_feat)[::-1]
    top = min(15, len(feat_cols))

    plt.figure(figsize=(6, 0.35*top + 2))
    plt.barh(range(top), imp_feat[rank[:top]][::-1])
    plt.yticks(range(top), [feat_cols[i] for i in rank[:top]][::-1])
    plt.xlabel("Mean |SHAP| (global importance)")
    plt.title("Top features (aggregated over time)")
    plt.tight_layout(); plt.savefig(os.path.join(OUTDIR, "shap_global_features.png"), dpi=150)
    plt.close()

    T = sv.shape[1]
    imp_time = np.mean(np.abs(sv), axis=(0,2))   # -> (T,)

    plt.figure(figsize=(7,3))
    plt.plot(range(T), imp_time, marker="o")
    plt.xlabel("Hour since ICU intime (t)")
    plt.ylabel("Mean |SHAP|")
    plt.title("When does the model care most?")
    plt.tight_layout(); plt.savefig(os.path.join(OUTDIR, "shap_time_importance.png"), dpi=150)
    plt.close()

    case_idx = 0
    M = sv[case_idx]   # (T, F) 這個病人的 SHAP 矩陣
    vmin, vmax = -np.max(np.abs(M)), np.max(np.abs(M))

    plt.figure(figsize=(9, 0.35*len(feat_cols) + 2))
    plt.imshow(M.T, aspect="auto", origin="lower", vmin=vmin, vmax=vmax)
    plt.colorbar(label="SHAP value (+ drives risk up)")
    plt.yticks(range(len(feat_cols)), feat_cols)
    plt.xlabel("Hour"); plt.ylabel("Feature")
    plt.title(f"Case #{case_idx}: SHAP heatmap (time × feature)")
    plt.tight_layout(); plt.savefig(os.path.join(OUTDIR, "shap_heatmap_case0.png"), dpi=150)
    plt.close()


def bootstrap_PLT(yte,te_prob):
    #ci_table = bootstrap_ci(yte, te_prob)

    # 輸出表格
    stats = bootstrap_ci(yte, te_prob)

    # 🔥 正確 boxplot
    plt.figure(figsize=(8,5))
    plt.boxplot([
        stats["AUC"],
        stats["AUPRC"],
        stats["F1"],
        stats["Brier"]
    ])

    plt.xticks([1,2,3,4], ["AUC","AUPRC","F1","Brier"])
    plt.title("Bootstrap Distribution of Metrics")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.show()

def bootstrap_ci(y, p, n_bootstrap=1000):
    rng = np.random.RandomState(42)

    stats = {
        "AUC": [],
        "AUPRC": [],
        "F1": [],
        "Brier": []
    }

    for _ in range(n_bootstrap):
        idx = rng.randint(0, len(y), len(y))
        y_b, p_b = y[idx], p[idx]

        thr = 0.5
        yhat_b = (p_b >= thr).astype(int)

        stats["AUC"].append(roc_auc_score(y_b, p_b))
        stats["AUPRC"].append(average_precision_score(y_b, p_b))
        stats["F1"].append(f1_score(y_b, yhat_b))
        stats["Brier"].append(brier_score_loss(y_b, p_b))

    return stats   # 🔥 改這裡（回傳 distribution）


def main(flg):
    os.makedirs(OUTDIR, exist_ok=True)
    print("==> 建 cohort/時窗、抓特徵事件")
    #data=make_cohort()
    df_char=pd.read_csv(f"./{flg}/fetch_chartevents.csv")
    df_lab = pd.read_csv(f"./{flg}/fetch_labevents.csv")
    df_ur = pd.read_csv(f"./{flg}/URINE.csv")
    #stays = pd.read_csv(f"Hyperkalemia/CSV/{flg}.csv")
    
    df_lab = df_lab[~df_lab['label'].isin(['POTASSIUM', 'Potassium, Whole Blood','Potassium'])]
    #select hadm_id ,admittime ,dischtime ,aki,potassium ,low_sodium  from z_ckd_adm zca 
    ydf = pd.read_csv(f"./{flg}/POTASSIUM.csv")#要預測的結果
    stays = pd.read_csv(f"./{flg}/adm.csv")#stays 是模型的「全部樣本」
    df_drug = pd.read_csv(f"./{flg}/高血鉀用藥.csv")

  
    print("==> 轉每小時網格（0–24h）")
    char_h = to_hourly(df_char, stays)#將不規則時間序列對齊到固定小時格
    lab_h  = to_hourly(df_lab,  stays)
    ur_h   = to_hourly(df_ur,   stays)
    drug_h = to_hourly_drug(df_drug, stays)

    X, feat_cols, sid_df = build_tensor3(char_h, lab_h, ur_h,drug_h, stays)# 將 long format（char/lab/output 每筆事件一列）轉為三維張量 (N × T × F)

    print("==> 取標籤（24–48h 高血鉀）")
 
    y = sid_df.merge(ydf, on="stay_id", how="left")["y_hk"].fillna(0).astype(int).values #要預測的結果

    # 切 Train/Val/Test（by stay）
    rng = np.random.default_rng(RANDOM_STATE)
    idx = np.arange(len(y))
    rng.shuffle(idx)

    TRAIN_P = float(os.getenv("TRAIN_P", "0.6"))  # 訓練集比例 70%
    remaining = max(0.0, 1.0 - TRAIN_P)
    VAL_P = remaining / 2.0
    TEST_P = remaining - VAL_P

    n_test = max(int(len(idx) * TEST_P), 1)
    n_val  = max(int(len(idx) * VAL_P), 1)
    ###############################################
    te_idx = idx[:n_test]
    va_idx = idx[n_test:n_test + n_val]
    tr_idx = idx[n_test + n_val:]


    Xtr, Xva, Xte = X[tr_idx], X[va_idx], X[te_idx]
    ytr, yva, yte = y[tr_idx], y[va_idx], y[te_idx]
    #############################################
    Ntr, T, F = Xtr.shape
    Nva = Xva.shape[0]
    Nte = Xte.shape[0]

    Xtr_2d = Xtr.reshape(-1, F)
    Xva_2d = Xva.reshape(-1, F)
    Xte_2d = Xte.reshape(-1, F)

    scaler = StandardScaler()
    scaler.fit(Xtr_2d)

    Xtr = scaler.transform(Xtr_2d).reshape(Ntr, T, F)
    Xva = scaler.transform(Xva_2d).reshape(Nva, T, F)
    Xte = scaler.transform(Xte_2d).reshape(Nte, T, F)
    joblib.dump(scaler, os.path.join(OUTDIR, "scaler.joblib"))

    
    print("==> 訓練 GRU")
    model = build_gru((X.shape[1], X.shape[2]))
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
    model.fit(Xtr, ytr, validation_data=(Xva, yva),
               epochs=80,#epoch（訓練週期）是 深度學習訓練裡的一個基本單位
               batch_size=128, #batch_size=128 時，大概也就 1–2k 個樣本，每次參數更新時，送進模型的樣本數量。它是一個「訓練速度 vs 模型效果」之間的平衡點
               class_weight=cw,
               callbacks=cbs,
               verbose=2)

    
    va_prob = model.predict(Xva, batch_size=256).ravel()
    te_prob = model.predict(Xte, batch_size=256).ravel()# 模型預測
    #model.predict: 驗證集每個樣本的 預測機率(0~1)

    print("==> 評估")
    val_metrics = eval_block(yva, va_prob, "VAL")
    test_metrics= eval_block(yte, te_prob, "TEST")# 真實標籤
    plot_eval_metrics(val_metrics, tag="VAL")#驗證集
    plot_eval_metrics(test_metrics, tag="TEST")#測試集

    plot_eval_radar([val_metrics, test_metrics], ["VAL","TEST"])
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

if __name__ == "__main__":
   
    main("POTA_TRAN_DATA20260429")#POTA_20251016、SODIUM_20251016
    
    print("OK")
    
SQL="""

select
CASE WHEN a.drug LIKE '%lisinopril%' THEN 'RAASi'
    WHEN a.drug LIKE '%enalapril%' THEN 'RAASi'
    WHEN a.drug LIKE '%ramipril%' THEN 'RAASi'
    WHEN a.drug LIKE '%losartan%' THEN 'RAASi'
    WHEN a.drug LIKE '%irbesartan%' THEN 'RAASi'
    WHEN a.drug LIKE '%spironolactone%' THEN 'K_SPARING'
    WHEN a.drug LIKE '%eplerenone%' THEN 'K_SPARING'
    WHEN a.drug LIKE '%amiloride%' THEN   'K_SPARING'
    WHEN a.drug LIKE '%triamterene%' THEN 'K_SPARING'
    WHEN a.drug LIKE '%heparin%' THEN 'HEPARIN'
    WHEN a.drug LIKE '%enoxaparin%' THEN 'HEPARIN'
    WHEN a.drug LIKE '%ibuprofen%' THEN 'NSAID'
    WHEN a.drug LIKE '%ketorolac%' THEN 'NSAID'
    WHEN a.drug LIKE '%furosemide%' THEN 'LOOP'
    WHEN a.drug LIKE '%bumetanide%' THEN 'LOOP'
    WHEN a.drug LIKE '%torsemide%' THEN 'LOOP'
    WHEN a.drug LIKE '%hydrochlorothiazide%' THEN 'THIAZIDE'
    WHEN a.drug LIKE '%chlorthalidone%' THEN 'THIAZIDE'
    END



 ,a.* from prescriptions a
inner join z_k_adm b on(b.subject_id=a.subject_id and b.hadm_id=a.hadm_id)
where (LOWER(a.drug) like '%lisinopril%'
or LOWER(a.drug) like '%enalapril%'
or LOWER(a.drug) like '%ramipril%'
or LOWER(a.drug) like '%losartan%'
or LOWER(a.drug) like '%valsartan%'
or LOWER(a.drug) like '%irbesartan%'
or LOWER(a.drug) like '%spironolactone%'
or LOWER(a.drug) like '%eplerenone%'
or LOWER(a.drug) like '%amiloride%'
or LOWER(a.drug) like '%triamterene%'

or LOWER(a.drug) like '%heparin%'
or LOWER(a.drug) like '%enoxaparin%'
or LOWER(a.drug) like '%ibuprofen%'
or LOWER(a.drug) like '%ketorolac%'
or LOWER(a.drug) like '%furosemide%'
or LOWER(a.drug) like '%bumetanide%'
or LOWER(a.drug) like '%torsemide%'
or LOWER(a.drug) like '%hydrochlorothiazide%'
or LOWER(a.drug) like '%chlorthalidone%'
)
"""

