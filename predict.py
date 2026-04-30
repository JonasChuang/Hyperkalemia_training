from utility import FEATURE_MAP, to_hourly, to_hourly_drug
import os, json, joblib, numpy as np, pandas as pd, shap
import tensorflow as tf
from utility import SEQ_HOURS,LBL_FROM,LBL_TO,K_CUTOFF,RANDOM_STATE,TEST_SIZE,VAL_SIZE

def build_tensor_infer_old(char_h, lab_h, ur_h, drug_h, stays, scaler_path, feature_names_path):

    frames = [x for x in [char_h, lab_h, ur_h, drug_h] if x is not None and not x.empty]
    if not frames:
        raise RuntimeError("No hourly features extracted.")

    long = pd.concat(frames, ignore_index=True)
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    #long["label"] = long["label"].str.upper()
    long["label"] = long["label"].str.strip().str.upper()
    long["feature"] = long["label"].map(FEATURE_MAP)
    long = long.dropna(subset=["feature"])

    wide = (
        long.pivot_table(
            index=["stay_id", "time_index"],
            columns="feature",
            values="value",
            aggfunc="last"
        ).reset_index()
    )

    grid = (
        stays[["stay_id"]].drop_duplicates().assign(key=1)
        .merge(pd.DataFrame({"time_index": np.arange(SEQ_HOURS), "key": 1}), on="key")
        .drop("key", axis=1)
    )

    wide = (
        grid.merge(wide, on=["stay_id", "time_index"], how="left")
        .sort_values(["stay_id", "time_index"])
    )

    # ===== 關鍵：用訓練時的 feature 順序當標準 =====
    saved_feat_cols = np.load(feature_names_path, allow_pickle=True).tolist()

    current_feat_cols = [c for c in wide.columns if c not in ["stay_id", "time_index"]]

    # 補齊訓練時有、現在沒有的欄位
    for col in saved_feat_cols:
        if col not in wide.columns:
            wide[col] = np.nan

    # 丟掉現在多出來、但訓練時沒有的欄位
    extra_cols = [c for c in current_feat_cols if c not in saved_feat_cols]
    if extra_cols:
        wide = wide.drop(columns=extra_cols)

    # 重新排序成訓練時順序
    wide = wide[["stay_id", "time_index"] + saved_feat_cols]

    # 轉數值
    wide[saved_feat_cols] = wide[saved_feat_cols].apply(pd.to_numeric, errors="coerce")

    # 每個 stay 前向填補
    def _ff(g):
        g[saved_feat_cols] = g[saved_feat_cols].ffill()
        return g

    wide = wide.groupby("stay_id", as_index=False).apply(_ff).reset_index(drop=True)

    # 再用目前資料中位數補
    med = wide[saved_feat_cols].median(numeric_only=True)
    wide[saved_feat_cols] = wide[saved_feat_cols].fillna(med)

    # 若某些欄整欄都 NaN，median 仍會是 NaN，再補 0
    wide[saved_feat_cols] = wide[saved_feat_cols].fillna(0.0)

    # scaler transform
    scaler = joblib.load(scaler_path)
    wide[saved_feat_cols] = scaler.transform(wide[saved_feat_cols])

    # 組 tensor
    order = wide[["stay_id"]].drop_duplicates().values.ravel()
    N, T, F = len(order), SEQ_HOURS, len(saved_feat_cols)
    X = np.zeros((N, T, F), dtype=np.float32)

    for i, sid in enumerate(order):
        blk = (
            wide[wide["stay_id"] == sid]
            .sort_values("time_index")[saved_feat_cols]
            .values
        )
        X[i, :, :] = blk[:T]
    print("訓練特徵數:", len(saved_feat_cols))
    print("目前 real_data 特徵數:", len(current_feat_cols))
    print("缺少的特徵數:", sum([c not in current_feat_cols for c in saved_feat_cols]))
    print("多出的特徵數:", len(extra_cols))
    print("saved_feat_cols:", saved_feat_cols)
    print("current_feat_cols:", current_feat_cols)

    missing_cols = [c for c in saved_feat_cols if c not in current_feat_cols]
    extra_cols = [c for c in current_feat_cols if c not in saved_feat_cols]

    print("缺少欄位:", missing_cols)
    print("多出欄位:", extra_cols)

    return X, saved_feat_cols, pd.DataFrame({"stay_id": order})


def build_tensor_infer(char_h, lab_h, ur_h, drug_h, stays, scaler_path, feature_names_path):
    frames = [x for x in [char_h, lab_h, ur_h, drug_h] if x is not None and not x.empty]
    if not frames:
        raise RuntimeError("No hourly features extracted.")

    long = pd.concat(frames, ignore_index=True)
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long["label"] = long["label"].str.strip().str.upper()

    #long["feature"] = long["label"].map(FEATURE_MAP)
    #long = long.dropna(subset=["feature"])

    #long["feature"] = long["label"].map(FEATURE_MAP)
    #long = long.dropna(subset=["feature"])
    long["label"] = long["label"].astype(str).str.strip().str.upper()
    long["feature"] = long["label"].map(FEATURE_MAP)
    long = long.dropna(subset=["feature"])


    wide = (
        long.pivot_table(
            index=["stay_id", "time_index"],
            columns="feature",
            values="value",
            aggfunc="last"
        ).reset_index()
    )

    grid = (
        stays[["stay_id"]].drop_duplicates().assign(key=1)
        .merge(pd.DataFrame({"time_index": np.arange(SEQ_HOURS), "key": 1}), on="key")
        .drop("key", axis=1)
    )

    wide = (
        grid.merge(wide, on=["stay_id", "time_index"], how="left")
        .sort_values(["stay_id", "time_index"])
        .reset_index(drop=True)
    )

    saved_feat_cols = np.load(feature_names_path, allow_pickle=True).tolist()
    current_feat_cols = [c for c in wide.columns if c not in ["stay_id", "time_index"]]

    for col in saved_feat_cols:
        if col not in wide.columns:
            wide[col] = np.nan

    extra_cols = [c for c in current_feat_cols if c not in saved_feat_cols]
    if extra_cols:
        wide = wide.drop(columns=extra_cols)

    wide = wide[["stay_id", "time_index"] + saved_feat_cols]

    wide[saved_feat_cols] = wide[saved_feat_cols].apply(pd.to_numeric, errors="coerce")

    # 這裡不要再用 apply(_ff)
    wide[saved_feat_cols] = wide.groupby("stay_id")[saved_feat_cols].ffill()

    med = wide[saved_feat_cols].median(numeric_only=True)
    wide[saved_feat_cols] = wide[saved_feat_cols].fillna(med)
    wide[saved_feat_cols] = wide[saved_feat_cols].fillna(0.0)

    scaler = joblib.load(scaler_path)
    wide[saved_feat_cols] = scaler.transform(wide[saved_feat_cols])

    order = wide["stay_id"].drop_duplicates().to_numpy()
    N, T, F = len(order), SEQ_HOURS, len(saved_feat_cols)
    X = np.zeros((N, T, F), dtype=np.float32)

    for i, sid in enumerate(order):
        blk = (
            wide.loc[wide["stay_id"] == sid]
            .sort_values("time_index")[saved_feat_cols]
            .values
        )
        X[i, :, :] = blk[:T]
    print("訓練特徵數:", len(saved_feat_cols))
    print("目前 real_data 特徵數:", len(current_feat_cols))
    print("缺少的特徵數:", sum([c not in current_feat_cols for c in saved_feat_cols]))
    print("多出的特徵數:", len(extra_cols))
    print("saved_feat_cols:", saved_feat_cols)
    print("current_feat_cols:", current_feat_cols)
    missing_cols = [c for c in saved_feat_cols if c not in current_feat_cols]
    extra_cols = [c for c in current_feat_cols if c not in saved_feat_cols]

    print("缺少欄位:", missing_cols)
    print("多出欄位:", extra_cols)

    return X, saved_feat_cols, pd.DataFrame({"stay_id": order})




def predict_real():
    model_path = ".\\artifacts_hk_seq\\gru_hk_model.keras"
    scaler_path = ".\\artifacts_hk_seq\\scaler.joblib"
    feature_names_path = ".\\artifacts_hk_seq\\feature_names.npy"
    # 讀真實資料
    df_char = pd.read_csv(".\\real_data2\\fetch_chartevents.csv")
    df_lab = pd.read_csv(".\\real_data2\\fetch_labevents.csv")
    df_ur = pd.read_csv(".\\real_data2\\URINE.csv")
    df_drug = pd.read_csv(".\\real_data2\\高血鉀用藥.csv")
    stays = pd.read_csv(".\\real_data2\\adm.csv")   # 至少要有 stay_id, t0
    

    # 預測時一樣排除 potassium，避免 leakage
    df_lab = df_lab[~df_lab["label"].isin(["POTASSIUM", "Potassium, Whole Blood", "Potassium"])]

    char_h = to_hourly(df_char, stays)
    lab_h = to_hourly(df_lab, stays)
    ur_h = to_hourly(df_ur, stays)
    drug_h = to_hourly_drug(df_drug, stays)

    # X, feat_cols, sid_df = build_tensor_infer(
    #     char_h, lab_h, ur_h, drug_h, stays, scaler_path
    # )
    X, feat_cols, sid_df = build_tensor_infer(
    char_h,
    lab_h,
    ur_h,
    drug_h,
    stays,
    scaler_path,
    feature_names_path
    )
   

    model = tf.keras.models.load_model(model_path, compile=False)

    prob = model.predict(X, batch_size=256).ravel()
    #機率分布
    print("min:", prob.min())
    print("max:", prob.max())
    print("mean:", prob.mean())
    print((prob >= 0.5).mean())

    out = sid_df.copy()
    out["hk_risk"] = prob
    with open("./artifacts_hk_seq/metrics.json", "r") as f:
        metrics = json.load(f)

    thr = metrics["val"]["threshold"]   # 或 test

    out["hk_pred"] = (out["hk_risk"] >= thr).astype(int)

    #out["hk_pred"] = (out["hk_risk"] >= 0.5).astype(int)  # 先暫用 0.5


    def risk_group(p):
        if p >= 0.45:
            return "high"
        elif p >= 0.30:
            return "medium"
        else:
            return "low"

    out["hk_group"] = out["hk_risk"].apply(risk_group)

    out.to_csv("pred_result.csv", index=False, encoding="utf-8-sig")
    print(out.head())
    



if __name__ == "__main__":
   
    predict_real()
    
    print("OK")
