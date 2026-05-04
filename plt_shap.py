import matplotlib.pyplot as plt
from sklearn.metrics import roc_auc_score, average_precision_score, f1_score, brier_score_loss, precision_recall_curve,roc_curve
import os, json, joblib, numpy as np, pandas as pd, shap
OUTDIR = os.getenv("OUTDIR", "./artifacts_hk_seq")
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