import os
import json
import pandas as pd
import numpy as np


def _load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return json.load(f)


def _basic_clean(df: pd.DataFrame) -> pd.DataFrame:
    # strip whitespace on all string columns
    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].astype(str).str.strip().replace("nan", np.nan)

    # drop rows where ALL values are null
    df = df.dropna(how="all")

    # drop columns where ALL values are null
    df = df.dropna(axis=1, how="all")

    return df


def _apply_replacements(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    replacements = cfg.get("value_replacements", {})
    for col, mapping in replacements.items():
        if col in df.columns:
            df[col] = df[col].replace(mapping)
    return df


def _apply_dtypes(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    dtype_map = cfg.get("dtype_mapping", {})
    for col, target_type in dtype_map.items():
        if col not in df.columns:
            continue
        try:
            if target_type == "datetime":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif target_type == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif target_type == "int":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif target_type == "string":
                df[col] = df[col].astype(str)
        except Exception as e:
            print(f"[WARN] Failed to convert {col} to {target_type}: {e}")
    return df


def _map_pol_pod(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    pol_col = cfg.get("pol_column")
    pod_col = cfg.get("pod_column")

    pol_pod_map_path = cfg.get("pol_pod_mapping_file")
    if not pol_pod_map_path or not os.path.exists(pol_pod_map_path):
        print("[INFO] No POL/POD mapping file found, skipping.")
        return df

    mapping_df = pd.read_csv(pol_pod_map_path)  # columns: code, full_name, continent
    mapping_df = mapping_df.drop_duplicates("code")

    code_to_full = dict(zip(mapping_df["code"], mapping_df["full_name"]))
    code_to_cont = dict(zip(mapping_df["code"], mapping_df["continent"]))

    pol_cont_col = "pol_continent"
    pod_cont_col = "pod_continent"

    if pol_col in df.columns:
        df["POL_full"] = df[pol_col].map(code_to_full)
        df[pol_cont_col] = df[pol_col].map(code_to_cont)

    if pod_col in df.columns:
        df["POD_full"] = df[pod_col].map(code_to_full)
        df[pod_cont_col] = df[pod_col].map(code_to_cont)

    if pol_cont_col in df.columns and pod_cont_col in df.columns:
        df["trade"] = df[pol_cont_col].fillna("UNK") + "-" + df[pod_cont_col].fillna("UNK")

    return df


def _scenario_metric(
    df: pd.DataFrame,
    metric_col: str | None,
    l_col: str | None,
    w_col: str | None,
    h_col: str | None,
    units_col: str | None,
    avg_file: str | None,
    model_col: str | None,
    result_col: str,
) -> pd.DataFrame:

    # ensure numeric
    for c in [metric_col, l_col, w_col, h_col, units_col]:
        if c and c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # load avg mapping if available
    avg_map = None
    if avg_file and os.path.exists(avg_file) and model_col and model_col in df.columns:
        avg_df = pd.read_csv(avg_file)   # columns: model, avg_metric
        avg_map = dict(zip(avg_df["model"], avg_df["avg_metric"]))

    metric_vals = df[metric_col] if metric_col and metric_col in df.columns else pd.Series([np.nan]*len(df))
    L = df[l_col] if l_col and l_col in df.columns else pd.Series([np.nan]*len(df))
    W = df[w_col] if w_col and w_col in df.columns else pd.Series([np.nan]*len(df))
    H = df[h_col] if h_col and h_col in df.columns else pd.Series([np.nan]*len(df))
    units = df[units_col] if units_col and units_col in df.columns else pd.Series([1]*len(df))
    model_vals = df[model_col] if model_col and model_col in df.columns else pd.Series([None]*len(df))

    result = []
    for m, l, w, h, u, mdl in zip(metric_vals, L, W, H, units, model_vals):
        # Scenario 1: metric present -> metric * units
        if pd.notna(m):
            result.append(m * (u if pd.notna(u) else 1))
            continue

        # Scenario 2: metric null but L,B,H present
        if pd.notna(l) and pd.notna(w) and (h_col is None or pd.notna(h)):
            calc = l * w * (h if h_col is not None else 1)
            result.append(calc * (u if pd.notna(u) else 1))
            continue

        # Scenario 3: all null but avg available
        if avg_map is not None and mdl in avg_map:
            avg_val = avg_map[mdl]
            result.append(avg_val * (u if pd.notna(u) else 1))
            continue

        # Scenario 4: nothing available
        result.append(0)

    df[result_col] = result
    return df


def run_silver_transform(
    customer_name: str,
    bronze_path: str,
    silver_output: str,
    config_path: str,
):
    print("===== SILVER TRANSFORM STARTED =====")
    print("Customer:", customer_name)
    print("Bronze path:", bronze_path)
    print("Config path:", config_path)

    cfg = _load_config(config_path)
    print("Config keys:", list(cfg.keys()))

    # -----------------------------------
    # Read bronze data (csv/parquet)
    # -----------------------------------
    files = [
        os.path.join(bronze_path, f)
        for f in os.listdir(bronze_path)
        if f.endswith(".parquet") or f.endswith(".csv")
    ]
    if not files:
        raise ValueError("No parquet/csv files found in bronze_path")

    dfs = []
    for f in files:
        if f.endswith(".parquet"):
            dfs.append(pd.read_parquet(f))
        else:
            dfs.append(pd.read_csv(f))
    df = pd.concat(dfs, ignore_index=True)
    print("Bronze rows:", len(df))

    # -----------------------------------
    # Rename columns → standard names
    # -----------------------------------
    col_map = cfg.get("column_mapping", {})
    if col_map:
        df = df.rename(columns=col_map)

    # read dimension + key columns from config
    dims = cfg.get("dimension_columns", {})
    len_col = dims.get("length")
    wid_col = dims.get("width")
    ht_col  = dims.get("height")
    cbm_col = dims.get("cbm")
    sqm_col = dims.get("sqm")
    units_col = dims.get("units")

    model_col = cfg.get("model_column")

    # -----------------------------------
    # Cleaning + standard rules
    # -----------------------------------
    df = _basic_clean(df)
    df = _apply_replacements(df, cfg)
    df = _apply_dtypes(df, cfg)
    df = _map_pol_pod(df, cfg)

    # -----------------------------------
    # CBM & SQM scenario logic
    # -----------------------------------
    df = _scenario_metric(
        df,
        metric_col=cbm_col,
        l_col=len_col,
        w_col=wid_col,
        h_col=ht_col,
        units_col=units_col,
        avg_file=cfg.get("avg_cbm_mapping_file"),
        model_col=model_col,
        result_col="cbm_result",
    )

    df = _scenario_metric(
        df,
        metric_col=sqm_col,
        l_col=len_col,
        w_col=wid_col,
        h_col=None,  # often sqm is L*W only
        units_col=units_col,
        avg_file=cfg.get("avg_sqm_mapping_file"),
        model_col=model_col,
        result_col="sqm_result",
    )

    # -----------------------------------
    # Save to silver output
    # -----------------------------------
    os.makedirs(silver_output, exist_ok=True)
    out_file = os.path.join(silver_output, "silver_clean.parquet")
    df.to_parquet(out_file, index=False)

    print(f"[SUCCESS] Silver data written to: {out_file}")
    print("===== SILVER TRANSFORM COMPLETED =====")



import argparse
import os
from customers.common_silver_engine import run_silver_transform
