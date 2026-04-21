"""
Pilot pull: fetch 10K rows from each SECOP II dataset on datos.gov.co
to validate schemas, field fill rates, and data quality.
"""
import json
import hashlib
import time
from datetime import datetime, timezone
from pathlib import Path
import requests
import pandas as pd

BASE = "https://www.datos.gov.co/resource"
RAW_DIR = Path("data/raw")

DATASETS = {
    "secop2_procesos": {
        "id": "p6dx-8zbt",
        "date_field": "fecha_de_publicacion_del",
    },
    "secop2_contratos": {
        "id": "jbjy-vk9h",
        "date_field": "fecha_de_firma",
    },
    "secop2_adiciones": {
        "id": "cb9c-h8sn",
        "date_field": "fecharegistro",
    },
}

LIMIT = 10000
DATE_FROM = "2019-01-01T00:00:00"
DATE_TO = "2025-01-01T00:00:00"


def fetch_pilot(name: str, cfg: dict) -> pd.DataFrame:
    dataset_id = cfg["id"]
    date_field = cfg["date_field"]
    url = f"{BASE}/{dataset_id}.json"
    params = {
        "$limit": LIMIT,
        "$offset": 0,
        "$where": f"{date_field} >= '{DATE_FROM}' AND {date_field} < '{DATE_TO}'",
        "$order": f"{date_field} ASC",
    }

    print(f"\n{'='*60}")
    print(f"Fetching {name} ({dataset_id}) — {LIMIT} rows")
    print(f"URL: {url}")
    print(f"Params: {params}")

    resp = requests.get(url, params=params, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    raw_bytes = resp.content

    print(f"Rows returned: {len(data)}")
    print(f"Bytes: {len(raw_bytes):,}")

    # Save raw
    out_dir = RAW_DIR / name
    out_dir.mkdir(parents=True, exist_ok=True)
    pilot_file = out_dir / "pilot_10k.json"
    pilot_file.write_bytes(raw_bytes)

    # Manifest
    sha = hashlib.sha256(raw_bytes).hexdigest()
    manifest = {
        "url": resp.url,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "byte_count": len(raw_bytes),
        "sha256": sha,
        "record_count": len(data),
        "is_pilot": True,
    }
    (out_dir / "manifest_pilot.json").write_text(json.dumps(manifest, indent=2))

    df = pd.DataFrame(data)
    return df


def analyze_schema(name: str, df: pd.DataFrame):
    print(f"\n{'='*60}")
    print(f"Schema analysis: {name}")
    print(f"Shape: {df.shape}")
    print(f"\nColumns ({len(df.columns)}):")
    print("-" * 80)

    fill_rates = []
    for col in df.columns:
        non_null = df[col].notna().sum()
        fill_pct = non_null / len(df) * 100
        # Check for sentinel "No Definido" etc.
        real_values = df[col][
            ~df[col].isin(["No Definido", "No definido", "No Aplica", "No aplica", ""])
        ].notna().sum()
        real_pct = real_values / len(df) * 100
        sample = str(df[col].dropna().iloc[0]) if non_null > 0 else "<empty>"
        if len(sample) > 60:
            sample = sample[:60] + "..."
        fill_rates.append({
            "column": col,
            "fill_%": round(fill_pct, 1),
            "real_%": round(real_pct, 1),
            "sample": sample,
        })

    fill_df = pd.DataFrame(fill_rates)
    print(fill_df.to_string(index=False))

    # Enum columns — show unique values for key categorical fields
    enum_cols_proc = [
        "modalidad_de_contratacion", "estado_del_procedimiento", "fase",
        "ordenentidad", "unidad_de_duracion", "adjudicado",
        "estado_de_apertura_del_proceso", "tipo_de_contrato",
    ]
    enum_cols_contr = [
        "modalidad_de_contratacion", "estado_contrato", "orden",
        "sector", "rama", "tipo_de_contrato", "tipodocproveedor",
        "es_grupo", "es_pyme", "origen_de_los_recursos", "destino_gasto",
    ]
    enum_cols_adic = ["tipo"]

    for col in enum_cols_proc + enum_cols_contr + enum_cols_adic:
        if col in df.columns:
            vals = df[col].value_counts().head(20)
            print(f"\nEnum values for '{col}' ({df[col].nunique()} unique):")
            for v, c in vals.items():
                print(f"  {v}: {c}")

    return fill_df


def main():
    results = {}
    for name, cfg in DATASETS.items():
        df = fetch_pilot(name, cfg)
        fill_df = analyze_schema(name, df)
        results[name] = {"df": df, "fill": fill_df}
        time.sleep(1)  # courtesy pause

    # Check join keys
    print(f"\n{'='*60}")
    print("JOIN KEY ANALYSIS")
    print("="*60)

    proc_df = results["secop2_procesos"]["df"]
    contr_df = results["secop2_contratos"]["df"]
    adic_df = results["secop2_adiciones"]["df"]

    if "id_del_portafolio" in proc_df.columns and "proceso_de_compra" in contr_df.columns:
        proc_ids = set(proc_df["id_del_portafolio"].dropna())
        contr_proc_ids = set(contr_df["proceso_de_compra"].dropna())
        overlap = proc_ids & contr_proc_ids
        print(f"Procesos id_del_portafolio unique: {len(proc_ids)}")
        print(f"Contratos proceso_de_compra unique: {len(contr_proc_ids)}")
        print(f"Overlap in pilot: {len(overlap)}")
        print(f"Sample proc IDs: {list(proc_ids)[:3]}")
        print(f"Sample contr proc IDs: {list(contr_proc_ids)[:3]}")

    if "id_contrato" in contr_df.columns and "id_contrato" in adic_df.columns:
        contr_ids = set(contr_df["id_contrato"].dropna())
        adic_contr_ids = set(adic_df["id_contrato"].dropna())
        overlap2 = contr_ids & adic_contr_ids
        print(f"\nContratos id_contrato unique: {len(contr_ids)}")
        print(f"Adiciones id_contrato unique: {len(adic_contr_ids)}")
        print(f"Overlap in pilot: {len(overlap2)}")

    # Department normalization preview
    print(f"\n{'='*60}")
    print("DEPARTMENT VALUES")
    if "departamento_entidad" in proc_df.columns:
        print("\nProcesos - departamento_entidad:")
        print(proc_df["departamento_entidad"].value_counts().head(15).to_string())
    if "departamento" in contr_df.columns:
        print("\nContratos - departamento:")
        print(contr_df["departamento"].value_counts().head(15).to_string())


if __name__ == "__main__":
    main()
