"""Build covariate tables from downloaded raw data.

Produces:
  data/covariates/divipola_crosswalk.parquet
  data/covariates/municipality_covariates.parquet
  data/covariates/consortium_members.parquet
"""

import json
import math
import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "covariates" / "raw"
OUT_DIR = DATA_DIR / "covariates"

# ── Normalization helpers ─────────────────────────────────────────


def _strip_accents(s: str) -> str:
    """Remove accents and normalize to uppercase."""
    nfkd = unicodedata.normalize("NFKD", str(s))
    return "".join(c for c in nfkd if not unicodedata.combining(c)).upper().strip()


# Known mismatches between SECOP municipality names and DIVIPOLA
MANUAL_OVERRIDES: dict[tuple[str, str], str] = {
    # (dept_norm, muni_norm) -> codigo_divipola
    # "GUADALAJARA DE BUGA" in SECOP -> "BUGA" in DIVIPOLA
    ("VALLE DEL CAUCA", "GUADALAJARA DE BUGA"): "76111",
    ("VALLE DEL CAUCA", "BUGA"): "76111",
    ("CUNDINAMARCA", "AGUA DE DIOS"): "25001",
    ("BOLIVAR", "SANTA CRUZ DE MOMPOX"): "13468",
    ("BOLIVAR", "MOMPOX"): "13468",
    ("NARINO", "SAN ANDRES DE TUMACO"): "52835",
    ("NARINO", "TUMACO"): "52835",
    ("BOYACA", "VILLA DE LEYVA"): "15407",
    ("BOYACA", "VILLA DE LEIVA"): "15407",
    ("SANTANDER", "BARRANCABERMEJA"): "68081",
    ("DISTRITO CAPITAL DE BOGOTA", "BOGOTA"): "11001",
    ("DISTRITO CAPITAL DE BOGOTA", "BOGOTA D.C."): "11001",
    ("DISTRITO CAPITAL DE BOGOTA", "BOGOTA, D.C."): "11001",
    ("SAN ANDRES, PROVIDENCIA Y SANTA CATALINA", "SAN ANDRES"): "88001",
    ("SAN ANDRES, PROVIDENCIA Y SANTA CATALINA", "PROVIDENCIA"): "88564",
}


# ── DIVIPOLA crosswalk ────────────────────────────────────────────


def build_divipola_crosswalk(existing_munis: pd.DataFrame) -> pd.DataFrame:
    """Match (department_norm, municipality_norm) -> codigo_divipola.

    existing_munis must have columns: department_norm, municipality_norm
    Returns DataFrame with columns: department_norm, municipality_norm, codigo_divipola
    """
    divipola_path = RAW_DIR / "divipola.json"
    if not divipola_path.exists():
        print("  WARNING: divipola.json not found, returning empty crosswalk", flush=True)
        return pd.DataFrame(columns=["department_norm", "municipality_norm", "codigo_divipola"])

    with open(divipola_path, encoding="utf-8") as f:
        raw = json.load(f)

    divipola = pd.DataFrame(raw)

    # Build DIVIPOLA lookup from API data
    divi_lookup: dict[tuple[str, str], str] = {}
    divi_by_dept: dict[str, list[tuple[str, str]]] = {}

    if len(divipola) > 0:
        # Identify columns (Socrata field names vary across datasets)
        # Known patterns: codigo_municipio / cod_mpio, nombre_municipio / nom_mpio,
        # nombre_departamento / dpto, codigo_departamento / cod_dpto
        dept_col = None
        muni_col = None
        code_col = None
        dept_name_col = None
        for col in divipola.columns:
            cl = col.lower()
            if cl in ("cod_mpio", "codigo_municipio", "codmpio", "codigo_mpio"):
                code_col = col
            elif cl in ("nom_mpio", "nombre_municipio", "municipio", "nommpio"):
                muni_col = col
            elif cl in ("dpto", "nombre_departamento", "departamento", "nom_dpto"):
                dept_name_col = col
            elif cl in ("cod_dpto", "codigo_departamento", "coddpto"):
                dept_col = col

        if not all([code_col, muni_col, dept_name_col]):
            print(f"  WARNING: Could not identify DIVIPOLA columns: {divipola.columns.tolist()}", flush=True)
        else:
            divipola["dept_norm"] = divipola[dept_name_col].apply(_strip_accents)
            divipola["muni_norm"] = divipola[muni_col].apply(_strip_accents)
            divipola["codigo_divipola"] = divipola[code_col].astype(str)

            for _, r in divipola.iterrows():
                divi_lookup[(r["dept_norm"], r["muni_norm"])] = r["codigo_divipola"]

            for (dept, muni), code in divi_lookup.items():
                divi_by_dept.setdefault(dept, []).append((muni, code))

    # Unique munis from refpop
    unique_munis = (
        existing_munis[["department_norm", "municipality_norm"]]
        .drop_duplicates()
        .dropna()
    )

    results = []
    n_exact = 0
    n_substr = 0
    n_manual = 0
    n_miss = 0

    for _, r in unique_munis.iterrows():
        dept = _strip_accents(str(r["department_norm"]))
        muni = _strip_accents(str(r["municipality_norm"]))
        key = (dept, muni)

        # 1. Manual override
        if key in MANUAL_OVERRIDES:
            results.append({"department_norm": r["department_norm"],
                            "municipality_norm": r["municipality_norm"],
                            "codigo_divipola": MANUAL_OVERRIDES[key]})
            n_manual += 1
            continue

        # 2. Exact match
        if key in divi_lookup:
            results.append({"department_norm": r["department_norm"],
                            "municipality_norm": r["municipality_norm"],
                            "codigo_divipola": divi_lookup[key]})
            n_exact += 1
            continue

        # 3. Substring match within same department
        found = False
        if dept in divi_by_dept:
            candidates = divi_by_dept[dept]
            # Try: SECOP name contains DIVIPOLA name or vice versa
            for divi_muni, code in candidates:
                if divi_muni in muni or muni in divi_muni:
                    results.append({"department_norm": r["department_norm"],
                                    "municipality_norm": r["municipality_norm"],
                                    "codigo_divipola": code})
                    n_substr += 1
                    found = True
                    break
            # Try: last word match (e.g. "GUADALAJARA DE BUGA" -> "BUGA")
            if not found:
                muni_last = muni.split()[-1] if muni else ""
                for divi_muni, code in candidates:
                    divi_last = divi_muni.split()[-1] if divi_muni else ""
                    if muni_last == divi_last and len(muni_last) > 3:
                        results.append({"department_norm": r["department_norm"],
                                        "municipality_norm": r["municipality_norm"],
                                        "codigo_divipola": code})
                        n_substr += 1
                        found = True
                        break

        if not found:
            results.append({"department_norm": r["department_norm"],
                            "municipality_norm": r["municipality_norm"],
                            "codigo_divipola": None})
            n_miss += 1

    total = n_exact + n_substr + n_manual + n_miss
    match_rate = (total - n_miss) / max(total, 1) * 100
    print(f"  DIVIPOLA crosswalk: {total} munis, exact={n_exact}, substr={n_substr}, "
          f"manual={n_manual}, miss={n_miss} ({match_rate:.1f}% match rate)", flush=True)

    return pd.DataFrame(results)


# ── Municipality covariates ───────────────────────────────────────


def _load_pdet() -> set[str]:
    """Load PDET municipality codes."""
    path = RAW_DIR / "pdet.json"
    if not path.exists():
        return set()
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    df = pd.DataFrame(raw)
    # Find the municipality code column (cod_muni, codigo_municipio, etc.)
    for col in df.columns:
        cl = col.lower()
        if "muni" in cl and "cod" in cl:
            return set(df[col].astype(str).str.zfill(5))
    # Fallback: any column with "cod" that has 5-digit-like values
    for col in df.columns:
        if "cod" in col.lower():
            vals = df[col].astype(str).str.zfill(5)
            if vals.str.len().median() == 5:
                return set(vals)
    return set()


def _load_zomac() -> set[str]:
    """Load ZOMAC municipality codes from Excel."""
    path = RAW_DIR / "zomac.xlsx"
    if not path.exists():
        return set()
    try:
        df = pd.read_excel(path, engine="openpyxl", header=None)
        # ZOMAC file has headers in row 1 (row 0 is a title row)
        # Look for a row containing "COD DANE" or similar header
        for i in range(min(5, len(df))):
            row_vals = [str(v).upper().strip() for v in df.iloc[i].values if pd.notna(v)]
            if any("COD" in v and "DANE" in v for v in row_vals):
                df.columns = df.iloc[i]
                df = df.iloc[i + 1:]
                break
            if any("CODIGO" in v or "DIVIPOLA" in v for v in row_vals):
                df.columns = df.iloc[i]
                df = df.iloc[i + 1:]
                break

        # Find the DANE code column
        for col in df.columns:
            cl = str(col).upper().strip()
            if "COD" in cl and ("DANE" in cl or "MUNI" in cl or "DIVIPOLA" in cl):
                codes = df[col].dropna()
                return set(codes.astype(int).astype(str).str.zfill(5))
        # Fallback: look for numeric column with 5-digit codes
        for col in df.columns:
            if df[col].dtype in ("int64", "float64"):
                vals = df[col].dropna()
                if vals.between(1000, 99999).all():
                    return set(vals.astype(int).astype(str).str.zfill(5))
    except Exception as e:
        print(f"  WARNING: Failed to parse zomac.xlsx: {e}", flush=True)
    return set()


def _load_fiscal_categories() -> dict[str, int]:
    """Load fiscal categories (cod_muni -> category 1-6)."""
    path = RAW_DIR / "fiscal_categories.xlsx"
    if not path.exists():
        return {}
    try:
        df = pd.read_excel(path, engine="openpyxl")
        code_col = None
        cat_col = None
        for col in df.columns:
            cl = str(col).lower()
            if ("codigo" in cl or "divipola" in cl) and "muni" in cl:
                code_col = col
            elif "categ" in cl:
                cat_col = col
        if code_col and cat_col:
            result = {}
            for _, r in df.iterrows():
                code = str(int(r[code_col])).zfill(5) if pd.notna(r[code_col]) else None
                cat = r[cat_col]
                if code and pd.notna(cat):
                    # Category might be "Especial", "1", "2", ... "6"
                    cat_str = str(cat).strip()
                    if cat_str.isdigit():
                        result[code] = int(cat_str)
                    elif "especial" in cat_str.lower():
                        result[code] = 0  # Categoría especial (major cities)
            return result
    except Exception as e:
        print(f"  WARNING: Failed to parse fiscal_categories.xlsx: {e}", flush=True)
    return {}


def _load_dane_population() -> pd.DataFrame:
    """Load DANE population by municipality: urban/rural split."""
    path = RAW_DIR / "dane_population.xlsx"
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_excel(path, engine="openpyxl", header=None)
        # DANE Excel files have complex headers; try to find the data region
        # Look for a row with "DPMP" or "DP" as a header marker
        # Fallback: return empty and log
        # This is a best-effort parser for DANE's somewhat variable Excel layouts
        for start_row in range(min(20, len(df))):
            row_vals = [str(v).lower() for v in df.iloc[start_row].values if pd.notna(v)]
            if any("codigo" in v or "dp" == v.strip() or "dpmp" == v.strip() for v in row_vals):
                df.columns = df.iloc[start_row]
                df = df.iloc[start_row + 1:]
                break

        # Try to find cod_muni, total_pop, urban_pop columns
        result_rows = []
        code_col = None
        total_col = None
        urban_col = None
        for col in df.columns:
            cl = str(col).lower().strip()
            if cl in ("dpmp", "codmpio", "codigo_municipio", "cod_mun"):
                code_col = col
            elif "total" in cl and ("pob" in cl or "hab" in cl):
                total_col = col
            elif ("cabecera" in cl or "urban" in cl) and ("pob" in cl or "hab" in cl):
                urban_col = col

        if code_col and total_col:
            for _, r in df.iterrows():
                code = r[code_col]
                if pd.isna(code):
                    continue
                code = str(int(float(code))).zfill(5) if str(code).replace(".", "").isdigit() else None
                if not code:
                    continue
                total_pop = float(r[total_col]) if pd.notna(r[total_col]) else 0
                urban_pop = float(r.get(urban_col, 0)) if urban_col and pd.notna(r.get(urban_col)) else 0
                result_rows.append({
                    "codigo_divipola": code,
                    "total_population": int(total_pop),
                    "urban_population": int(urban_pop),
                })
            return pd.DataFrame(result_rows)
    except Exception as e:
        print(f"  WARNING: Failed to parse dane_population.xlsx: {e}", flush=True)
    return pd.DataFrame()


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance in km."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# Department capital coordinates (approximate)
DEPT_CAPITAL_COORDS: dict[str, tuple[float, float]] = {
    "AMAZONAS": (-1.01, -71.93), "ANTIOQUIA": (6.25, -75.56),
    "ARAUCA": (7.08, -70.76), "ATLANTICO": (10.96, -74.78),
    "BOLIVAR": (10.39, -75.51), "BOYACA": (5.53, -73.36),
    "CALDAS": (5.07, -75.52), "CAQUETA": (1.61, -75.61),
    "CASANARE": (5.34, -72.39), "CAUCA": (2.44, -76.61),
    "CESAR": (10.47, -73.25), "CHOCO": (5.69, -76.66),
    "CORDOBA": (8.75, -75.88), "CUNDINAMARCA": (4.71, -74.07),
    "DISTRITO CAPITAL DE BOGOTA": (4.71, -74.07),
    "GUAINIA": (3.87, -67.92), "GUAVIARE": (2.57, -72.64),
    "HUILA": (2.93, -75.28), "LA GUAJIRA": (11.54, -72.91),
    "MAGDALENA": (11.24, -74.20), "META": (4.15, -73.63),
    "NARINO": (1.21, -77.28), "NORTE DE SANTANDER": (7.89, -72.50),
    "PUTUMAYO": (1.15, -76.65), "QUINDIO": (4.53, -75.68),
    "RISARALDA": (4.81, -75.69),
    "SAN ANDRES, PROVIDENCIA Y SANTA CATALINA": (12.58, -81.70),
    "SANTANDER": (7.13, -73.13), "SUCRE": (9.30, -75.39),
    "TOLIMA": (4.44, -75.24), "VALLE DEL CAUCA": (3.45, -76.53),
    "VAUPES": (1.25, -70.23), "VICHADA": (6.19, -67.49),
}


def build_municipality_covariates(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """Build per-municipality covariate table joined on codigo_divipola."""
    codes = crosswalk[crosswalk["codigo_divipola"].notna()]["codigo_divipola"].unique()
    if len(codes) == 0:
        print("  WARNING: No valid DIVIPOLA codes, returning empty covariates", flush=True)
        return pd.DataFrame()

    result = pd.DataFrame({"codigo_divipola": codes})

    # PDET flag
    pdet_set = _load_pdet()
    result["is_pdet"] = result["codigo_divipola"].isin(pdet_set)
    print(f"  PDET municipalities matched: {result['is_pdet'].sum()}", flush=True)

    # ZOMAC flag
    zomac_set = _load_zomac()
    result["is_zomac"] = result["codigo_divipola"].isin(zomac_set)
    print(f"  ZOMAC municipalities matched: {result['is_zomac'].sum()}", flush=True)

    # Fiscal category
    fiscal = _load_fiscal_categories()
    result["fiscal_category"] = result["codigo_divipola"].map(fiscal)
    n_fiscal = result["fiscal_category"].notna().sum()
    print(f"  Fiscal category matched: {n_fiscal}/{len(result)}", flush=True)

    # DANE population
    pop_df = _load_dane_population()
    if len(pop_df) > 0:
        result = result.merge(pop_df, on="codigo_divipola", how="left")
        result["rurality_ratio"] = np.where(
            result["total_population"] > 0,
            1 - result["urban_population"] / result["total_population"],
            np.nan,
        )
        n_pop = result["total_population"].notna().sum()
        print(f"  Population matched: {n_pop}/{len(result)}", flush=True)
    else:
        result["total_population"] = np.nan
        result["urban_population"] = np.nan
        result["rurality_ratio"] = np.nan

    # Distance to department capital (computed from crosswalk dept + DIVIPOLA)
    # Join dept from crosswalk
    dept_map = crosswalk.dropna(subset=["codigo_divipola"]).set_index(
        "codigo_divipola"
    )["department_norm"].to_dict()
    result["department_norm"] = result["codigo_divipola"].map(dept_map).apply(
        lambda x: _strip_accents(str(x)) if pd.notna(x) else None
    )

    # For distance computation we need municipality coords;
    # approximate from the municipality_coords.json cache if available
    muni_cache_path = DATA_DIR / "municipality_coords.json"
    muni_coords: dict[str, list[float]] = {}
    if muni_cache_path.exists():
        with open(muni_cache_path, encoding="utf-8") as f:
            muni_coords = json.load(f)

    # Build reverse lookup: codigo_divipola -> (dept, muni) from crosswalk
    code_to_dm = {}
    for _, r in crosswalk.dropna(subset=["codigo_divipola"]).iterrows():
        code_to_dm.setdefault(r["codigo_divipola"], (
            _strip_accents(str(r["department_norm"])),
            _strip_accents(str(r["municipality_norm"])),
        ))

    distances = []
    for code in result["codigo_divipola"]:
        dm = code_to_dm.get(code)
        if not dm:
            distances.append(np.nan)
            continue
        dept, muni = dm
        cache_key = f"{dept}|{muni}"
        capital_coords = DEPT_CAPITAL_COORDS.get(dept)
        muni_coord = muni_coords.get(cache_key)
        if capital_coords and muni_coord:
            d = _haversine_km(muni_coord[0], muni_coord[1], capital_coords[0], capital_coords[1])
            distances.append(round(d, 1))
        else:
            distances.append(np.nan)

    result["distance_to_capital_km"] = distances

    return result


# ── Consortium lookup ─────────────────────────────────────────────


def build_consortium_lookup() -> pd.DataFrame:
    """Parse consortium member data from SECOP II Grupos de Proveedores.

    Returns DataFrame: consortium_nit, member_nit, member_name, participation_pct, is_leader
    """
    path = RAW_DIR / "consortium_members.json"
    if not path.exists():
        print("  WARNING: consortium_members.json not found", flush=True)
        return pd.DataFrame(columns=[
            "consortium_nit", "member_nit", "member_name", "participation_pct", "is_leader"
        ])

    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    df = pd.DataFrame(raw)
    print(f"  Raw consortium rows: {len(df):,}", flush=True)

    # Map columns (Socrata field names)
    col_map = {}
    for col in df.columns:
        cl = col.lower()
        # codigo_grupo is the unique consortium ID in SECOP
        # nit_grupo is unreliable (mostly "No Definido")
        if cl == "codigo_grupo":
            col_map["consortium_id"] = col
        elif cl == "nombre_grupo":
            col_map["consortium_name"] = col
        elif "nit_participante" in cl:
            col_map["member_nit"] = col
        elif "nombre_participante" in cl:
            col_map["member_name"] = col
        elif cl == "participacion":
            col_map["participation_pct"] = col
        elif "es_lider" in cl:
            col_map["is_leader"] = col

    if "consortium_id" not in col_map or "member_nit" not in col_map:
        print(f"  WARNING: Could not map consortium columns: {df.columns.tolist()}", flush=True)
        return pd.DataFrame(columns=[
            "consortium_id", "consortium_name", "member_nit", "member_name",
            "participation_pct", "is_leader"
        ])

    result = pd.DataFrame()
    result["consortium_id"] = df[col_map["consortium_id"]].astype(str)
    if "consortium_name" in col_map:
        result["consortium_name"] = df[col_map["consortium_name"]].fillna("").astype(str)
    else:
        result["consortium_name"] = ""
    result["member_nit"] = df[col_map["member_nit"]].astype(str)
    if "member_name" in col_map:
        result["member_name"] = df[col_map["member_name"]].fillna("").astype(str)
    else:
        result["member_name"] = ""

    if "participation_pct" in col_map:
        result["participation_pct"] = pd.to_numeric(df[col_map["participation_pct"]], errors="coerce")
        # Normalize: if values are > 1, assume they are percentages (0-100)
        if result["participation_pct"].median() > 1:
            result["participation_pct"] = result["participation_pct"] / 100
    else:
        result["participation_pct"] = np.nan

    if "is_leader" in col_map:
        result["is_leader"] = df[col_map["is_leader"]].apply(
            lambda x: str(x).lower() in ("true", "1", "si", "yes", "s")
        )
    else:
        result["is_leader"] = False

    # Deduplicate on (consortium_id, member_nit)
    before = len(result)
    result = result.drop_duplicates(subset=["consortium_id", "member_nit"])
    print(f"  Deduplicated: {before:,} -> {len(result):,}", flush=True)

    # Validate participation sums per consortium_id
    group_sums = result.groupby("consortium_id")["participation_pct"].sum()
    valid_groups = group_sums[(group_sums >= 0.9) & (group_sums <= 1.1)]
    n_valid = len(valid_groups)
    n_total = len(group_sums)
    print(f"  Participation sum validation: {n_valid:,}/{n_total:,} groups sum to ~1.0", flush=True)

    # For groups where participation is missing/invalid, distribute equally
    invalid_groups = set(group_sums.index) - set(valid_groups.index)
    if invalid_groups:
        for grp in invalid_groups:
            mask = result["consortium_id"] == grp
            n_members = mask.sum()
            if n_members > 0:
                result.loc[mask, "participation_pct"] = 1.0 / n_members

    return result


# ── Main ──────────────────────────────────────────────────────────


def main():
    print("=" * 60, flush=True)
    print("BUILD COVARIATES", flush=True)
    print("=" * 60, flush=True)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load existing refpop municipalities
    refpop_path = DATA_DIR / "reference_population.parquet"
    if refpop_path.exists():
        refpop = pd.read_parquet(refpop_path, columns=["department_norm", "municipality_norm"])
    else:
        print("  WARNING: reference_population.parquet not found, using empty", flush=True)
        refpop = pd.DataFrame(columns=["department_norm", "municipality_norm"])

    # 1. DIVIPOLA crosswalk
    print("\n[1] Building DIVIPOLA crosswalk...", flush=True)
    crosswalk = build_divipola_crosswalk(refpop)
    crosswalk.to_parquet(OUT_DIR / "divipola_crosswalk.parquet", index=False, compression="zstd")
    print(f"  Saved divipola_crosswalk.parquet: {len(crosswalk)} rows", flush=True)

    # 2. Municipality covariates
    print("\n[2] Building municipality covariates...", flush=True)
    covariates = build_municipality_covariates(crosswalk)
    if len(covariates) > 0:
        covariates.to_parquet(OUT_DIR / "municipality_covariates.parquet", index=False, compression="zstd")
        print(f"  Saved municipality_covariates.parquet: {len(covariates)} rows", flush=True)
    else:
        print("  Skipped: no covariate data available", flush=True)

    # 3. Consortium lookup
    print("\n[3] Building consortium lookup...", flush=True)
    consortium = build_consortium_lookup()
    consortium.to_parquet(OUT_DIR / "consortium_members.parquet", index=False, compression="zstd")
    print(f"  Saved consortium_members.parquet: {len(consortium):,} rows", flush=True)

    print(f"\n{'=' * 60}", flush=True)
    print("BUILD COVARIATES COMPLETE", flush=True)
    print(f"{'=' * 60}", flush=True)


if __name__ == "__main__":
    main()
