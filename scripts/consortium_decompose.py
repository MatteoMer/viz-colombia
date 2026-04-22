"""Consortium decomposition — expand consortium contracts into member rows.

For signals that aggregate by supplier_id (S5 HHI, S8 relationships,
S9 fragmentation, S2c/S3c contractor portfolios), consortium contracts
need to be "seen through" to the underlying member firms.

This module builds a decomposed view where:
- Consortium contracts become N rows (one per member)
  with effective_supplier_id = member_nit and
  effective_value_cop = awarded_value_cop * participation_pct
- Non-consortium contracts pass through unchanged
  with effective_supplier_id = supplier_id
"""

from pathlib import Path

import pandas as pd

DATA_DIR = Path("data")
COVARIATES_DIR = DATA_DIR / "covariates"


def load_consortium_members() -> pd.DataFrame | None:
    """Load consortium member lookup. Returns None if unavailable."""
    path = COVARIATES_DIR / "consortium_members.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    if df.empty:
        return None
    return df


def build_decomposed_view(
    refpop: pd.DataFrame,
    consortium_members: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build decomposed view expanding consortium contracts into member rows.

    Parameters
    ----------
    refpop : DataFrame
        Reference population with columns: contract_id, supplier_id,
        supplier_name, awarded_value_cop, is_consortium, entity_nit,
        signature_year, object_description.
    consortium_members : DataFrame or None
        Consortium lookup with columns: consortium_nit, member_nit,
        member_name, participation_pct.

    Returns
    -------
    DataFrame with original columns plus:
        effective_supplier_id : str — member_nit for consortium, supplier_id otherwise
        effective_value_cop : float — proportional value for consortium, full value otherwise
    """
    # Columns to preserve
    keep_cols = [
        "contract_id", "supplier_id", "supplier_name", "awarded_value_cop",
        "entity_nit", "signature_year", "object_description",
    ]
    # Use only columns that exist
    keep_cols = [c for c in keep_cols if c in refpop.columns]

    base = refpop[keep_cols].copy()

    if consortium_members is None or consortium_members.empty:
        # No consortium data — pass everything through
        base["effective_supplier_id"] = base["supplier_id"]
        base["effective_value_cop"] = base["awarded_value_cop"]
        return base

    # Identify consortium contracts
    is_consortium = refpop.get("is_consortium", pd.Series(False, index=refpop.index))

    # Non-consortium contracts pass through
    non_cons = base[~is_consortium].copy()
    non_cons["effective_supplier_id"] = non_cons["supplier_id"]
    non_cons["effective_value_cop"] = non_cons["awarded_value_cop"]

    # Consortium contracts: match by normalized supplier_name → consortium_name
    cons = base[is_consortium].copy()
    if cons.empty:
        return non_cons

    # Build name-based lookup: normalized consortium_name → consortium_id
    import unicodedata
    def _norm(s):
        if not isinstance(s, str):
            return ""
        nfkd = unicodedata.normalize("NFKD", s)
        return "".join(c for c in nfkd if not unicodedata.combining(c)).upper().strip()

    name_col = "consortium_name" if "consortium_name" in consortium_members.columns else None
    id_col = "consortium_id" if "consortium_id" in consortium_members.columns else "consortium_nit"

    if name_col:
        # Build normalized name → consortium_id mapping
        cm = consortium_members.copy()
        cm["_name_norm"] = cm[name_col].apply(_norm)
        cons["_name_norm"] = cons["supplier_name"].apply(_norm)

        # Join on normalized name
        expanded = cons.merge(
            cm[["_name_norm", id_col, "member_nit", "member_name", "participation_pct"]].drop_duplicates(),
            on="_name_norm",
            how="left",
        )
    else:
        # Fallback: join on supplier_id = consortium_nit
        expanded = cons.merge(
            consortium_members[[id_col, "member_nit", "member_name", "participation_pct"]],
            left_on="supplier_id",
            right_on=id_col,
            how="left",
        )

    # Contracts that matched consortium data
    matched = expanded[expanded["member_nit"].notna()].copy()
    matched["effective_supplier_id"] = matched["member_nit"]
    matched["effective_value_cop"] = matched["awarded_value_cop"] * matched["participation_pct"]

    # Consortium contracts without member data — pass through as-is
    unmatched_ids = set(cons["contract_id"]) - set(matched["contract_id"])
    unmatched = cons[cons["contract_id"].isin(unmatched_ids)].copy()
    unmatched["effective_supplier_id"] = unmatched["supplier_id"]
    unmatched["effective_value_cop"] = unmatched["awarded_value_cop"]

    # Clean up merge columns
    drop_cols = [id_col, "member_nit", "member_name", "participation_pct", "_name_norm"]
    matched = matched.drop(columns=[c for c in drop_cols if c in matched.columns])

    result = pd.concat([non_cons, matched, unmatched], ignore_index=True)

    # Report
    n_orig = len(refpop)
    n_expanded = len(result)
    n_matched_contracts = len(cons) - len(unmatched_ids)
    print(f"  Consortium decomposition: {n_orig:,} -> {n_expanded:,} rows "
          f"({n_matched_contracts:,} consortium contracts expanded, "
          f"{len(unmatched_ids):,} without member data)", flush=True)

    return result
