"""Download external covariate datasets for contextual enrichment.

Fetches:
  1. DIVIPOLA codebook (Socrata JSON)
  2. PDET municipalities (Socrata JSON)
  3. ZOMAC municipalities (Excel)
  4. DANE population by municipality (Excel)
  5. DANE ethnic composition by municipality (Excel)
  6. Fiscal categories — Ley 617 (Excel)
  7. Consortium members — SECOP II Grupos de Proveedores (Socrata JSON, paged)

All outputs are cached in data/covariates/raw/. Re-run is idempotent:
skips files that already exist.
"""

import json
import time
from pathlib import Path

import requests

RAW_DIR = Path("data/covariates/raw")

# Socrata endpoints (datos.gov.co)
SOCRATA_BASE = "https://www.datos.gov.co/resource"
DIVIPOLA_RESOURCE = "gdxc-w37w"
PDET_RESOURCE = "idrk-ba8y"
CONSORTIUM_RESOURCE = "ceth-n4bn"

# Excel / file URLs
ZOMAC_URL = (
    "https://www.finagro.com.co/sites/default/files/"
    "anexo_municipios_pdet_zomac.xlsx"
)
DANE_POP_URL = (
    "https://www.dane.gov.co/files/investigaciones/poblacion/"
    "proyepobla06_20/PPED-AreaMun-2018-2042_VP.xlsx"
)
DANE_ETHNIC_URL = (
    "https://www.dane.gov.co/files/investigaciones/poblacion/"
    "proyepobla06_20/anex-DCD-Proypoblacion-PerteneniaEtnicoRacialmun.xlsx"
)
FISCAL_URL = (
    "https://www.contaduria.gov.co/documents/20127/2827957/"
    "CT01+Categorizacion.xlsx"
)

SOCRATA_PAGE_SIZE = 50_000
CONSORTIUM_MAX_ROWS = 750_000  # safety cap
HEADERS = {"Accept": "application/json"}
TIMEOUT = 60


def _socrata_fetch_all(resource_id: str, out_path: Path, max_rows: int = 0):
    """Page through a Socrata JSON endpoint and save the full result. Non-fatal on failure."""
    if out_path.exists():
        print(f"  [cached] {out_path.name}", flush=True)
        return

    try:
        all_rows: list[dict] = []
        offset = 0
        while True:
            url = (
                f"{SOCRATA_BASE}/{resource_id}.json"
                f"?$limit={SOCRATA_PAGE_SIZE}&$offset={offset}"
            )
            print(f"  Fetching {resource_id} offset={offset}...", flush=True)
            resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            resp.raise_for_status()
            page = resp.json()
            if not page:
                break
            all_rows.extend(page)
            offset += len(page)
            if len(page) < SOCRATA_PAGE_SIZE:
                break
            if max_rows and offset >= max_rows:
                break
            time.sleep(0.5)

        out_path.write_text(json.dumps(all_rows, ensure_ascii=False), encoding="utf-8")
        print(f"  Saved {len(all_rows):,} rows -> {out_path.name}", flush=True)
    except Exception as e:
        print(f"  WARNING: Failed to fetch {resource_id}: {e}", flush=True)
        print(f"  Pipeline will continue without this dataset.", flush=True)


def _download_file(url: str, out_path: Path):
    """Download a binary file (Excel) with caching. Non-fatal on failure."""
    if out_path.exists():
        print(f"  [cached] {out_path.name}", flush=True)
        return
    print(f"  Downloading {out_path.name}...", flush=True)
    try:
        resp = requests.get(url, timeout=TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        out_path.write_bytes(resp.content)
        print(f"  Saved {len(resp.content) // 1024} KB -> {out_path.name}", flush=True)
    except Exception as e:
        print(f"  WARNING: Failed to download {out_path.name}: {e}", flush=True)
        print(f"  Pipeline will continue without this dataset.", flush=True)


def download_divipola():
    _socrata_fetch_all(DIVIPOLA_RESOURCE, RAW_DIR / "divipola.json")


def download_pdet():
    _socrata_fetch_all(PDET_RESOURCE, RAW_DIR / "pdet.json")


def download_zomac():
    _download_file(ZOMAC_URL, RAW_DIR / "zomac.xlsx")


def download_dane_population():
    _download_file(DANE_POP_URL, RAW_DIR / "dane_population.xlsx")


def download_dane_ethnic():
    _download_file(DANE_ETHNIC_URL, RAW_DIR / "dane_ethnic.xlsx")


def download_fiscal_categories():
    _download_file(FISCAL_URL, RAW_DIR / "fiscal_categories.xlsx")


def download_consortium_members():
    _socrata_fetch_all(
        CONSORTIUM_RESOURCE,
        RAW_DIR / "consortium_members.json",
        max_rows=CONSORTIUM_MAX_ROWS,
    )


def main():
    print("=" * 60, flush=True)
    print("DOWNLOAD COVARIATES", flush=True)
    print("=" * 60, flush=True)

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    download_divipola()
    download_pdet()
    download_zomac()
    download_dane_population()
    download_dane_ethnic()
    download_fiscal_categories()
    download_consortium_members()

    print("\nAll covariate downloads complete.", flush=True)


if __name__ == "__main__":
    main()
