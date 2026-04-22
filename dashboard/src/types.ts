export interface KPI {
  n_analyzed: number;
  n_flagged: number;
  exposure_cop: number;
  n_dq_excluded: number;
  n_total: number;
  n_context_shifted?: number;
  n_contractors?: number;
  n_high_risk?: number;
}

export interface ContextCard {
  type: "thin_market" | "consortium" | "regimen_subtype" | "value_plausibility" | "no_explanation";
  confidence: "high" | "moderate" | "low" | null;
  headline: string;
  explanation: string;
  affected_signals: string[];
  members?: { nit: string; name: string; pct: number }[];
}

export interface MuniContext {
  is_pdet: boolean;
  is_zomac: boolean;
  fiscal_cat: number;
  pop: number;
  rurality: number;
  dist_capital_km: number;
}

export interface Department {
  name: string;
  n_contracts: number;
  n_flagged: number;
  flag_rate: number;
  exposure: number;
  composite: number;
}

export interface Contract {
  id: string;
  entity: string;
  entity_nit?: string;
  supplier: string;
  supplier_nit?: string;
  dept: string;
  muni: string;
  value: number;
  status: string;
  composite: number;
  pctl: number;
  lat: number | null;
  lon: number | null;
  z: Record<string, number>;
  z_global: Record<string, number>;
  cohort: string;
  is_mandato: boolean;
  is_eice: boolean;
  exempt: string[];
  desc: string;
  signals: string;
  url: string;
  dq_excluded: boolean;
  dq_flags: string;
  composite_adj?: number;
  pctl_adj?: number;
  s1_not_eval?: boolean;
  ranking_unstable?: boolean;
  cards?: ContextCard[] | null;
  ctx?: MuniContext | null;
}

export interface Contractor {
  id: string;
  name: string;
  composite: number;
  n: number;
  exposure: number;
  flagged: number;
  signals: string;
}

/** Slim dot for ALL 28K contracts (map rendering) */
export interface Dot {
  i: string;  // contract_id
  a: number;  // lat
  o: number;  // lon
  c: number;  // composite
  v: number;  // value
  d: string;  // department
  m: string;  // municipality
  e: string;  // entity (truncated)
  q: number;  // dq_excluded (1=yes, 0=no)
  y: number;  // year of signature (0=unknown)
  cx: number; // execution category avg
  cc: number; // competition category avg
  cp: number; // pricing category avg
  cr: number; // relationships category avg
}

export interface FilterState {
  scoreRange: [number, number];
  yearRange: [number, number];
  category: Category;
}

export interface DashboardData {
  kpi: KPI;
  departments: Department[];
  geojson: GeoJSON.FeatureCollection;
  dots: Dot[];
  contracts: Contract[];
  contractors: Contractor[];
  methodology: string;
}

export type View = "overview" | "contracts" | "contractors" | "methodology";
export type Category =
  | "all"
  | "execution"
  | "competition"
  | "pricing"
  | "relationships";
