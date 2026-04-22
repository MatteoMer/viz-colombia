export function fmtCop(v: number): string {
  if (v >= 1e12) return `COP ${(v / 1e12).toFixed(1)}T`;
  if (v >= 1e9) return `COP ${(v / 1e9).toFixed(1)}B`;
  if (v >= 1e6) return `COP ${(v / 1e6).toFixed(0)}M`;
  return `COP ${v.toLocaleString("en")}`;
}

export function fmtUsd(cop: number): string {
  const u = cop / 4000;
  if (u >= 1e9) return `$${(u / 1e9).toFixed(1)}B`;
  if (u >= 1e6) return `$${(u / 1e6).toFixed(1)}M`;
  if (u >= 1e3) return `$${(u / 1e3).toFixed(0)}K`;
  return `$${Math.round(u)}`;
}

export function fmtPct(v: number): string {
  return `${(v * 100).toFixed(1)}%`;
}

export function fmtNum(v: number): string {
  return v.toLocaleString("en");
}

export function titleCase(s: string): string {
  return s
    .split(" ")
    .map((w) => w.charAt(0) + w.slice(1).toLowerCase())
    .join(" ");
}

// ── Choropleth: dark-magenta-to-cream ramp by flag_rate ──
// Data: 2 depts at 0%, rest 2.5%–23%. Ramp anchored at 0 with a sharp
// step so the zero depts are clearly dark, then spreads across 2.5-23%.
const FLAG_RATE_STOPS: [number, [number, number, number]][] = [
  [0.000, [16, 14, 22]],    // #100e16  near-black (zero flags)
  [0.020, [26, 20, 32]],    // #1a1420  just above zero — still very dark
  [0.050, [61, 31, 61]],    // #3d1f3d  low end of actual data
  [0.080, [99, 38, 78]],    // #63264e  below median
  [0.120, [150, 58, 96]],   // #963a60  median (~12%)
  [0.160, [201, 79, 109]],  // #c94f6d  above median
  [0.200, [240, 168, 120]], // #f0a878  high
  [0.250, [253, 228, 184]], // #fde4b8  top (Putumayo ~23%)
];

export function flagRateToColor(flagRate: number): [number, number, number, number] {
  const t = Math.max(0, Math.min(1, flagRate));
  for (let i = 1; i < FLAG_RATE_STOPS.length; i++) {
    const [t0, c0] = FLAG_RATE_STOPS[i - 1];
    const [t1, c1] = FLAG_RATE_STOPS[i];
    if (t <= t1) {
      const f = (t - t0) / (t1 - t0);
      return [
        Math.round(c0[0] + (c1[0] - c0[0]) * f),
        Math.round(c0[1] + (c1[1] - c0[1]) * f),
        Math.round(c0[2] + (c1[2] - c0[2]) * f),
        210, // ~0.82 opacity
      ];
    }
  }
  return [253, 228, 184, 210];
}

// ── Scatterplot dot color ramp (cool accent, contrasts warm choropleth) ──
export function scoreToRGBA(score: number): [number, number, number, number] {
  if (score < 3) return [42, 74, 106, 77];     // #2a4a6a @ 0.30
  if (score < 6) return [74, 143, 201, 128];    // #4a8fc9 @ 0.50
  if (score < 8) return [111, 212, 245, 179];   // #6fd4f5 @ 0.70
  return [232, 250, 255, 230];                   // #e8faff @ 0.90
}

/** Whether a dot should get a white stroke (score 8+) */
export function isTopTier(score: number): boolean {
  return score >= 8;
}

// ── Heatmap color range for deck.gl HeatmapLayer ──
export const HEATMAP_COLOR_RANGE: [number, number, number][] = [
  [26, 0, 32],       // #1a0020  deep violet
  [74, 0, 80],       // #4a0050  deep purple
  [139, 0, 112],     // #8b0070  deep magenta
  [196, 30, 122],    // #c41e7a  magenta-pink
  [232, 69, 147],    // #e84593  hot pink
  [255, 126, 179],   // #ff7eb3  light pink
  [255, 201, 222],   // #ffc9de  pale pink
  [255, 245, 204],   // #fff5cc  pale yellow-white
];

// Legacy choropleth (still used in non-map views)
const CHOROPLETH_COLORS = [
  "#1a2235",
  "#1e3050",
  "#1f4068",
  "#2a5a6a",
  "#6b6030",
  "#8b5030",
  "#8b3035",
];
const COMPOSITE_BREAKS = [0, 0.1, 0.25, 0.4, 0.65, 1.0];

export function riskColor(composite: number): string {
  for (let i = 0; i < COMPOSITE_BREAKS.length; i++)
    if (composite <= COMPOSITE_BREAKS[i]) return CHOROPLETH_COLORS[i];
  return CHOROPLETH_COLORS[CHOROPLETH_COLORS.length - 1];
}

export function dotColor(v: number): string {
  if (v < 3) return "#2a4a6a";
  if (v < 6) return "#4a8fc9";
  if (v < 8) return "#6fd4f5";
  return "#e8faff";
}

export function scoreBadgeClass(v: number): string {
  if (v < 0.5) return "border-[#6fd4f5]/30 text-[#6fd4f5]";
  if (v < 1.5) return "border-[#f0a878]/30 text-[#f0a878]";
  return "border-[#e04a5f]/30 text-[#e04a5f]";
}

export const SIGNAL_LABELS: Record<string, string> = {
  stall: "Payment Stall",
  creep_c: "Value Creep",
  creep_k: "Contractor Creep",
  slip_c: "Schedule Slip",
  slip_k: "Contractor Slip",
  bunch: "Threshold Bunching",
  hhi: "HHI Concentration",
  single: "Single Bidder",
  speed: "Award Speed",
  rel: "Relationship",
  frag: "Fragmentation",
};

export const SIGNAL_DESCRIPTIONS: Record<string, string> = {
  stall: "Contract has received little or no payment relative to how long it has been active — may indicate stalled or abandoned work.",
  creep_c: "Final contract value is significantly higher than the original estimated value — suggests cost overruns or scope changes after award.",
  creep_k: "This contractor's contracts consistently end up costing more than estimated — a portfolio-wide pattern of value increases.",
  slip_c: "Contract duration was extended well beyond its original timeline — the project is taking much longer than planned.",
  slip_k: "This contractor's projects are routinely extended — a pattern of schedule overruns across their portfolio.",
  bunch: "The contracting entity awards many contracts just below regulatory thresholds — possibly splitting contracts to avoid oversight requirements.",
  hhi: "A small number of contractors win most of this entity's contracts — suggests limited competition or preferred relationships.",
  single: "A high share of this entity's competitive tenders received only one bid — may indicate restricted competition.",
  speed: "The time from publication to award is unusually fast or slow compared to similar contracts — very fast awards may suggest pre-arranged outcomes.",
  rel: "This entity-contractor pair does significantly more business together than expected by chance — an unusually concentrated relationship.",
  frag: "Multiple similar contracts awarded to the same supplier in the same year — may indicate deliberate splitting to avoid oversight thresholds.",
};

export const SIGNAL_CATS: Record<string, string[]> = {
  execution: ["stall", "slip_c"],
  competition: ["single", "bunch", "speed"],
  pricing: ["creep_c", "creep_k", "frag"],
  relationships: ["hhi", "rel"],
};

export function getCatScore(
  z: Record<string, number>,
  composite: number,
  category: string,
): number {
  if (category === "all") return composite;
  const sigs = SIGNAL_CATS[category] ?? [];
  if (sigs.length === 0) return composite;
  return sigs.reduce((a, s) => a + (z[s] ?? 0), 0) / sigs.length;
}

/** Get category score from dot's pre-computed fields */
export function getDotCatScore(dot: { c: number; cx: number; cc: number; cp: number; cr: number }, category: string): number {
  switch (category) {
    case "execution": return dot.cx;
    case "competition": return dot.cc;
    case "pricing": return dot.cp;
    case "relationships": return dot.cr;
    default: return dot.c;
  }
}
