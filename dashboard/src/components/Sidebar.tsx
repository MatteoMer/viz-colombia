import { useMemo } from "react";
import type { View, KPI } from "../types";
import { fmtNum } from "../utils";

interface NavItem {
  view: View;
  label: string;
  countKey?: "n_analyzed" | "n_contractors";
}

const NAV: NavItem[] = [
  { view: "overview", label: "OVERVIEW" },
  { view: "contracts", label: "CONTRACTS", countKey: "n_analyzed" },
  { view: "contractors", label: "CONTRACTORS", countKey: "n_contractors" },
  { view: "methodology", label: "METHODOLOGY" },
];

interface SidebarProps {
  view: View;
  onViewChange: (v: View) => void;
  kpi?: KPI | null;
  nContractors?: number;
}

export function Sidebar({ view, onViewChange, kpi, nContractors }: SidebarProps) {
  const alertCounts = useMemo(() => {
    if (!kpi) return { active: 0, unreviewed: 0, highRisk: 0 };
    return {
      active: kpi.n_flagged,
      unreviewed: kpi.n_flagged,
      highRisk: kpi.n_high_risk ?? Math.round(kpi.n_flagged * 0.15),
    };
  }, [kpi]);

  return (
    <aside className="w-[200px] min-w-[200px] bg-[#0a0a0f] text-[#555560] flex flex-col z-20 border-r border-[#1a1a22]">
      {/* Identity block */}
      <div className="px-4 pt-5 pb-4 border-b border-[#1a1a22]">
        <div className="flex items-center gap-2.5">
          <div className="w-7 h-7 border border-[#2a2a32] flex items-center justify-center">
            <span className="font-mono text-[10px] font-bold text-[#6fd4f5] tracking-wider">COL</span>
          </div>
          <div>
            <span className="block text-[13px] font-bold text-[#d0d0d8] tracking-tight leading-tight font-display">
              COLOMBIA
            </span>
            <span className="block text-[9px] text-[#555560] font-mono font-medium tracking-[0.12em] uppercase">
              RISK MONITOR
            </span>
          </div>
        </div>
        <div className="mt-1.5 font-mono text-[9px] text-[#3a3a42] tracking-wide">
          v2.1.0
        </div>
      </div>

      {/* Navigation section */}
      <div className="pt-3 pb-1 px-4">
        <div className="section-label mb-2">NAVIGATION</div>
      </div>
      <nav className="pb-2">
        {NAV.map((n) => (
          <button
            key={n.view}
            onClick={() => onViewChange(n.view)}
            className={`flex items-center justify-between w-full px-4 py-[6px] transition-colors relative ${
              view === n.view
                ? "text-[#d0d0d8]"
                : "text-[#555560] hover:text-[#888890]"
            }`}
          >
            {view === n.view && (
              <div className="absolute left-0 top-0 bottom-0 w-[2px] bg-[#6fd4f5]" />
            )}
            <span className="nav-label">{n.label}</span>
            {n.countKey && kpi && (
              <span className="font-mono text-[10px] text-[#3a3a42] tabular-nums">
                {fmtNum(n.countKey === "n_contractors" ? (nContractors ?? 0) : (kpi[n.countKey] ?? 0))}
              </span>
            )}
          </button>
        ))}
      </nav>

      {/* Monitoring section */}
      <div className="border-t border-[#1a1a22] pt-3 pb-1 px-4">
        <div className="section-label mb-2">MONITORING</div>
      </div>
      <div className="pb-3">
        <MonitorRow label="ACTIVE ALERTS" count={alertCounts.active} color="alert" />
        <MonitorRow label="UNREVIEWED" count={alertCounts.unreviewed} color="alert" />
        <MonitorRow label="HIGH RISK" count={alertCounts.highRisk} color="warn" />
      </div>

      {/* Filters section */}
      <div className="border-t border-[#1a1a22] pt-3 pb-1 px-4">
        <div className="section-label mb-2">FILTERS</div>
      </div>
      <div className="px-4 pb-3 space-y-1">
        <FilterRow label="DATE RANGE" value="ALL" />
        <FilterRow label="CATEGORY" value="ALL" />
        <FilterRow label="DEPARTMENT" value="ALL" />
      </div>

      {/* Spacer */}
      <div className="flex-1" />

      {/* Footer — data source readout */}
      <div className="border-t border-[#1a1a22] px-4 py-3">
        <div className="section-label mb-2">DATA SOURCE</div>
        <div className="space-y-[3px]">
          <FooterRow label="" value="SECOP II · Obra" bold />
          <FooterRow label="RECORDS" value={kpi ? fmtNum(kpi.n_total) : "—"} />
          <FooterRow label="EXCLUDED" value={kpi ? fmtNum(kpi.n_dq_excluded) : "—"} />
          <FooterRow label="REFRESHED" value="14:32Z" />
        </div>
        <div className="mt-3 flex items-start gap-1.5 text-[9px] text-[#3a3a42] leading-snug">
          <span className="shrink-0 mt-px">&#x26A0;</span>
          <span className="pl-1">
            Statistical anomaly is not evidence of wrongdoing. Flagged items
            require human review.
          </span>
        </div>
      </div>
    </aside>
  );
}

function MonitorRow({ label, count, color }: { label: string; count: number; color: "alert" | "warn" }) {
  const dotColor = color === "alert" ? "#e04a5f" : "#f0a878";
  return (
    <div className="flex items-center justify-between px-4 py-[5px]">
      <div className="flex items-center gap-2">
        <span
          className="w-[5px] h-[5px] rounded-full"
          style={{ backgroundColor: count > 0 ? dotColor : "#2a2a32" }}
        />
        <span className="nav-label text-[#555560]">{label}</span>
      </div>
      <span className="font-mono text-[10px] tabular-nums" style={{ color: count > 0 ? dotColor : "#3a3a42" }}>
        {fmtNum(count)}
      </span>
    </div>
  );
}

function FilterRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between py-[4px] cursor-pointer hover:text-[#888890] transition-colors group">
      <span className="font-mono text-[10px] font-medium uppercase tracking-[0.06em] text-[#555560]">{label}</span>
      <span className="font-mono text-[10px] text-[#3a3a42] group-hover:text-[#555560] flex items-center gap-1">
        {value}
        <svg width="8" height="8" viewBox="0 0 8 8" className="opacity-40">
          <path d="M2 3 L4 5 L6 3" stroke="currentColor" fill="none" strokeWidth="1" />
        </svg>
      </span>
    </div>
  );
}

function FooterRow({ label, value, bold }: { label: string; value: string; bold?: boolean }) {
  return (
    <div className="flex items-center justify-between">
      {label ? (
        <span className="font-mono text-[10px] text-[#3a3a42] uppercase tracking-[0.06em]">{label}</span>
      ) : (
        <span />
      )}
      <span className={`font-mono text-[11px] ${bold ? "text-[#d0d0d8] font-medium" : "text-[#888890]"} tabular-nums`}>
        {value}
      </span>
    </div>
  );
}
