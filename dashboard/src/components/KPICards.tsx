import { useState, useEffect, useMemo } from "react";
import { BarChart, Bar, AreaChart, Area, ResponsiveContainer } from "recharts";
import type { KPI, Dot } from "../types";
import { fmtNum, fmtCop, fmtUsd } from "../utils";

function useCountUp(target: number, duration = 900): number {
  const [current, setCurrent] = useState(0);
  useEffect(() => {
    const start = performance.now();
    let raf: number;
    function tick(now: number) {
      const t = Math.min((now - start) / duration, 1);
      const ease = 1 - Math.pow(1 - t, 3);
      setCurrent(Math.round(target * ease));
      if (t < 1) raf = requestAnimationFrame(tick);
    }
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [target, duration]);
  return current;
}

export function KPICards({ kpi, dots }: { kpi: KPI; dots?: Dot[] }) {
  const analyzedCount = useCountUp(kpi.n_analyzed);
  const flaggedCount = useCountUp(kpi.n_flagged);

  const histogramData = useMemo(() => {
    if (!dots) return [];
    const buckets = Array.from({ length: 20 }, (_, i) => ({ bin: i, count: 0 }));
    dots.forEach((d) => {
      if (d.q === 1) return;
      const idx = Math.max(0, Math.min(19, Math.floor(d.c / 0.25)));
      buckets[idx].count++;
    });
    return buckets.map((b) => ({ ...b, count: b.count > 0 ? Math.log10(b.count) : 0 }));
  }, [dots]);

  const deptAlertData = useMemo(() => {
    if (!dots) return [];
    const deptMap: Record<string, number> = {};
    dots.forEach((d) => {
      if (d.q === 1 || d.c < 1.5) return;
      deptMap[d.d] = (deptMap[d.d] ?? 0) + 1;
    });
    return Object.entries(deptMap)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 8)
      .map(([dept, count]) => ({ dept, count }));
  }, [dots]);

  const exposureData = useMemo(() => {
    if (!dots) return [];
    const buckets = Array.from({ length: 10 }, (_, i) => ({ bin: i, value: 0 }));
    dots.forEach((d) => {
      if (d.q === 1) return;
      const idx = Math.max(0, Math.min(9, Math.floor(d.c / 0.5)));
      buckets[idx].value += d.v;
    });
    return buckets;
  }, [dots]);

  return (
    <div className="flex gap-2">
      <Card
        accent="#6fd4f5"
        number={fmtNum(analyzedCount)}
        label="CONTRACTS ANALYZED"
        sub={`${fmtNum(kpi.n_total)} total · ${fmtNum(kpi.n_dq_excluded)} excluded`}
        sparkline={histogramData.length > 0 ? (
          <ResponsiveContainer width="100%" height={24}>
            <BarChart data={histogramData} margin={{ top: 0, right: 0, bottom: 0, left: 0 }}>
              <Bar dataKey="count" radius={0} fill="#6fd4f5" fillOpacity={0.25} />
            </BarChart>
          </ResponsiveContainer>
        ) : undefined}
      />
      <Card
        accent="#e04a5f"
        number={fmtNum(flaggedCount)}
        label="ACTIVE ALERTS"
        sub="Top-decile composite score"
        sparkline={deptAlertData.length > 0 ? (
          <ResponsiveContainer width="100%" height={24}>
            <BarChart data={deptAlertData} margin={{ top: 0, right: 0, bottom: 0, left: 0 }}>
              <Bar dataKey="count" radius={0} fill="#e04a5f" fillOpacity={0.3} />
            </BarChart>
          </ResponsiveContainer>
        ) : undefined}
      />
      <Card
        accent="#f0a878"
        number={fmtCop(kpi.exposure_cop)}
        label="EXPOSURE AT RISK"
        sub={`${fmtUsd(kpi.exposure_cop)} · Flagged contract value`}
        sparkline={exposureData.length > 0 ? (
          <ResponsiveContainer width="100%" height={24}>
            <AreaChart data={exposureData} margin={{ top: 0, right: 0, bottom: 0, left: 0 }}>
              <defs>
                <linearGradient id="exposureGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#f0a878" stopOpacity={0.2} />
                  <stop offset="100%" stopColor="#f0a878" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <Area type="monotone" dataKey="value" stroke="#f0a878" strokeOpacity={0.4} strokeWidth={1} fill="url(#exposureGrad)" />
            </AreaChart>
          </ResponsiveContainer>
        ) : undefined}
      />
    </div>
  );
}

function Card({
  accent,
  number,
  label,
  sub,
  sparkline,
}: {
  accent: string;
  number: string;
  label: string;
  sub: string;
  sparkline?: React.ReactNode;
}) {
  return (
    <div className="flex-1 panel p-3 overflow-hidden relative">
      <div
        className="absolute top-0 left-0 w-[2px] h-full"
        style={{ backgroundColor: accent, opacity: 0.6 }}
      />
      <div className="font-mono text-[22px] font-bold tracking-tight text-[#d0d0d8] leading-none tabular-nums">
        {number}
      </div>
      <div
        className="font-mono text-[9px] font-medium mt-1.5 uppercase tracking-[0.08em] opacity-50"
        style={{ color: accent }}
      >
        {label}
      </div>
      <div className="font-mono text-[10px] text-[#3a3a42] mt-0.5">{sub}</div>
      {sparkline && <div className="mt-2 -mx-1">{sparkline}</div>}
    </div>
  );
}
