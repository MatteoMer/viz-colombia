import { useState, useMemo } from "react";
import type { Department } from "../types";
import { titleCase, fmtNum, fmtPct, fmtCop } from "../utils";

type SortKey = keyof Department;

export function DeptTable({ departments }: { departments: Department[] }) {
  const [sortKey, setSortKey] = useState<SortKey>("n_flagged");
  const [asc, setAsc] = useState(false);

  const sorted = useMemo(() => {
    return [...departments].sort((a, b) => {
      const va = a[sortKey];
      const vb = b[sortKey];
      if (typeof va === "string" && typeof vb === "string")
        return asc ? va.localeCompare(vb) : vb.localeCompare(va);
      return asc ? (va as number) - (vb as number) : (vb as number) - (va as number);
    });
  }, [departments, sortKey, asc]);

  function toggleSort(key: SortKey) {
    if (sortKey === key) setAsc(!asc);
    else { setSortKey(key); setAsc(false); }
  }

  const TH = ({ k, children }: { k: SortKey; children: React.ReactNode }) => (
    <th
      onClick={() => toggleSort(k)}
      className="bg-[#0a0a0f]/95 text-[#555560] font-mono text-[9px] font-medium uppercase tracking-[0.08em] text-left px-3 py-1.5 border-b border-[#1a1a22] cursor-pointer whitespace-nowrap hover:text-[#888890] select-none"
    >
      {children}
      <span className={`ml-1 text-[7px] ${sortKey === k ? "text-[#6fd4f5] opacity-100" : "opacity-20"}`}>
        {sortKey === k ? (asc ? "\u25B2" : "\u25BC") : "\u25B2"}
      </span>
    </th>
  );

  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="flex-1 overflow-y-auto">
        <table className="w-full border-collapse font-mono text-[10px]">
          <thead className="sticky top-0 z-[2]">
            <tr>
              <TH k="name">DEPT</TH>
              <TH k="n_contracts">N</TH>
              <TH k="n_flagged">FLAG</TH>
              <TH k="flag_rate">RATE</TH>
              <TH k="exposure">EXPOSURE</TH>
            </tr>
          </thead>
          <tbody>
            {sorted.map((d) => (
              <tr key={d.name} className="table-row-hover transition-colors">
                <td className="px-3 py-1 border-b border-[#1a1a22]/50 text-[#888890]">
                  {titleCase(d.name)}
                </td>
                <td className="px-3 py-1 border-b border-[#1a1a22]/50 text-right tabular-nums text-[#555560]">
                  {fmtNum(d.n_contracts)}
                </td>
                <td className="px-3 py-1 border-b border-[#1a1a22]/50 text-right tabular-nums">
                  {d.n_flagged > 0 ? (
                    <span className="text-[#f0a878]">{d.n_flagged}</span>
                  ) : (
                    <span className="text-[#2a2a32]">0</span>
                  )}
                </td>
                <td className="px-3 py-1 border-b border-[#1a1a22]/50 text-right tabular-nums text-[#555560] relative">
                  <div className="flex items-center justify-end gap-1.5">
                    <div className="w-10 h-[2px] bg-[#1a1a22] overflow-hidden">
                      <div
                        className="h-full bg-[#6fd4f5]/40"
                        style={{ width: `${Math.max(...departments.map(x => x.flag_rate)) > 0 ? (d.flag_rate / Math.max(...departments.map(x => x.flag_rate))) * 100 : 0}%` }}
                      />
                    </div>
                    <span className="w-10 text-right">{fmtPct(d.flag_rate)}</span>
                  </div>
                </td>
                <td className="px-3 py-1 border-b border-[#1a1a22]/50 text-right tabular-nums text-[#555560]">
                  {d.exposure > 0 ? fmtCop(d.exposure) : <span className="text-[#1a1a22]">&mdash;</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
