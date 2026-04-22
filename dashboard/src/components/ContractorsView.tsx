import { useState, useMemo } from "react";
import type { Contractor } from "../types";
import { fmtCop, scoreBadgeClass } from "../utils";
import { Tooltip } from "./Tooltip";

type SortKey = "name" | "n" | "flagged" | "exposure" | "composite" | "signals";

export function ContractorsView({ contractors }: { contractors: Contractor[] }) {
  const [sortKey, setSortKey] = useState<SortKey>("composite");
  const [asc, setAsc] = useState(false);

  const sorted = useMemo(() => {
    return [...contractors].sort((a, b) => {
      const va = a[sortKey];
      const vb = b[sortKey];
      if (typeof va === "string" && typeof vb === "string")
        return asc ? va.localeCompare(vb) : vb.localeCompare(va);
      return asc ? (va as number) - (vb as number) : (vb as number) - (va as number);
    });
  }, [contractors, sortKey, asc]);

  function toggleSort(key: SortKey) {
    if (sortKey === key) setAsc(!asc);
    else { setSortKey(key); setAsc(false); }
  }

  const TH = ({ k, children, className = "" }: { k: SortKey; children: React.ReactNode; className?: string }) => (
    <th
      onClick={() => toggleSort(k)}
      className={`bg-[#0a0a0f]/95 text-[#555560] font-mono text-[9px] font-medium uppercase tracking-[0.08em] text-left px-3 py-1.5 border-b border-[#1a1a22] cursor-pointer whitespace-nowrap hover:text-[#888890] select-none ${className}`}
    >
      {children}
      <span className={`ml-1 text-[8px] ${sortKey === k ? "text-[#6fd4f5] opacity-100" : "opacity-20"}`}>
        {sortKey === k ? (asc ? "\u25B2" : "\u25BC") : "\u25B2"}
      </span>
    </th>
  );

  return (
    <>
      <div className="flex justify-between items-center">
        <h2 className="font-display text-[14px] font-bold text-[#d0d0d8] uppercase tracking-wide">Contractor League</h2>
        <span className="font-mono text-[9px] text-[#3a3a42] px-2 py-0.5 border border-[#1a1a22] tabular-nums uppercase tracking-wider">
          {contractors.length} RECORDS
        </span>
      </div>
      <div className="panel flex-1 overflow-auto">
        <table className="w-full border-collapse font-mono text-[10px]">
          <thead className="sticky top-0 z-[2]">
            <tr>
              <TH k="name">CONTRACTOR</TH>
              <TH k="n" className="text-right">N</TH>
              <TH k="flagged" className="text-right">FLAGGED</TH>
              <TH k="exposure" className="text-right">EXPOSURE</TH>
              <TH k="composite" className="text-right">SCORE</TH>
              <TH k="signals">SIGNALS</TH>
            </tr>
          </thead>
          <tbody>
            {sorted.map((c) => (
              <tr key={c.id} className="table-row-hover transition-colors">
                <td className="px-3 py-1.5 border-b border-[#1a1a22]/50 text-[#888890] max-w-[280px] truncate" title={c.name}>
                  <span className="inline-flex items-center gap-1.5">
                    {c.name}
                    {c.donor && (
                      <Tooltip text={`Campaign donor: ${c.donor.candidates} (${c.donor.positions}). Total donated: COP ${c.donor.total.toLocaleString()}`}>
                        <span className="inline-block px-1 py-0 border border-[#f0a878]/40 bg-[#f0a878]/15 font-mono text-[7px] font-bold text-[#f0a878] uppercase tracking-wider cursor-help shrink-0">
                          DONOR
                        </span>
                      </Tooltip>
                    )}
                  </span>
                </td>
                <td className="px-3 py-1.5 border-b border-[#1a1a22]/50 text-right tabular-nums text-[#555560]">
                  {c.n}
                </td>
                <td className="px-3 py-1.5 border-b border-[#1a1a22]/50 text-right tabular-nums text-[#555560]">
                  {c.flagged}
                </td>
                <td className="px-3 py-1.5 border-b border-[#1a1a22]/50 text-right tabular-nums text-[#555560]">
                  {fmtCop(c.exposure)}
                </td>
                <td className="px-3 py-1.5 border-b border-[#1a1a22]/50 text-right">
                  <span className={`inline-block px-1.5 py-0 border font-medium text-[9px] ${scoreBadgeClass(c.composite)}`}>
                    {c.composite.toFixed(2)}
                  </span>
                </td>
                <td className="px-3 py-1.5 border-b border-[#1a1a22]/50 text-[9px] text-[#3a3a42] max-w-[250px] truncate">
                  {c.signals}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
