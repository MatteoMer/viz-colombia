import { useState, useMemo, useEffect, useRef } from "react";
import type { Contract } from "../types";
import { fmtCop, titleCase, scoreBadgeClass } from "../utils";

type SortKey = "id" | "entity" | "supplier" | "dept" | "value" | "composite" | "signals";

const PAGE_SIZE = 100;

export function ContractsView({
  contracts,
  supplierFilter,
  onClearSupplierFilter,
  onSelect,
  highlightedId,
}: {
  contracts: Contract[];
  supplierFilter?: string | null;
  onClearSupplierFilter?: () => void;
  onSelect: (c: Contract) => void;
  highlightedId?: string | null;
}) {
  const [sortKey, setSortKey] = useState<SortKey>("composite");
  const [asc, setAsc] = useState(false);
  const [query, setQuery] = useState("");
  const [page, setPage] = useState(0);
  const [showDQ, setShowDQ] = useState(false);
  const [hideBenign, setHideBenign] = useState(false);
  const searchRef = useRef<HTMLInputElement>(null);
  const highlightRef = useRef<HTMLTableRowElement>(null);

  const dqCount = useMemo(() => contracts.filter((c) => c.dq_excluded).length, [contracts]);

  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        searchRef.current?.focus();
      }
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  useEffect(() => {
    if (highlightedId && highlightRef.current) {
      highlightRef.current.scrollIntoView({ behavior: "smooth", block: "center" });
    }
  }, [highlightedId]);

  const filtered = useMemo(() => {
    let list = contracts;
    if (supplierFilter) {
      list = list.filter((c) => c.supplier_nit === supplierFilter);
    } else {
      list = list.filter((c) => (showDQ ? c.dq_excluded : !c.dq_excluded));
      if (hideBenign && !showDQ) {
        list = list.filter((c) => {
          if (!c.cards || c.cards.length === 0) return true;
          return !c.cards.some((card) => card.confidence === "high");
        });
      }
    }
    if (!query.trim()) return list;
    const q = query.toLowerCase();
    return list.filter(
      (c) =>
        c.id.toLowerCase().includes(q) ||
        c.entity.toLowerCase().includes(q) ||
        c.supplier.toLowerCase().includes(q) ||
        c.dept.toLowerCase().includes(q) ||
        (c.muni && c.muni.toLowerCase().includes(q)) ||
        (c.desc && c.desc.toLowerCase().includes(q)),
    );
  }, [contracts, query, showDQ, hideBenign, supplierFilter]);

  const sorted = useMemo(() => {
    return [...filtered].sort((a, b) => {
      let va = a[sortKey];
      let vb = b[sortKey];
      if (sortKey === "composite") {
        va = a.composite_adj ?? a.composite;
        vb = b.composite_adj ?? b.composite;
      }
      if (typeof va === "string" && typeof vb === "string")
        return asc ? va.localeCompare(vb) : vb.localeCompare(va);
      return asc ? (va as number) - (vb as number) : (vb as number) - (va as number);
    });
  }, [filtered, sortKey, asc]);

  const totalPages = Math.ceil(sorted.length / PAGE_SIZE);
  const safePage = Math.min(page, Math.max(0, totalPages - 1));
  const pageSlice = sorted.slice(safePage * PAGE_SIZE, (safePage + 1) * PAGE_SIZE);

  function toggleSort(key: SortKey) {
    if (sortKey === key) setAsc(!asc);
    else { setSortKey(key); setAsc(false); }
  }

  function handleSearch(value: string) {
    setQuery(value);
    setPage(0);
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
      {supplierFilter && (
        <div className="flex items-center gap-2 bg-[#6fd4f5]/5 border border-[#6fd4f5]/20 px-3 py-1.5 font-mono text-[10px] text-[#6fd4f5]">
          <span className="font-medium uppercase tracking-wider">PORTFOLIO:</span>
          <span className="text-[#888890]">NIT {supplierFilter}</span>
          <button
            onClick={() => onClearSupplierFilter?.()}
            className="ml-2 px-1.5 py-0 border border-[#6fd4f5]/30 text-[#6fd4f5] font-medium hover:bg-[#6fd4f5]/10 transition-colors"
          >
            CLEAR &times;
          </button>
        </div>
      )}
      <div className="flex justify-between items-center gap-3">
        <h2 className="font-display text-[14px] font-bold text-[#d0d0d8] uppercase tracking-wide shrink-0">Contracts</h2>
        <div className="flex-1 max-w-md relative">
          <input
            ref={searchRef}
            type="text"
            value={query}
            onChange={(e) => handleSearch(e.target.value)}
            placeholder="Search by ID, entity, supplier, department..."
            className="w-full px-3 py-1 font-mono text-[10px] border border-[#1a1a22] bg-[#0d0d12] text-[#d0d0d8] focus:outline-none focus:border-[#6fd4f5]/30 placeholder:text-[#3a3a42]"
          />
          <kbd className="absolute right-2 top-1/2 -translate-y-1/2 font-mono text-[8px] text-[#3a3a42] bg-[#111116] px-1 py-0 border border-[#1a1a22]">
            {"\u2318"}K
          </kbd>
        </div>
        {!showDQ && (
          <button
            onClick={() => { setHideBenign(!hideBenign); setPage(0); }}
            className={`font-mono text-[9px] px-2 py-0.5 font-medium shrink-0 border transition-colors uppercase tracking-wider ${
              hideBenign
                ? "bg-[#6fd4f5]/5 text-[#6fd4f5] border-[#6fd4f5]/30"
                : "bg-transparent text-[#555560] border-[#1a1a22] hover:text-[#888890]"
            }`}
          >
            {hideBenign ? "SHOW ALL" : "HIDE BENIGN"}
          </button>
        )}
        {dqCount > 0 && (
          <button
            onClick={() => { setShowDQ(!showDQ); setPage(0); }}
            className={`font-mono text-[9px] px-2 py-0.5 font-medium shrink-0 border transition-colors uppercase tracking-wider ${
              showDQ
                ? "bg-[#f0a878]/5 text-[#f0a878] border-[#f0a878]/30"
                : "bg-transparent text-[#555560] border-[#1a1a22] hover:text-[#888890]"
            }`}
          >
            {showDQ ? "SCORED" : `DQ (${dqCount})`}
          </button>
        )}
        <span className="font-mono text-[9px] text-[#3a3a42] px-2 py-0.5 border border-[#1a1a22] shrink-0 tabular-nums uppercase tracking-wider">
          {filtered.length.toLocaleString()} RECORDS
        </span>
      </div>
      <div className="panel flex-1 overflow-auto">
        <table className="w-full border-collapse font-mono text-[10px]">
          <thead className="sticky top-0 z-[2]">
            <tr>
              <TH k="id">CONTRACT</TH>
              <TH k="entity">ENTITY</TH>
              <TH k="supplier">SUPPLIER</TH>
              <TH k="dept">DEPT</TH>
              <TH k="value" className="text-right">VALUE</TH>
              <TH k="composite" className="text-right">SCORE</TH>
              <TH k="signals">SIGNALS</TH>
            </tr>
          </thead>
          <tbody>
            {pageSlice.map((c) => (
              <tr
                key={c.id}
                ref={c.id === highlightedId ? highlightRef : undefined}
                onClick={() => onSelect(c)}
                className={`table-row-hover cursor-pointer transition-colors relative ${
                  c.id === highlightedId ? "bg-[#6fd4f5]/5" : ""
                }`}
              >
                {c.id === highlightedId && (
                  <td className="absolute left-0 top-0 bottom-0 w-[2px] bg-[#6fd4f5] p-0" />
                )}
                <td className="px-3 py-1.5 border-b border-[#1a1a22]/50 text-[#555560]">
                  {c.id.replace("CO1.PCCNTR.", "")}
                </td>
                <td className="px-3 py-1.5 border-b border-[#1a1a22]/50 text-[#888890] max-w-[200px] truncate" title={c.entity}>
                  {c.entity}
                </td>
                <td className="px-3 py-1.5 border-b border-[#1a1a22]/50 text-[#888890] max-w-[180px] truncate" title={c.supplier}>
                  {c.supplier}
                </td>
                <td className="px-3 py-1.5 border-b border-[#1a1a22]/50 text-[#555560]">
                  {titleCase(c.dept)}
                </td>
                <td className="px-3 py-1.5 border-b border-[#1a1a22]/50 text-right tabular-nums text-[#555560]">
                  {fmtCop(c.value)}
                </td>
                <td className="px-3 py-1.5 border-b border-[#1a1a22]/50 text-right">
                  {c.dq_excluded ? (
                    <span className="inline-block px-1.5 py-0 border border-[#f0a878]/30 font-medium text-[#f0a878] text-[9px]">
                      DQ
                    </span>
                  ) : (
                    <span
                      className={`inline-block px-1.5 py-0 border font-medium text-[9px] ${scoreBadgeClass(c.composite_adj ?? c.composite)}`}
                      title={c.composite_adj != null && Math.abs(c.composite_adj - c.composite) > 0.01
                        ? `Raw: ${c.composite.toFixed(2)} / Adjusted: ${c.composite_adj.toFixed(2)}`
                        : undefined}
                    >
                      {(c.composite_adj ?? c.composite).toFixed(2)}
                    </span>
                  )}
                </td>
                <td className="px-3 py-1.5 border-b border-[#1a1a22]/50 text-[9px] text-[#3a3a42] max-w-[250px] truncate">
                  {c.signals}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {totalPages > 1 && (
        <div className="flex items-center justify-between pt-1.5 font-mono text-[9px] text-[#555560]">
          <span className="tabular-nums uppercase tracking-wider">
            {(safePage * PAGE_SIZE + 1).toLocaleString()}&ndash;{Math.min((safePage + 1) * PAGE_SIZE, sorted.length).toLocaleString()} of {sorted.length.toLocaleString()}
          </span>
          <div className="flex gap-0.5">
            <PagBtn onClick={() => setPage(0)} disabled={safePage === 0}>&laquo;</PagBtn>
            <PagBtn onClick={() => setPage(safePage - 1)} disabled={safePage === 0}>PREV</PagBtn>
            <span className="px-2 py-0.5 font-medium text-[#888890] tabular-nums">
              {safePage + 1} / {totalPages}
            </span>
            <PagBtn onClick={() => setPage(safePage + 1)} disabled={safePage >= totalPages - 1}>NEXT</PagBtn>
            <PagBtn onClick={() => setPage(totalPages - 1)} disabled={safePage >= totalPages - 1}>&raquo;</PagBtn>
          </div>
        </div>
      )}
    </>
  );
}

function PagBtn({ onClick, disabled, children }: { onClick: () => void; disabled: boolean; children: React.ReactNode }) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className="px-1.5 py-0.5 border border-[#1a1a22] disabled:opacity-20 hover:bg-[#6fd4f5]/5 hover:text-[#6fd4f5] transition-colors font-mono text-[9px] uppercase tracking-wider"
    >
      {children}
    </button>
  );
}
