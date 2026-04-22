import { useRef, useMemo, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { Dot, Category } from "../types";
import { fmtCop, titleCase, getDotCatScore, scoreBadgeClass } from "../utils";

interface Props {
  dots: Dot[];
  category: Category;
  deptFilter?: string | null;
  onClearDeptFilter?: () => void;
  onHover: (dot: Dot | null) => void;
  onClick: (dot: Dot) => void;
  highlightedId?: string | null;
}

export function RankedList({
  dots,
  category,
  deptFilter,
  onClearDeptFilter,
  onHover,
  onClick,
  highlightedId,
}: Props) {
  const parentRef = useRef<HTMLDivElement>(null);
  const [search, setSearch] = useState("");

  const sorted = useMemo(() => {
    let filtered = dots.filter((d) => d.q !== 1);
    if (deptFilter) {
      filtered = filtered.filter((d) => d.d === deptFilter);
    }
    if (search) {
      const q = search.toLowerCase();
      filtered = filtered.filter(
        (d) =>
          d.i.toLowerCase().includes(q) ||
          d.e.toLowerCase().includes(q) ||
          d.d.toLowerCase().includes(q),
      );
    }
    return filtered.sort((a, b) => getDotCatScore(b, category) - getDotCatScore(a, category));
  }, [dots, category, deptFilter, search]);

  const virtualizer = useVirtualizer({
    count: sorted.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 40,
    overscan: 10,
  });

  return (
    <div className="flex flex-col h-full">
      {deptFilter && onClearDeptFilter && (
        <div className="px-3 pt-2">
          <button
            onClick={onClearDeptFilter}
            className="inline-flex items-center gap-1.5 px-2 py-0.5 border border-[#6fd4f5]/30 text-[#6fd4f5] font-mono text-[9px] font-medium uppercase tracking-wider hover:bg-[#6fd4f5]/5 transition-colors"
          >
            {titleCase(deptFilter)}
            <span className="text-[10px]">&times;</span>
          </button>
        </div>
      )}

      <div className="px-3 py-2 border-b border-[#1a1a22]">
        <div className="relative">
          <svg width="12" height="12" viewBox="0 0 12 12" className="absolute left-2 top-1/2 -translate-y-1/2 text-[#3a3a42]" fill="none" stroke="currentColor" strokeWidth="1">
            <circle cx="5" cy="5" r="3.5" />
            <line x1="7.5" y1="7.5" x2="10.5" y2="10.5" />
          </svg>
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search..."
            className="w-full bg-[#0d0d12] border border-[#1a1a22] pl-7 pr-3 py-1 font-mono text-[10px] text-[#d0d0d8] placeholder:text-[#3a3a42] focus:outline-none focus:border-[#6fd4f5]/30"
          />
        </div>
        <div className="font-mono text-[9px] text-[#3a3a42] mt-1 uppercase tracking-wider">
          {sorted.length.toLocaleString()} records
        </div>
      </div>

      <div ref={parentRef} className="flex-1 overflow-y-auto">
        <div
          style={{
            height: `${virtualizer.getTotalSize()}px`,
            position: "relative",
            width: "100%",
          }}
        >
          {virtualizer.getVirtualItems().map((virtualRow) => {
            const dot = sorted[virtualRow.index];
            const score = getDotCatScore(dot, category);
            const isHighlighted = dot.i === highlightedId;

            return (
              <div
                key={dot.i}
                data-index={virtualRow.index}
                ref={virtualizer.measureElement}
                style={{
                  position: "absolute",
                  top: 0,
                  left: 0,
                  width: "100%",
                  transform: `translateY(${virtualRow.start}px)`,
                }}
                className={`px-3 py-1.5 border-b border-[#1a1a22]/60 cursor-pointer transition-colors relative ${
                  isHighlighted ? "bg-[#6fd4f5]/5" : "hover:bg-[#6fd4f5]/3"
                }`}
                onMouseEnter={() => onHover(dot)}
                onMouseLeave={() => onHover(null)}
                onClick={() => onClick(dot)}
              >
                {isHighlighted && (
                  <div className="absolute left-0 top-0 bottom-0 w-[2px] bg-[#6fd4f5]" />
                )}
                <div className="flex items-start justify-between gap-2">
                  <div className="min-w-0 flex-1">
                    <div className="font-mono text-[10px] text-[#888890] truncate">
                      {dot.e || dot.i}
                    </div>
                    <div className="font-mono text-[9px] text-[#3a3a42] truncate">
                      {titleCase(dot.d)} · {fmtCop(dot.v)}
                    </div>
                  </div>
                  <span
                    className={`shrink-0 inline-block px-1.5 py-0 font-mono text-[9px] font-medium tabular-nums border ${scoreBadgeClass(score)}`}
                  >
                    {score.toFixed(2)}
                  </span>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
