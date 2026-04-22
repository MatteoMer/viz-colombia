import { useState, useEffect, useRef } from "react";
import { AnimatePresence, motion } from "framer-motion";
import type { Dot, Department, Category } from "../types";
import { RankedList } from "./RankedList";
import { DeptTable } from "./DeptTable";

type Tab = "contracts" | "departments";

interface Props {
  dots: Dot[];
  departments: Department[];
  category: Category;
  deptFilter?: string | null;
  onClearDeptFilter?: () => void;
  highlightedId?: string | null;
  onDotHover: (dot: Dot | null) => void;
  onDotClick: (dot: Dot) => void;
}

export function SidePanel({
  dots,
  departments,
  category,
  deptFilter,
  onClearDeptFilter,
  highlightedId,
  onDotHover,
  onDotClick,
}: Props) {
  const [tab, setTab] = useState<Tab>("contracts");
  const [collapsed, setCollapsed] = useState(false);
  const prevDeptFilter = useRef(deptFilter);

  useEffect(() => {
    if (deptFilter && !prevDeptFilter.current) {
      setCollapsed(true);
    } else if (!deptFilter && prevDeptFilter.current) {
      setCollapsed(false);
    }
    prevDeptFilter.current = deptFilter;
  }, [deptFilter]);

  return (
    <div className="absolute top-3 right-3 bottom-3 z-10 pointer-events-auto flex">
      <button
        onClick={() => setCollapsed(!collapsed)}
        className="self-start mt-2 -mr-px panel px-1.5 py-3 text-[#555560] hover:text-[#888890] transition-colors"
      >
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1">
          {collapsed ? (
            <><rect x="1" y="1" width="12" height="12" /><line x1="5" y1="1" x2="5" y2="13" /></>
          ) : (
            <><rect x="1" y="1" width="12" height="12" /><line x1="9" y1="1" x2="9" y2="13" /></>
          )}
        </svg>
      </button>
      <AnimatePresence>
        {!collapsed && (
          <motion.div
            initial={{ width: 0, opacity: 0 }}
            animate={{ width: 350, opacity: 1 }}
            exit={{ width: 0, opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="panel flex flex-col overflow-hidden"
          >
            {/* Tabs */}
            <div className="flex border-b border-[#1a1a22]">
              <button
                onClick={() => setTab("contracts")}
                className={`flex-1 px-4 py-2 font-mono text-[10px] font-medium uppercase tracking-[0.08em] transition-colors relative ${
                  tab === "contracts"
                    ? "text-[#6fd4f5]"
                    : "text-[#555560] hover:text-[#888890]"
                }`}
              >
                {tab === "contracts" && (
                  <div className="absolute bottom-0 left-0 right-0 h-[1px] bg-[#6fd4f5]" />
                )}
                CONTRACTS
              </button>
              <button
                onClick={() => setTab("departments")}
                className={`flex-1 px-4 py-2 font-mono text-[10px] font-medium uppercase tracking-[0.08em] transition-colors relative ${
                  tab === "departments"
                    ? "text-[#6fd4f5]"
                    : "text-[#555560] hover:text-[#888890]"
                }`}
              >
                {tab === "departments" && (
                  <div className="absolute bottom-0 left-0 right-0 h-[1px] bg-[#6fd4f5]" />
                )}
                DEPARTMENTS
              </button>
            </div>

            <div className="flex-1 min-h-0">
              {tab === "contracts" ? (
                <RankedList
                  dots={dots}
                  category={category}
                  deptFilter={deptFilter}
                  onClearDeptFilter={onClearDeptFilter}
                  onHover={onDotHover}
                  onClick={onDotClick}
                  highlightedId={highlightedId}
                />
              ) : (
                <DeptTable departments={departments} />
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
