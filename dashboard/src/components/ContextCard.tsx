import { useState } from "react";
import type { ContextCard as ContextCardType } from "../types";

const CONFIDENCE_BADGE: Record<string, { cls: string; label: string }> = {
  high: { cls: "text-[#6fd4f5] border-[#6fd4f5]/30", label: "HIGH" },
  moderate: { cls: "text-[#555560] border-[#2a2a32]", label: "MOD" },
  low: { cls: "text-[#3a3a42] border-[#1a1a22]", label: "LOW" },
};

export function ContextCardComponent({ card }: { card: ContextCardType }) {
  const [expanded, setExpanded] = useState(false);
  const badge = card.confidence ? CONFIDENCE_BADGE[card.confidence] : null;
  const isAlert = card.type === "no_explanation";

  return (
    <div
      className={`border px-3 py-2 text-xs cursor-pointer transition-colors ${
        isAlert
          ? "bg-[#e04a5f]/5 border-[#e04a5f]/20 hover:bg-[#e04a5f]/8"
          : "bg-[#111116] border-[#1a1a22] hover:bg-[#141419]"
      }`}
      onClick={() => setExpanded(!expanded)}
    >
      <div className="flex items-start gap-2">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className={`font-mono text-[11px] font-medium ${isAlert ? "text-[#e04a5f]" : "text-[#d0d0d8]"}`}>
              {card.headline}
            </span>
            {badge && (
              <span className={`inline-block px-1.5 py-0 border font-mono text-[8px] font-medium tracking-wider ${badge.cls}`}>
                {badge.label}
              </span>
            )}
          </div>
          {expanded && (
            <p className="mt-1.5 text-[#555560] leading-relaxed text-[11px]">
              {card.explanation}
            </p>
          )}
          {expanded && card.members && card.members.length > 0 && (
            <div className="mt-2 space-y-0.5">
              <span className="font-mono text-[9px] text-[#3a3a42] uppercase tracking-wider">Members:</span>
              {card.members.map((m) => (
                <div key={m.nit} className="flex justify-between font-mono text-[10px] text-[#555560] pl-2">
                  <span className="truncate max-w-[200px]">{m.name || m.nit}</span>
                  <span className="text-[#3a3a42] shrink-0 ml-2">{m.pct}%</span>
                </div>
              ))}
            </div>
          )}
          {!expanded && (
            <span className="text-[#3a3a42] text-[9px] font-mono">EXPAND</span>
          )}
        </div>
      </div>
    </div>
  );
}
