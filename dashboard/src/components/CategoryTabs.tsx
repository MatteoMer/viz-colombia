import type { Category } from "../types";
import { Tooltip } from "./Tooltip";

const TABS: { key: Category; label: string; desc: string }[] = [
  { key: "all", label: "All Signals", desc: "Composite score combining all anomaly signals" },
  { key: "execution", label: "Execution", desc: "Payment stalls and schedule slippage — is the work actually getting done on time?" },
  { key: "competition", label: "Competition", desc: "Single-bidder rates, threshold bunching, and award speed — was the bidding process genuinely competitive?" },
  { key: "pricing", label: "Pricing", desc: "Value creep at contract and contractor level — are costs escalating beyond estimates?" },
  { key: "relationships", label: "Relationships", desc: "HHI concentration and entity-contractor relationship intensity — are the same contractors always winning?" },
];

export function CategoryTabs({
  category,
  onChange,
}: {
  category: Category;
  onChange: (c: Category) => void;
}) {
  return (
    <div className="flex gap-1 glass rounded-lg p-1">
      {TABS.map((t) => (
        <Tooltip key={t.key} text={t.desc}>
          <button
            onClick={() => onChange(t.key)}
            className={`px-3.5 py-1.5 rounded-md text-[11px] font-semibold transition-all ${
              category === t.key
                ? "bg-white/[0.06] text-white/80"
                : "text-zinc-600 hover:text-zinc-400 hover:bg-white/[0.03]"
            }`}
          >
            {t.label}
          </button>
        </Tooltip>
      ))}
    </div>
  );
}
