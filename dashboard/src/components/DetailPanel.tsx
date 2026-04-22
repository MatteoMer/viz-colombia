import { motion } from "framer-motion";
import type { Contract } from "../types";
import { fmtCop, fmtUsd, titleCase, scoreBadgeClass, SIGNAL_LABELS, SIGNAL_DESCRIPTIONS, SIGNAL_CATS } from "../utils";
import { Tooltip } from "./Tooltip";
import { ContextCardComponent } from "./ContextCard";

const DQ_FLAG_LABELS: Record<string, string> = {
  natural_person_mega: "Natural person (C\u00e9dula) awarded >500M COP \u2014 likely a data-entry error in contract value.",
  entity_median_outlier: "Contract value exceeds 100\u00d7 the entity\u2019s median \u2014 possible extra zeros or unit confusion.",
  value_payment_mismatch: "Contract value is >10\u00d7 the highest payment recorded \u2014 awarded amount may be inflated.",
};

const COHORT_NAMES: Record<string, string> = {
  competitive: "Competitive (licitaci\u00f3n / selecci\u00f3n abreviada)",
  directa: "Contrataci\u00f3n directa",
  minima: "M\u00ednima cuant\u00eda",
  especial: "R\u00e9gimen especial",
  especial_ese: "R\u00e9gimen especial (E.S.E. salud)",
  especial_universidad: "R\u00e9gimen especial (universidad)",
  especial_d092: "R\u00e9gimen especial (Decreto 092)",
  especial_convenio: "R\u00e9gimen especial (convenio)",
  especial_otro: "R\u00e9gimen especial (otro)",
  mandato: "Mandato sin representaci\u00f3n",
  eice: "EICE (empresa industrial y comercial)",
};

const CAT_LABELS: Record<string, string> = {
  execution: "EXECUTION",
  competition: "COMPETITION",
  pricing: "PRICING",
  relationships: "RELATIONSHIPS",
};

export function DetailPanel({
  contract: c,
  onClose,
  onViewPortfolio,
}: {
  contract: Contract;
  onClose: () => void;
  onViewPortfolio?: (supplierNit: string) => void;
}) {
  const allZ = [
    ...Object.values(c.z ?? {}).map(Math.abs),
    ...Object.values(c.z_global ?? {}).map(Math.abs),
  ];
  const maxZ = Math.max(3, ...allZ);
  const exempt = new Set(c.exempt ?? []);

  const displayedSignals = new Set(Object.values(SIGNAL_CATS).flat());
  const redSignals = Object.entries(c.z ?? {})
    .filter(([k, v]) => Math.abs(v) > 1 && !exempt.has(k) && displayedSignals.has(k))
    .sort(([, a], [, b]) => Math.abs(b) - Math.abs(a))
    .map(([k]) => k);

  const hasAdj = c.composite_adj != null && Math.abs(c.composite_adj - c.composite) > 0.01;

  return (
    <>
      <motion.div
        className="fixed inset-0 bg-black/60 z-40"
        onClick={onClose}
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
      />

      <motion.div
        className="fixed top-0 right-0 bottom-0 w-[440px] bg-[#0a0a0f] border-l border-[#1a1a22] z-50 shadow-2xl shadow-black/40 flex flex-col"
        initial={{ x: 440 }}
        animate={{ x: 0 }}
        exit={{ x: 440 }}
        transition={{ type: "spring", damping: 30, stiffness: 300 }}
      >
        {/* Header */}
        <div className="flex justify-between items-start px-5 py-3 border-b border-[#1a1a22] shrink-0">
          <div>
            <h3 className="font-mono text-[12px] font-bold text-[#d0d0d8] leading-snug">{c.id}</h3>
            <div className="font-mono text-[10px] text-[#555560] mt-0.5">{c.entity}</div>
          </div>
          <button
            onClick={onClose}
            className="text-lg text-[#555560] hover:text-[#d0d0d8] px-2 py-1 leading-none transition-colors"
          >
            &times;
          </button>
        </div>

        <div className="flex-1 overflow-y-auto">

          {/* Verdict strip */}
          {!c.dq_excluded ? (
            <div className="px-5 py-3 bg-[#0d0d12] border-b border-[#1a1a22] space-y-2">
              <div className="flex items-center gap-4 flex-wrap">
                <div>
                  <div className="section-label">VALUE</div>
                  <div className="font-mono text-[12px] font-medium text-[#d0d0d8]">{fmtCop(c.value)} <span className="text-[#3a3a42]">({fmtUsd(c.value)})</span></div>
                </div>
                <div className="w-px h-6 bg-[#1a1a22]" />
                <div>
                  <div className="section-label">SCORE</div>
                  <div className="flex items-baseline gap-1.5">
                    {hasAdj ? (
                      <Tooltip text="The context-adjusted score accounts for thin markets, consortia, and special procurement regimes that may explain some signals.">
                        <span className="cursor-help">
                          <span className="font-mono text-[10px] text-[#3a3a42] line-through">{c.composite.toFixed(2)} (P{(c.pctl * 100).toFixed(0)})</span>
                          {" "}
                          <span className={`inline-block px-1.5 py-0 font-mono text-[10px] font-medium border ${scoreBadgeClass(c.composite_adj!)}`}>
                            {c.composite_adj!.toFixed(2)}
                          </span>
                          <span className="font-mono text-[10px] text-[#555560] ml-1">P{((c.pctl_adj ?? c.pctl) * 100).toFixed(0)} adj</span>
                        </span>
                      </Tooltip>
                    ) : (
                      <>
                        <span className={`inline-block px-1.5 py-0 font-mono text-[10px] font-medium border ${scoreBadgeClass(c.composite)}`}>
                          {c.composite.toFixed(2)}
                        </span>
                        <span className="font-mono text-[10px] text-[#555560]">P{(c.pctl * 100).toFixed(0)}</span>
                      </>
                    )}
                  </div>
                </div>
                {redSignals.length > 0 && (
                  <>
                    <div className="w-px h-6 bg-[#1a1a22]" />
                    <div>
                      <div className="section-label">ELEVATED</div>
                      <div className="font-mono text-[10px] text-[#e04a5f] font-medium">
                        {redSignals.length} signal{redSignals.length > 1 ? "s" : ""}: {redSignals.map((k) => SIGNAL_LABELS[k] ?? k).join(", ")}
                      </div>
                    </div>
                  </>
                )}
              </div>
            </div>
          ) : (
            <div className="px-5 py-3 bg-[#f0a878]/5 border-b border-[#1a1a22]">
              <div className="flex items-center gap-2 font-mono text-[10px] text-[#f0a878]">
                <span className="font-medium">&#x26A0; DATA QUALITY REVIEW</span>
                <span className="text-[#3a3a42]">&mdash;</span>
                <span>Excluded from scoring</span>
              </div>
              {(c.dq_flags || "").split(",").filter(Boolean).length > 0 && (
                <ul className="mt-1.5 ml-4 list-disc text-[10px] text-[#f0a878]/70 space-y-0.5 font-mono">
                  {(c.dq_flags || "").split(",").filter(Boolean).map((flag) => (
                    <li key={flag}>{DQ_FLAG_LABELS[flag] ?? flag}</li>
                  ))}
                </ul>
              )}
            </div>
          )}

          <div className="px-5 py-4 space-y-4">

            {/* Banners */}
            {!c.dq_excluded && c.s1_not_eval && (
              <Banner color="cyan" title="PAYMENT STALL NOT EVALUATED">
                This contract has no recorded payments in SECOP II. The payment stall signal is set to neutral.
              </Banner>
            )}
            {!c.dq_excluded && c.ranking_unstable && (
              <Banner color="amber" title="RANKING UNSTABLE">
                This contract appears in the top-50 in fewer than 90% of bootstrap resamples.
              </Banner>
            )}
            {(c.is_mandato || c.is_eice) && (
              <Banner color="amber" title={c.is_mandato ? "MANDATO SIN REPRESENTACI\u00d3N" : "EICE ENTITY"}>
                {c.is_mandato && c.is_eice
                  ? "Awarded via mandato to a municipal development entity (EICE). "
                  : c.is_mandato
                    ? "Awarded via mandato sin representaci\u00f3n. "
                    : "Awarded to a municipal development entity (EICE). "}
                Signals scored against peer contracts of the same type.
              </Banner>
            )}

            {/* Contract details */}
            <Section title="CONTRACT DETAILS" id="contract">
              <div className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-0.5">
                <GridRow label="ENTITY" value={c.entity} />
                {c.entity_nit && <GridRow label="ENTITY NIT" value={c.entity_nit} />}
                <GridRow label="SUPPLIER" value={c.supplier} />
                {c.supplier_nit && <GridRow label="SUPPLIER NIT" value={c.supplier_nit} />}
                <GridRow label="LOCATION">
                  <span className="flex items-center gap-1.5 flex-wrap justify-end">
                    {titleCase(c.dept)} · {titleCase(c.muni || "\u2014")}
                    {c.ctx?.is_pdet && <Chip>PDET</Chip>}
                    {c.ctx?.is_zomac && <Chip>ZOMAC</Chip>}
                  </span>
                </GridRow>
                <GridRow label="VALUE" value={`${fmtCop(c.value)} (${fmtUsd(c.value)})`} />
                <GridRow label="STATUS" value={c.status || "\u2014"} />
                <GridRow label="COHORT" value={COHORT_NAMES[c.cohort] ?? c.cohort} />
              </div>
              {c.supplier_nit && onViewPortfolio && (
                <button
                  onClick={() => onViewPortfolio(c.supplier_nit!)}
                  className="mt-2 font-mono text-[10px] px-2 py-0.5 font-medium text-[#6fd4f5] border border-[#6fd4f5]/30 hover:bg-[#6fd4f5]/5 transition-colors uppercase tracking-wider"
                >
                  VIEW SUPPLIER PORTFOLIO &rarr;
                </button>
              )}
              {c.desc && (
                <p className="mt-3 font-mono text-[10px] text-[#555560] leading-relaxed bg-[#0d0d12] border border-[#1a1a22] p-3">
                  {c.desc}
                </p>
              )}
            </Section>

            {/* Signals */}
            {!c.dq_excluded && c.z && (
              <Section title="SIGNALS" id="signals">
                <p className="font-mono text-[9px] text-[#3a3a42] mb-3 uppercase tracking-wider">
                  Solid = cohort · Ghost = global · Cohort: <strong className="text-[#555560]">{COHORT_NAMES[c.cohort] ?? c.cohort}</strong>
                </p>
                {Object.entries(SIGNAL_CATS).map(([catKey, signals]) => (
                  <div key={catKey} className="mb-3">
                    <h5 className="section-label mb-1.5">
                      {CAT_LABELS[catKey] ?? catKey}
                    </h5>
                    <div className="flex flex-col gap-1">
                      {signals.map((key) => (
                        <SignalBar
                          key={key}
                          signalKey={key}
                          z={c.z}
                          zGlobal={c.z_global}
                          maxZ={maxZ}
                          isExempt={exempt.has(key)}
                          cohort={c.cohort}
                        />
                      ))}
                    </div>
                  </div>
                ))}
              </Section>
            )}

            {/* Context */}
            {!c.dq_excluded && ((c.cards && c.cards.length > 0) || c.ctx) && (
              <Section title="CONTEXT" id="context">
                {c.cards && c.cards.length > 0 && (
                  <div className="flex flex-col gap-1.5 mb-3">
                    {c.cards.map((card, i) => (
                      <ContextCardComponent key={i} card={card} />
                    ))}
                  </div>
                )}
                {c.ctx && (
                  <div className="grid grid-cols-3 gap-2">
                    <CtxStat label="POPULATION" value={c.ctx.pop.toLocaleString()} />
                    <CtxStat label="RURALITY" value={`${(c.ctx.rurality * 100).toFixed(0)}%`} />
                    <CtxStat label="DIST. CAPITAL" value={`${c.ctx.dist_capital_km.toFixed(0)} km`} />
                  </div>
                )}
              </Section>
            )}

            {/* Footer */}
            {c.url && (
              <div className="pt-2 pb-3 border-t border-[#1a1a22]">
                <a
                  href={c.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="font-mono text-[10px] font-medium text-[#6fd4f5] hover:underline uppercase tracking-wider"
                >
                  VIEW ON SECOP II &rarr;
                </a>
              </div>
            )}
          </div>
        </div>
      </motion.div>
    </>
  );
}

// ── Sub-components ──

function Section({ title, id, children }: { title: string; id?: string; children: React.ReactNode }) {
  return (
    <div id={id}>
      <h4 className="section-label mb-2">{title}</h4>
      {children}
    </div>
  );
}

function GridRow({ label, value, children }: { label: string; value?: string; children?: React.ReactNode }) {
  return (
    <>
      <span className="font-mono text-[10px] text-[#3a3a42] uppercase tracking-wider py-0.5">{label}</span>
      <span className="font-mono text-[10px] text-[#888890] font-medium text-right py-0.5 truncate">
        {children ?? value}
      </span>
    </>
  );
}

function Chip({ children }: { children: React.ReactNode }) {
  return <span className="inline-block px-1 py-0 border border-[#2a2a32] font-mono text-[8px] font-medium text-[#555560] uppercase tracking-wider">{children}</span>;
}

function CtxStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-[#0d0d12] border border-[#1a1a22] p-2">
      <div className="section-label">{label}</div>
      <div className="font-mono text-[11px] font-medium text-[#888890] mt-0.5 tabular-nums">{value}</div>
    </div>
  );
}

function Banner({ color, title, children }: { color: "cyan" | "amber" | "orange"; title: string; children: React.ReactNode }) {
  const borderColor = color === "cyan" ? "#6fd4f5" : color === "amber" ? "#f0a878" : "#f0a878";
  const textColor = color === "cyan" ? "#6fd4f5" : "#f0a878";
  return (
    <div
      className="border-l-2 px-3 py-2 font-mono text-[10px] leading-relaxed"
      style={{ borderColor, color: textColor, backgroundColor: `${borderColor}08` }}
    >
      <span className="font-medium">{title}</span> &mdash; <span className="opacity-70">{children}</span>
    </div>
  );
}

function SignalBar({
  signalKey,
  z,
  zGlobal,
  maxZ,
  isExempt,
  cohort,
}: {
  signalKey: string;
  z: Record<string, number>;
  zGlobal?: Record<string, number>;
  maxZ: number;
  isExempt: boolean;
  cohort: string;
}) {
  const label = SIGNAL_LABELS[signalKey] ?? signalKey;
  const v = z[signalKey] ?? 0;
  const vGlobal = zGlobal?.[signalKey] ?? 0;
  const color = isExempt
    ? "#3a3a42"
    : v > 1 ? "#e04a5f" : v > 0.5 ? "#f0a878" : v > 0 ? "#f0a878" : "#6fd4f5";
  const tooltipText = isExempt
    ? `${SIGNAL_DESCRIPTIONS[signalKey]}\n\nExempt for ${COHORT_NAMES[cohort] ?? cohort} contracts.`
    : `${SIGNAL_DESCRIPTIONS[signalKey]}\n\nCohort: ${v.toFixed(1)}\u03c3. Global: ${vGlobal.toFixed(1)}\u03c3.`;

  return (
    <div className={`flex items-center gap-2 ${isExempt ? "opacity-30" : ""}`}>
      <Tooltip text={tooltipText}>
        <span className="w-[80px] font-mono text-[9px] text-[#555560] text-right shrink-0 cursor-help border-b border-dashed border-[#1a1a22] uppercase tracking-wider">
          {label}
          {isExempt && <span className="block text-[8px] text-[#3a3a42] leading-tight">(exempt)</span>}
        </span>
      </Tooltip>
      <svg className="flex-1 h-4" viewBox="0 0 200 16" preserveAspectRatio="none">
        <line x1="0" y1="8" x2={Math.min(200, (Math.abs(vGlobal) / maxZ) * 200)} y2="8" stroke="#2a2a32" strokeWidth="2" />
        {!isExempt && (
          <>
            <line x1="0" y1="8" x2={Math.min(200, (Math.abs(v) / maxZ) * 200)} y2="8" stroke={color} strokeWidth="2" />
            <rect x={Math.max(0, Math.min(200, (Math.abs(v) / maxZ) * 200) - 2)} y="5" width="4" height="6" fill={color} />
          </>
        )}
      </svg>
      <span className="w-10 font-mono text-[9px] font-medium text-[#555560] text-right shrink-0 tabular-nums">
        {isExempt ? "n/a" : <>{v.toFixed(1)}&sigma;</>}
      </span>
    </div>
  );
}
