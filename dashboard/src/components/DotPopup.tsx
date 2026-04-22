import type { Dot } from "../types";
import { fmtCop, fmtUsd, titleCase, scoreBadgeClass } from "../utils";

export function DotPopup({ dot, onClose }: { dot: Dot; onClose: () => void }) {
  return (
    <>
      <div
        className="fixed inset-0 bg-black/60 z-40"
        onClick={onClose}
      />
      <div className="fixed top-0 right-0 bottom-0 w-[440px] bg-[#0a0a0f] border-l border-[#1a1a22] z-50 shadow-2xl shadow-black/40 flex flex-col">
        <div className="flex justify-between items-start px-5 py-3 border-b border-[#1a1a22]">
          <div>
            <h3 className="font-mono text-[12px] font-bold text-[#d0d0d8]">{dot.i}</h3>
            <div className="font-mono text-[10px] text-[#555560] mt-0.5">{dot.e}</div>
          </div>
          <button
            onClick={onClose}
            className="text-lg text-[#555560] hover:text-[#d0d0d8] px-2 py-1 leading-none transition-colors"
          >
            &times;
          </button>
        </div>
        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-4">
          <div>
            <h4 className="section-label mb-2">
              BASIC INFO
            </h4>
            <div className="flex justify-between py-1 font-mono text-[10px] border-b border-[#1a1a22]">
              <span className="text-[#3a3a42] uppercase tracking-wider">DEPARTMENT</span>
              <span className="font-medium text-[#888890]">{titleCase(dot.d)}</span>
            </div>
            <div className="flex justify-between py-1 font-mono text-[10px] border-b border-[#1a1a22]">
              <span className="text-[#3a3a42] uppercase tracking-wider">VALUE</span>
              <span className="font-medium text-[#888890]">
                {fmtCop(dot.v)} ({fmtUsd(dot.v)})
              </span>
            </div>
            <div className="flex justify-between py-1 font-mono text-[10px] border-b border-[#1a1a22]">
              <span className="text-[#3a3a42] uppercase tracking-wider">SCORE</span>
              <span className={`inline-block px-1.5 py-0 border font-medium text-[9px] ${scoreBadgeClass(dot.c)}`}>
                {dot.c.toFixed(2)}
              </span>
            </div>
          </div>

          <div className="border-l-2 border-[#f0a878] px-3 py-2 bg-[#f0a878]/5">
            <p className="font-mono text-[10px] text-[#f0a878] leading-relaxed">
              Not in top 500 most anomalous. Detailed signal breakdown available for highest-scoring contracts only. Browse <strong>CONTRACTS</strong> for full details.
            </p>
          </div>
        </div>
      </div>
    </>
  );
}
