import { useRef, useEffect, useMemo, useState, useLayoutEffect } from "react";
import { motion } from "framer-motion";
import { select } from "d3-selection";
import { Tooltip } from "./Tooltip";
import { fmtCop } from "../utils";
import type { CampaignDonor, CampaignCandidateDetail } from "../types";

/* ── Constants ─────────────────────────────────────────── */

const AMBER = "#f0a878";
const STROKE_W = 1.5;
const MARKER_SZ = 8;
const H_CONNECTOR = 28; // horizontal connector width (px)
const H_MIN_WIDTH = 360; // min container width for horizontal layout

/* ── Helpers ───────────────────────────────────────────── */

function parseCandidates(donor: CampaignDonor): CampaignCandidateDetail[] {
  if (donor.candidates_detail?.length) return donor.candidates_detail;
  const names = donor.candidates.split(";").map((s) => s.trim()).filter(Boolean);
  const positions = donor.positions.split(",").map((s) => s.trim()).filter(Boolean);
  const parties = donor.parties.split(",").map((s) => s.trim()).filter(Boolean);
  return names.map((name, i) => ({
    name,
    position: positions[i] ?? positions[0] ?? "",
    party: parties[i] ?? parties[0] ?? "",
    donated: names.length === 1 ? donor.total_donated : 0,
  }));
}

function fmtDonation(amt: number): string {
  return amt > 0
    ? `DONATED ${fmtCop(amt)} \u00b7 2019`
    : "DONATED \u00b7 AMOUNT UNDISCLOSED";
}

function fmtPosition(pos: string): string {
  return `${pos.toUpperCase()} \u00b7 CONTROLS ENTITY`;
}

function fmtParties(raw: string, max = 2): string {
  const parts = raw.split(",").map((s) => s.trim()).filter(Boolean);
  if (parts.length <= max) return parts.join(", ");
  return `${parts.slice(0, max).join(", ")} +${parts.length - max} more`;
}

/* ── D3 arrow renderer ─────────────────────────────────── */

let _aid = 0;

function drawArrow(
  svg: SVGSVGElement | null,
  delay: number,
  direction: "vertical" | "horizontal",
  label?: string,
): (() => void) | undefined {
  if (!svg) return;
  const s = select(svg);
  s.selectAll("*").remove();

  const rect = svg.getBoundingClientRect();
  const w = rect.width;
  const h = rect.height;
  if (!w || !h) return;

  const mid = `dg-a${++_aid}`;
  const op = 0.6;

  // Arrowhead marker
  s.append("defs")
    .append("marker")
    .attr("id", mid)
    .attr("viewBox", "0 0 10 10")
    .attr("refX", 9)
    .attr("refY", 5)
    .attr("markerWidth", MARKER_SZ)
    .attr("markerHeight", MARKER_SZ)
    .attr("orient", "auto")
    .append("path")
    .attr("d", "M0,0.5 L9,5 L0,9.5 Z")
    .attr("fill", AMBER)
    .attr("opacity", op);

  let x1: number, y1: number, x2: number, y2: number, len: number;
  if (direction === "vertical") {
    const cx = w / 2;
    x1 = cx; y1 = 2; x2 = cx; y2 = h - 2;
    len = y2 - y1;
  } else {
    const cy = Math.round(h / 2);
    x1 = 2; y1 = cy; x2 = w - 2; y2 = cy;
    len = x2 - x1;
  }

  // Arrow line — animated via stroke-dashoffset
  const line = s
    .append("line")
    .attr("x1", x1).attr("y1", y1)
    .attr("x2", x2).attr("y2", y2)
    .attr("stroke", AMBER)
    .attr("stroke-width", STROKE_W)
    .attr("stroke-opacity", op)
    .attr("marker-end", `url(#${mid})`)
    .attr("stroke-dasharray", len)
    .attr("stroke-dashoffset", len)
    .style("transition", "stroke-dashoffset 300ms ease-out");

  // Label (vertical mode only — horizontal labels are rendered as HTML)
  let text: ReturnType<typeof s.append> | null = null;
  if (direction === "vertical" && label) {
    text = s
      .append("text")
      .attr("x", w / 2 + 14)
      .attr("y", h / 2 + 3)
      .attr("text-anchor", "start")
      .attr("font-family", "'IBM Plex Mono', monospace")
      .attr("font-size", 9)
      .attr("fill", AMBER)
      .attr("fill-opacity", op * 0.85)
      .attr("letter-spacing", "0.05em")
      .text(label)
      .style("opacity", "0")
      .style("transition", "opacity 200ms ease-out");
  }

  const t1 = setTimeout(() => line.attr("stroke-dashoffset", 0), delay);
  const t2 = text
    ? setTimeout(() => text!.style("opacity", "1"), delay + 120)
    : null;

  return () => {
    clearTimeout(t1);
    if (t2) clearTimeout(t2);
  };
}

/* ── Graph node sub-component ──────────────────────────── */

function GNode({
  role,
  name,
  highlighted,
  annotation,
  tooltipText,
  clickable,
  onClick,
  delay,
  children,
}: {
  role: string;
  name: string;
  highlighted?: boolean;
  annotation?: string;
  tooltipText?: string;
  clickable?: boolean;
  onClick?: () => void;
  delay: number;
  children?: React.ReactNode;
}) {
  const inner = (
    <motion.div
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: delay / 1000, duration: 0.15, ease: "easeOut" }}
      onClick={clickable ? onClick : undefined}
      onKeyDown={
        clickable
          ? (e: React.KeyboardEvent) => {
              if (e.key === "Enter" || e.key === " ") {
                e.preventDefault();
                onClick?.();
              }
            }
          : undefined
      }
      tabIndex={clickable ? 0 : undefined}
      role={clickable ? "button" : undefined}
      className={
        clickable
          ? "cursor-pointer focus-visible:ring-1 focus-visible:ring-[#f0a878]/50 outline-none"
          : ""
      }
      style={{
        width: "100%",
        border: `${highlighted ? 1.5 : 1}px solid ${AMBER}`,
        borderRadius: 4,
        background: `rgba(240,168,120,${highlighted ? 0.12 : 0.08})`,
        padding: "8px 10px",
      }}
    >
      <div
        className="font-mono text-[9px] uppercase tracking-wider"
        style={{ color: "rgba(240,168,120,0.5)", marginBottom: 2 }}
      >
        {role}
      </div>
      <div className="font-mono text-[11px] text-[#d0d0d8] leading-snug">
        {name}
      </div>
      {children}
      {annotation && (
        <div
          className="font-mono text-[9px] mt-1"
          style={{ color: AMBER, opacity: 0.7 }}
        >
          {annotation}
        </div>
      )}
    </motion.div>
  );

  return tooltipText ? (
    <Tooltip text={tooltipText}>{inner}</Tooltip>
  ) : (
    inner
  );
}

/* ── Edge label (horizontal mode) ──────────────────────── */

function EdgeLabel({ text, delay }: { text: string; delay: number }) {
  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ delay: delay / 1000, duration: 0.2 }}
      className="overflow-visible text-center whitespace-nowrap font-mono text-[8px] leading-none"
      style={{ color: "rgba(240,168,120,0.55)", letterSpacing: "0.04em" }}
    >
      {text}
    </motion.div>
  );
}

/* ── Main component ────────────────────────────────────── */

export function DonorGraph({
  supplierName,
  supplierNit,
  donor,
  entityName,
  entityNit,
  onViewPortfolio,
}: {
  supplierName: string;
  supplierNit?: string;
  donor: CampaignDonor;
  entityName: string;
  entityNit?: string;
  onViewPortfolio?: (nit: string) => void;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [cWidth, setCWidth] = useState(0);
  const arrow1 = useRef<SVGSVGElement>(null);
  const arrow2 = useRef<SVGSVGElement>(null);

  const candidates = useMemo(() => parseCandidates(donor), [donor]);
  const visible = candidates.slice(0, 3);
  const overflow = Math.max(0, candidates.length - 3);
  const isMulti = visible.length > 1;
  const isH = cWidth >= H_MIN_WIDTH && !isMulti;

  // Measure container width (runs before paint to avoid layout flash)
  useLayoutEffect(() => {
    if (!containerRef.current) return;
    const w = containerRef.current.getBoundingClientRect().width;
    if (import.meta.env.DEV) {
      console.log(
        `[DonorGraph] container=${Math.round(w)}px \u2192 ${w >= H_MIN_WIDTH && !isMulti ? "horizontal" : "vertical"}`,
      );
    }
    setCWidth(w);
    const observer = new ResizeObserver(([entry]) =>
      setCWidth(entry.contentRect.width),
    );
    observer.observe(containerRef.current);
    return () => observer.disconnect();
  }, [isMulti]);

  // Edge labels
  const edge1Label = isMulti
    ? `DONATED \u00b7 2019 \u00b7 ${candidates.length} CANDIDATES`
    : fmtDonation(visible[0]?.donated ?? 0);

  const edge2Label = isMulti
    ? [...new Set(visible.map((c) => c.position))]
        .slice(0, 2)
        .map(fmtPosition)
        .join(" / ")
    : fmtPosition(visible[0]?.position ?? "");

  // Accessibility
  const ariaLabel = useMemo(() => {
    const cands = visible.map((c) => {
      const amt =
        c.donated > 0 ? fmtCop(c.donated) : "an undisclosed amount";
      return `candidate ${c.name} (${c.party}), donating ${amt}, who ran for ${c.position}`;
    });
    const joined =
      cands.length === 1
        ? cands[0]
        : cands.slice(0, -1).join("; ") + "; and " + cands.at(-1);
    return `Supplier ${supplierName} donated to ${joined}, which controls contracting entity ${entityName}.`;
  }, [supplierName, entityName, visible]);

  // d3 arrows — re-draw when layout orientation or labels change
  const dir = isH ? ("horizontal" as const) : ("vertical" as const);
  useEffect(() => {
    const c1 = drawArrow(
      arrow1.current,
      250,
      dir,
      isH ? undefined : edge1Label,
    );
    const c2 = drawArrow(
      arrow2.current,
      500,
      dir,
      isH ? undefined : edge2Label,
    );
    return () => {
      c1?.();
      c2?.();
    };
  }, [edge1Label, edge2Label, dir, isH]);

  // Wrapper class: forces Tooltip's <span> to stretch full width
  const cell = isH
    ? "flex-1 min-w-0 flex flex-col [&>span]:flex [&>span]:w-full"
    : "[&>span]:flex [&>span]:w-full";

  // Shared metadata strip
  const metaStrip = (
    <div className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-0.5 mt-3 pt-3 border-t border-[#1a1a22]">
      <span className="font-mono text-[10px] text-[#3a3a42] uppercase tracking-wider py-0.5">
        TOTAL DONATED
      </span>
      <span className="font-mono text-[10px] text-[#888890] font-medium text-right py-0.5">
        {fmtCop(donor.total_donated)}
      </span>
      <span className="font-mono text-[10px] text-[#3a3a42] uppercase tracking-wider py-0.5">
        CANDIDATES
      </span>
      <span className="font-mono text-[10px] text-[#888890] font-medium text-right py-0.5">
        {donor.n_candidates}
      </span>
      {donor.parties && (
        <>
          <span className="font-mono text-[10px] text-[#3a3a42] uppercase tracking-wider py-0.5">
            PARTIES
          </span>
          <span className="font-mono text-[10px] text-[#888890] font-medium text-right py-0.5">
            {fmtParties(donor.parties)}
          </span>
        </>
      )}
    </div>
  );

  /* ── Horizontal layout ──────────────────────────────── */

  if (isH) {
    return (
      <div ref={containerRef} role="img" aria-label={ariaLabel}>
        {/* Edge labels row — mirrors graph row proportions */}
        <div className="flex items-end mb-1">
          <div className="flex-1 min-w-0" />
          <div
            style={{
              flex: `0 0 ${H_CONNECTOR}px`,
              overflow: "visible",
              textAlign: "center",
            }}
          >
            <EdgeLabel text={edge1Label} delay={250} />
          </div>
          <div className="flex-1 min-w-0" />
          <div
            style={{
              flex: `0 0 ${H_CONNECTOR}px`,
              overflow: "visible",
              textAlign: "center",
            }}
          >
            <EdgeLabel text={edge2Label} delay={500} />
          </div>
          <div className="flex-1 min-w-0" />
        </div>

        {/* Graph row — nodes + arrows */}
        <div className="flex items-center">
          <div className={cell}>
            <GNode
              role="SUPPLIER"
              name={supplierName}
              delay={0}
              clickable={!!supplierNit && !!onViewPortfolio}
              onClick={() => supplierNit && onViewPortfolio?.(supplierNit)}
              tooltipText={`NIT: ${supplierNit ?? "\u2014"} \u00b7 ${donor.donor_type}`}
            />
          </div>

          <div style={{ flex: `0 0 ${H_CONNECTOR}px` }}>
            <svg
              ref={arrow1}
              width={H_CONNECTOR}
              height="2"
              overflow="visible"
              style={{ display: "block" }}
            />
          </div>

          <div className={cell}>
            <GNode
              role="CANDIDATE"
              name={visible[0]?.name ?? ""}
              delay={150}
              tooltipText={[
                visible[0]?.party,
                visible[0]?.position,
                visible[0]?.donated
                  ? `Donated: ${fmtCop(visible[0].donated)}`
                  : null,
              ]
                .filter(Boolean)
                .join(" \u00b7 ")}
            />
          </div>

          <div style={{ flex: `0 0 ${H_CONNECTOR}px` }}>
            <svg
              ref={arrow2}
              width={H_CONNECTOR}
              height="2"
              overflow="visible"
              style={{ display: "block" }}
            />
          </div>

          <div className={cell}>
            <GNode
              role="ENTITY"
              name={entityName}
              delay={300}
              highlighted
              annotation={"\u25c0 this contract"}
              tooltipText={
                entityNit ? `Entity NIT: ${entityNit}` : undefined
              }
            />
          </div>
        </div>

        {metaStrip}
      </div>
    );
  }

  /* ── Vertical layout (multi-candidate or narrow) ────── */

  return (
    <div ref={containerRef} role="img" aria-label={ariaLabel} className="flex flex-col">
      <div className={cell}>
        <GNode
          role="SUPPLIER"
          name={supplierName}
          delay={0}
          clickable={!!supplierNit && !!onViewPortfolio}
          onClick={() => supplierNit && onViewPortfolio?.(supplierNit)}
          tooltipText={`NIT: ${supplierNit ?? "\u2014"} \u00b7 ${donor.donor_type}`}
        />
      </div>

      <svg
        ref={arrow1}
        className="w-full block"
        style={{ height: 44 }}
      />

      <div className="flex flex-col gap-1">
        {visible.map((c, i) => (
          <div key={i} className={cell}>
            <GNode
              role="CANDIDATE"
              name={c.name}
              delay={150}
              tooltipText={[
                c.party,
                c.position,
                c.donated > 0 ? `Donated: ${fmtCop(c.donated)}` : null,
              ]
                .filter(Boolean)
                .join(" \u00b7 ")}
            >
              {isMulti && (
                <div
                  className="font-mono text-[9px] mt-0.5"
                  style={{ color: "rgba(240,168,120,0.5)" }}
                >
                  {c.donated > 0 ? fmtCop(c.donated) : "Amt undisclosed"}{" "}
                  \u00b7 {c.position}
                </div>
              )}
            </GNode>
          </div>
        ))}
        {overflow > 0 && (
          <div
            className="font-mono text-[9px] px-2.5 py-1 uppercase tracking-wider"
            style={{ color: "rgba(240,168,120,0.45)" }}
          >
            +{overflow} more candidate{overflow > 1 ? "s" : ""}
          </div>
        )}
      </div>

      <svg
        ref={arrow2}
        className="w-full block"
        style={{ height: 44 }}
      />

      <div className={cell}>
        <GNode
          role="ENTITY"
          name={entityName}
          delay={300}
          highlighted
          annotation={"\u25c0 this contract"}
          tooltipText={entityNit ? `Entity NIT: ${entityNit}` : undefined}
        />
      </div>

      {metaStrip}
    </div>
  );
}
