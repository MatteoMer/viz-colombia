import { useRef, useEffect, useMemo } from "react";
import { scaleLinear, scaleLog } from "d3-scale";
import { brushX } from "d3-brush";
import { select } from "d3-selection";
import type { Dot } from "../types";

interface Props {
  dots: Dot[];
  scoreRange: [number, number];
  onBrush: (range: [number, number]) => void;
}

const W = 240;
const H = 44;
const MARGIN = { top: 2, right: 4, bottom: 14, left: 4 };
const INNER_W = W - MARGIN.left - MARGIN.right;
const INNER_H = H - MARGIN.top - MARGIN.bottom;
const N_BINS = 30;

export function ScoreHistogram({ dots, scoreRange, onBrush }: Props) {
  const svgRef = useRef<SVGSVGElement>(null);
  const brushRef = useRef<ReturnType<typeof brushX> | null>(null);
  const brushingRef = useRef(false);

  const { bins, maxCount, xScale } = useMemo(() => {
    const maxScore = 5;
    const xScale = scaleLinear().domain([0, maxScore]).range([0, INNER_W]);
    const binWidth = maxScore / N_BINS;
    const bins = Array.from({ length: N_BINS }, (_, i) => ({
      x0: i * binWidth,
      x1: (i + 1) * binWidth,
      count: 0,
    }));
    dots.forEach((d) => {
      if (d.q === 1) return;
      const idx = Math.min(N_BINS - 1, Math.max(0, Math.floor(d.c / binWidth)));
      bins[idx].count++;
    });
    const maxCount = Math.max(1, ...bins.map((b) => b.count));
    return { bins, maxCount, xScale };
  }, [dots]);

  const yScale = useMemo(
    () => scaleLog().domain([1, maxCount]).range([INNER_H, 0]).clamp(true),
    [maxCount],
  );

  useEffect(() => {
    const svg = svgRef.current;
    if (!svg) return;

    const brushGroup = select(svg).select<SVGGElement>(".brush-group");
    const brush = brushX<unknown>()
      .extent([
        [0, 0],
        [INNER_W, INNER_H],
      ])
      .on("brush", (event) => {
        if (!event.sourceEvent) return;
        brushingRef.current = true;
        const [x0, x1] = event.selection as [number, number];
        onBrush([xScale.invert(x0), xScale.invert(x1)]);
      })
      .on("end", (event) => {
        brushingRef.current = false;
        if (!event.selection) {
          onBrush([0, 5]);
        }
      });

    brushRef.current = brush;
    brushGroup.call(brush as any);

    if (scoreRange[0] > 0 || scoreRange[1] < 5) {
      brushGroup.call(
        brush.move as any,
        [xScale(scoreRange[0]), xScale(scoreRange[1])],
      );
    }

    return () => {
      brushGroup.on(".brush", null);
    };
  }, [xScale]);

  const ticks = [0, 1, 2, 3, 4, 5];

  return (
    <div className="panel px-3 py-2 w-[260px] score-histogram">
      <div className="section-label mb-1">
        SCORE DISTRIBUTION
      </div>
      <svg ref={svgRef} width={W} height={H} className="block">
        <g transform={`translate(${MARGIN.left},${MARGIN.top})`}>
          {bins.map((bin, i) => {
            const barH = bin.count > 0 ? INNER_H - yScale(bin.count) : 0;
            const inRange = bin.x0 >= scoreRange[0] && bin.x1 <= scoreRange[1];
            return (
              <rect
                key={i}
                x={xScale(bin.x0)}
                y={INNER_H - barH}
                width={Math.max(0, xScale(bin.x1) - xScale(bin.x0) - 1)}
                height={barH}
                fill={inRange ? "#6fd4f5" : "#1a1a22"}
                opacity={inRange ? 0.5 : 0.4}
              />
            );
          })}

          <g className="brush-group" />

          {ticks.map((t) => (
            <text
              key={t}
              x={xScale(t)}
              y={INNER_H + 10}
              textAnchor="middle"
              fill="#3a3a42"
              fontSize={8}
              fontFamily="IBM Plex Mono"
            >
              {t}
            </text>
          ))}
        </g>
      </svg>
    </div>
  );
}
