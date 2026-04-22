import { useState, useRef, useCallback, useImperativeHandle, forwardRef, useMemo } from "react";
import MapGL, { type MapRef } from "react-map-gl/maplibre";
import { GeoJsonLayer, ScatterplotLayer } from "@deck.gl/layers";
import type { PickingInfo } from "@deck.gl/core";
import "maplibre-gl/dist/maplibre-gl.css";

import { DeckGLOverlay } from "./DeckGLOverlay";
import type { Dot, Category } from "../types";
import {
  flagRateToColor,
  scoreToRGBA,
  isTopTier,
  getDotCatScore,
  fmtCop,
  fmtNum,
  fmtPct,
  titleCase,
} from "../utils";

interface Props {
  geojson: GeoJSON.FeatureCollection;
  muniGeojson?: GeoJSON.FeatureCollection | null;
  dots: Dot[];
  category: Category;
  onDotClick: (dot: Dot) => void;
  onDotHover?: (dot: Dot | null) => void;
  onDeptClick?: (deptName: string) => void;
  highlightedDotId?: string | null;
  highlightedDept?: string | null;
  deptFilter?: string | null;
}

export interface MapViewHandle {
  flyTo: (lat: number, lon: number, zoom?: number) => void;
  panTo: (lat: number, lon: number) => void;
  fitDept: (deptName: string) => void;
  resetView: () => void;
}

const INITIAL_VIEW = {
  longitude: -73.5,
  latitude: 4.5,
  zoom: 6,
  pitch: 0,
  bearing: 0,
};

const MAP_STYLE = "https://basemaps.cartocdn.com/gl/dark-matter-nolabels-gl-style/style.json";

// Score range for dot sizing (max observed ~10.5)
const SCORE_MAX = 10;

export const MapView = forwardRef<MapViewHandle, Props>(function MapView(
  { geojson, muniGeojson, dots, category, onDotClick, onDotHover, onDeptClick, highlightedDotId, highlightedDept, deptFilter },
  ref,
) {
  const mapRef = useRef<MapRef>(null);
  const [viewState, setViewState] = useState(INITIAL_VIEW);

  // Build dept name → bbox lookup from geojson features
  const deptBounds = useMemo(() => {
    const bounds: Record<string, { minLon: number; maxLon: number; minLat: number; maxLat: number }> = {};
    for (const feat of geojson.features) {
      const name = feat.properties?.name;
      if (!name || !feat.geometry) continue;
      const coords: number[][] = [];
      const extractCoords = (c: any) => {
        if (typeof c[0] === "number") { coords.push(c); return; }
        for (const sub of c) extractCoords(sub);
      };
      extractCoords((feat.geometry as any).coordinates);
      if (coords.length === 0) continue;
      let minLon = Infinity, maxLon = -Infinity, minLat = Infinity, maxLat = -Infinity;
      for (const [lon, lat] of coords) {
        if (lon < minLon) minLon = lon;
        if (lon > maxLon) maxLon = lon;
        if (lat < minLat) minLat = lat;
        if (lat > maxLat) maxLat = lat;
      }
      bounds[name] = { minLon, maxLon, minLat, maxLat };
    }
    return bounds;
  }, [geojson]);

  useImperativeHandle(ref, () => ({
    flyTo(lat: number, lon: number, zoom = 14) {
      mapRef.current?.flyTo({ center: [lon, lat], zoom, duration: 600 });
    },
    panTo(lat: number, lon: number) {
      const map = mapRef.current;
      if (!map) return;
      const bounds = map.getBounds();
      if (bounds && !bounds.contains([lon, lat] as any)) {
        map.panTo([lon, lat], { duration: 400 });
      }
    },
    fitDept(deptName: string) {
      const map = mapRef.current;
      const bb = deptBounds[deptName];
      if (!map || !bb) return;
      map.fitBounds(
        [[bb.minLon, bb.minLat], [bb.maxLon, bb.maxLat]],
        { padding: 60, duration: 600 },
      );
    },
    resetView() {
      mapRef.current?.flyTo({
        center: [INITIAL_VIEW.longitude, INITIAL_VIEW.latitude],
        zoom: INITIAL_VIEW.zoom,
        pitch: INITIAL_VIEW.pitch,
        bearing: INITIAL_VIEW.bearing,
        duration: 600,
      });
    },
  }));

  const onViewStateChange = useCallback(({ viewState: vs }: { viewState: typeof INITIAL_VIEW }) => {
    setViewState(vs);
  }, []);

  // Active dots: exclude DQ, filter to dept if selected
  const activeDots = useMemo(() => {
    let filtered = dots.filter((d) => d.q !== 1);
    if (deptFilter) {
      filtered = filtered.filter((d) => d.d === deptFilter);
    }
    return filtered;
  }, [dots, deptFilter]);

  // Sort dots by score ascending so high-score renders on top
  const sortedDots = useMemo(
    () => [...activeDots].sort((a, b) => getDotCatScore(a, category) - getDotCatScore(b, category)),
    [activeDots, category],
  );

  // Municipality features filtered to selected department
  const filteredMuniGeo = useMemo(() => {
    if (!muniGeojson || !deptFilter) return null;
    return {
      type: "FeatureCollection" as const,
      features: muniGeojson.features.filter(
        (f) => f.properties?.dept === deptFilter,
      ),
    };
  }, [muniGeojson, deptFilter]);

  // ── Three-tier zoom logic ──
  // Tier 1: Departments (zoom 5-8), fades 8-9
  // Tier 2: Municipalities (zoom 9-11 when dept selected), fades 11-12
  // Tier 3: Individual contracts (zoom 12+)
  const z = viewState.zoom;
  const hasDeptFilter = !!deptFilter;

  // Department choropleth: full at low zoom, dims when muni/scatter visible
  const deptOpacity = hasDeptFilter
    ? 0.2
    : z < 8 ? 0.9 : z > 9 ? 0.15 : 0.9 - 0.75 * (z - 8);

  // Municipality choropleth: visible when dept selected
  const muniOpacity = hasDeptFilter
    ? (z < 11 ? 0.75 : z > 12 ? 0.15 : 0.75 - 0.6 * (z - 11))
    : 0;

  // Scatter: visible at zoom 12+ or when dept selected AND zoomed past muni tier
  const scatterOpacity = hasDeptFilter
    ? (z < 11 ? 0 : z > 12 ? 1 : z - 11)
    : (z < 11 ? 0 : z > 12 ? 1 : z - 11);

  // ── Layer 1: Department choropleth ──
  const choroplethLayer = new GeoJsonLayer({
    id: "choropleth",
    data: geojson as any,
    stroked: true,
    filled: true,
    getFillColor: (f: any) => {
      const name = f.properties?.name;
      const base = flagRateToColor(f.properties.flag_rate ?? 0);
      if (highlightedDept && name === highlightedDept) {
        return [base[0] + 40, base[1] + 40, base[2] + 50, 220] as [number, number, number, number];
      }
      return base;
    },
    getLineColor: (f: any) => {
      const name = f.properties?.name;
      if (deptFilter && name === deptFilter) return [255, 255, 255, 80];
      return [255, 255, 255, 12];
    },
    lineWidthMinPixels: 0.5,
    opacity: deptOpacity,
    pickable: true,
    autoHighlight: true,
    highlightColor: [255, 255, 255, 25],
    onClick: (info: PickingInfo) => {
      const name = info.object?.properties?.name;
      if (name && name !== deptFilter && onDeptClick) onDeptClick(name);
      return true;
    },
    updateTriggers: {
      getFillColor: [highlightedDept],
      getLineColor: [deptFilter],
    },
  });

  // ── Layer 2: Municipality choropleth (dept drilldown) ──
  const muniLayer = filteredMuniGeo && muniOpacity > 0
    ? new GeoJsonLayer({
        id: "municipalities",
        data: filteredMuniGeo as any,
        stroked: true,
        filled: true,
        getFillColor: (f: any) => flagRateToColor(f.properties.flag_rate ?? 0),
        getLineColor: [10, 10, 16, 180],
        lineWidthMinPixels: 1,
        opacity: muniOpacity,
        pickable: muniOpacity > 0.2,
        autoHighlight: true,
        highlightColor: [255, 255, 255, 30],
        onClick: (info: PickingInfo) => {
          const map = mapRef.current;
          if (!map) return true;
          const muniName = info.object?.properties?.muni;
          // Find the highest-score dot in this municipality and fly to it
          if (muniName && deptFilter) {
            const muniDots = activeDots
              .filter((d) => d.m === muniName || d.d === muniName)
              .sort((a, b) => b.c - a.c);
            if (muniDots.length > 0) {
              const top = muniDots[0];
              map.flyTo({ center: [top.o, top.a], zoom: 12, duration: 600 });
              return true;
            }
          }
          // Fallback to click location
          if (info.coordinate) {
            map.flyTo({ center: [info.coordinate[0], info.coordinate[1]], zoom: 12, duration: 600 });
          }
          return true;
        },
      })
    : null;

  // ── Layer 3: Individual contract scatter (zoom 12+) ──
  const scatterLayer = new ScatterplotLayer<Dot>({
    id: "scatter",
    data: sortedDots,
    getPosition: (d) => [d.o, d.a],
    getFillColor: (d) => scoreToRGBA(getDotCatScore(d, category)),
    getRadius: (d) => Math.max(3, Math.min(12, 3 + 9 * (Math.max(0, getDotCatScore(d, category)) / SCORE_MAX))),
    radiusUnits: "pixels",
    opacity: scatterOpacity,
    visible: scatterOpacity > 0,
    pickable: scatterOpacity > 0.3,
    autoHighlight: true,
    highlightColor: [255, 255, 255, 100],
    stroked: true,
    getLineColor: (d) => {
      const score = getDotCatScore(d, category);
      return isTopTier(score) ? [255, 255, 255, 200] : [0, 0, 0, 100];
    },
    getLineWidth: 1,
    lineWidthUnits: "pixels",
    onClick: (info: PickingInfo<Dot>) => {
      if (info.object) onDotClick(info.object);
    },
    onHover: (info: PickingInfo<Dot>) => {
      onDotHover?.(info.object ?? null);
    },
    updateTriggers: {
      getFillColor: [category],
      getLineColor: [category],
      getRadius: [category],
    },
  });

  // Highlight ring
  const highlightDot = highlightedDotId ? sortedDots.find((d) => d.i === highlightedDotId) : null;
  const highlightLayer = highlightDot
    ? new ScatterplotLayer<Dot>({
        id: "highlight-ring",
        data: [highlightDot],
        getPosition: (d) => [d.o, d.a],
        getRadius: 16,
        getFillColor: [255, 255, 255, 30],
        getLineColor: [255, 255, 255, 200],
        getLineWidth: 2,
        lineWidthUnits: "pixels",
        radiusUnits: "pixels",
        stroked: true,
        filled: true,
      })
    : null;

  const layers = [
    choroplethLayer,
    muniLayer,
    scatterLayer,
    highlightLayer,
  ].filter(Boolean);

  // Tooltip rendering
  const getTooltip = useCallback(
    (info: PickingInfo) => {
      if (!info.object) return null;

      if (info.layer?.id === "choropleth") {
        const p = info.object.properties;
        if (!p?.name || !p.n_contracts) return null;
        return {
          html: `<div style="font-weight:600;color:#e8e8f0">${titleCase(p.name)}</div>
            <div style="color:#7a7a90;font-size:11px">${fmtNum(p.n_contracts)} contracts &middot; ${p.n_flagged} flagged</div>
            ${p.n_flagged > 0 ? `<div style="color:#7a7a90;font-size:11px">Flag rate: ${fmtPct(p.flag_rate)}</div>` : ""}
            <div style="color:#7a7a90;font-size:11px">Exposure: ${fmtCop(p.exposure || 0)}</div>
            <div style="color:#7a7a90;font-size:10px;margin-top:2px;opacity:0.6">Click to drill down</div>`,
          className: "deck-tooltip",
        };
      }

      if (info.layer?.id === "municipalities") {
        const p = info.object.properties;
        if (!p?.muni) return null;
        return {
          html: `<div style="font-weight:600;color:#e8e8f0">${titleCase(p.muni)}</div>
            <div style="color:#7a7a90;font-size:11px">${fmtNum(p.n || 0)} contracts${p.n_flagged ? ` &middot; ${p.n_flagged} flagged` : ""}</div>
            ${p.flag_rate > 0 ? `<div style="color:#7a7a90;font-size:11px">Flag rate: ${fmtPct(p.flag_rate)}</div>` : ""}
            <div style="color:#7a7a90;font-size:10px;margin-top:2px;opacity:0.6">Click to zoom in</div>`,
          className: "deck-tooltip",
        };
      }

      if (info.layer?.id === "scatter") {
        const d = info.object as Dot;
        const score = getDotCatScore(d, category);
        return {
          html: `<div style="font-weight:600;color:#e8e8f0">${d.e || d.i}</div>
            <div style="color:#7a7a90;font-size:11px">${titleCase(d.m)} &middot; ${fmtCop(d.v)}</div>
            <div style="color:#e8e8f0;font-size:11px;margin-top:3px">Score: <strong>${score.toFixed(2)}</strong></div>
            <div style="color:#7a7a90;font-size:10px;display:grid;grid-template-columns:auto auto;gap:0 8px;margin-top:2px">
              <span>Execution</span><span style="text-align:right">${d.cx.toFixed(1)}</span>
              <span>Competition</span><span style="text-align:right">${d.cc.toFixed(1)}</span>
              <span>Pricing</span><span style="text-align:right">${d.cp.toFixed(1)}</span>
              <span>Relationships</span><span style="text-align:right">${d.cr.toFixed(1)}</span>
            </div>
            <div style="color:#7a7a90;font-size:10px;margin-top:3px;opacity:0.6">Click for details</div>`,
          className: "deck-tooltip",
        };
      }

      return null;
    },
    [category],
  );

  return (
    <MapGL
      ref={mapRef}
      {...viewState}
      onMove={(evt) => onViewStateChange({ viewState: evt.viewState as typeof INITIAL_VIEW })}
      mapStyle={MAP_STYLE}
      style={{ width: "100%", height: "100%" }}
      minZoom={3}
      attributionControl={false}
    >
      <DeckGLOverlay layers={layers} getTooltip={getTooltip} />
    </MapGL>
  );
});
