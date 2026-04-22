import { useState, useEffect, useRef, useCallback } from "react";
import { AnimatePresence, motion } from "framer-motion";
import type { DashboardData, View, Category, Contract, Dot } from "./types";
import type { MapViewHandle } from "./components/MapView";
import { Sidebar } from "./components/Sidebar";
import { KPICards } from "./components/KPICards";
import { CategoryTabs } from "./components/CategoryTabs";
import { MapView } from "./components/MapView";
import { SidePanel } from "./components/SidePanel";
import { ContractsView } from "./components/ContractsView";
import { ContractorsView } from "./components/ContractorsView";
import { DetailPanel } from "./components/DetailPanel";
import { Methodology } from "./components/Methodology";
import { flagRateToColor } from "./utils";

const LEGEND_STOPS = [0, 0.02, 0.05, 0.08, 0.12, 0.16, 0.20, 0.25];

export default function App() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [view, setView] = useState<View>("overview");
  const [category, setCategory] = useState<Category>("all");
  const [selected, setSelected] = useState<Contract | null>(null);
  const [supplierFilter, setSupplierFilter] = useState<string | null>(null);
  const [highlightedId, setHighlightedId] = useState<string | null>(null);
  const [highlightedDept, setHighlightedDept] = useState<string | null>(null);
  const [deptFilter, setDeptFilter] = useState<string | null>(null);
  const mapRef = useRef<MapViewHandle>(null);

  const [details, setDetails] = useState<Record<string, Contract> | null>(null);
  const [muniGeo, setMuniGeo] = useState<GeoJSON.FeatureCollection | null>(null);

  useEffect(() => {
    fetch("/data.json")
      .then((r) => r.json())
      .then(setData);
    fetch("/details.json")
      .then((r) => r.json())
      .then(setDetails);
    fetch("/municipalities.geojson")
      .then((r) => r.json())
      .then(setMuniGeo)
      .catch(() => {});
  }, []);


  const handleDeptClick = useCallback(
    (deptName: string) => {
      setDeptFilter(deptName);
      if (mapRef.current) {
        mapRef.current.fitDept(deptName);
      }
    },
    [],
  );

  const handleDotClick = useCallback(
    (dot: Dot) => {
      const detail = details?.[dot.i];
      if (detail) {
        setSelected(detail);
        setHighlightedId(dot.i);
      } else {
        setHighlightedId(dot.i);
      }
    },
    [details],
  );

  const handleListDotClick = useCallback(
    (dot: Dot) => {
      setHighlightedId(dot.i);
      if (mapRef.current) {
        mapRef.current.flyTo(dot.a, dot.o, 14);
      }
      const detail = details?.[dot.i];
      if (detail) {
        setSelected(detail);
      }
    },
    [details],
  );

  function handleContractSelect(c: Contract) {
    setSelected(c);
    setHighlightedId(c.id);
    if (c.lat != null && c.lon != null && mapRef.current) {
      mapRef.current.flyTo(c.lat, c.lon, 14);
    }
  }

  const handleDotHover = useCallback((dot: Dot | null) => {
    setHighlightedId(dot?.i ?? null);
    setHighlightedDept(dot?.d ?? null);
    if (dot && mapRef.current) {
      mapRef.current.panTo(dot.a, dot.o);
    }
  }, []);

  if (!data) {
    return (
      <div className="flex h-screen font-sans">
        <aside className="w-[200px] min-w-[200px] bg-[#0a0a0f] flex flex-col border-r border-[#1a1a22]">
          <div className="px-4 pt-5 pb-4 border-b border-[#1a1a22]">
            <div className="skeleton h-4 w-28 mb-2" />
            <div className="skeleton h-2.5 w-20" />
          </div>
          <div className="py-3 space-y-1 px-4">
            {[1, 2, 3, 4].map((i) => (
              <div key={i} className="skeleton h-6 w-full" />
            ))}
          </div>
        </aside>
        <main className="flex-1 bg-[#08080c] flex flex-col">
          <div className="flex gap-2 p-4">
            {[1, 2, 3].map((i) => (
              <div key={i} className="flex-1 skeleton h-20" />
            ))}
          </div>
          <div className="flex-1 m-4 mt-0 skeleton" />
        </main>
      </div>
    );
  }

  return (
    <div className="flex h-screen font-sans bg-bg">
      <Sidebar view={view} onViewChange={(v) => {
        setView(v);
        if (v === "overview") {
          setCategory("all");
          setSelected(null);
          setSupplierFilter(null);
          setHighlightedId(null);
          setHighlightedDept(null);
          setDeptFilter(null);
          mapRef.current?.resetView();
        }
      }} kpi={data.kpi} nContractors={data.contractors.length} />

      <main className="flex-1 overflow-hidden bg-bg flex flex-col">
        {view === "overview" ? (
          <div className="relative flex-1 min-h-0">
            <MapView
              ref={mapRef}
              geojson={data.geojson}
              muniGeojson={muniGeo}
              dots={data.dots}
              category={category}
              onDotClick={handleDotClick}
              onDeptClick={handleDeptClick}
              highlightedDotId={highlightedId}
              highlightedDept={highlightedDept}
              deptFilter={deptFilter}
            />

            {!deptFilter && (
              <>
                <div className="absolute top-3 left-4 right-[400px] z-10 pointer-events-none">
                  <div className="pointer-events-auto overflow-visible">
                    <KPICards kpi={data.kpi} dots={data.dots} />
                  </div>
                </div>
              </>
            )}

            <SidePanel
              dots={data.dots}
              departments={data.departments}
              category={category}
              deptFilter={deptFilter}
              onClearDeptFilter={() => setDeptFilter(null)}
              highlightedId={highlightedId}
              onDotHover={handleDotHover}
              onDotClick={handleListDotClick}
            />

            {/* Legend */}
            <div className="absolute bottom-4 left-14 z-10 pointer-events-none">
              <div className="pointer-events-auto panel px-3 py-1.5">
                <div className="flex items-center gap-2 font-mono text-[9px] text-[#555560] uppercase tracking-wider">
                  <span>FLAG RATE</span>
                  <div className="flex h-[3px] w-[120px]">
                    {LEGEND_STOPS.map((stop) => {
                      const [r, g, b] = flagRateToColor(stop);
                      return (
                        <span
                          key={stop}
                          className="flex-1"
                          style={{ background: `rgb(${r},${g},${b})` }}
                        />
                      );
                    })}
                  </div>
                  <span className="text-[8px]">LOW &rarr; HIGH</span>
                </div>
              </div>
            </div>
          </div>
        ) : (
          <div className="flex-1 overflow-y-auto">
            <div className="flex flex-col gap-4 p-5 h-full">
              <KPICards kpi={data.kpi} dots={data.dots} />

              <AnimatePresence mode="wait">
                {view === "contracts" && (
                  <motion.div
                    key="contracts"
                    className="flex flex-col gap-4 flex-1 min-h-0"
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    transition={{ duration: 0.15 }}
                  >
                    <ContractsView
                      contracts={data.contracts}
                      supplierFilter={supplierFilter}
                      onClearSupplierFilter={() => setSupplierFilter(null)}
                      onSelect={handleContractSelect}
                      highlightedId={highlightedId}
                    />
                  </motion.div>
                )}

                {view === "contractors" && (
                  <motion.div
                    key="contractors"
                    className="flex flex-col gap-4 flex-1 min-h-0"
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    transition={{ duration: 0.15 }}
                  >
                    <ContractorsView contractors={data.contractors} />
                  </motion.div>
                )}

                {view === "methodology" && (
                  <motion.div
                    key="methodology"
                    className="flex-1 min-h-0"
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    transition={{ duration: 0.15 }}
                  >
                    <Methodology content={data.methodology} />
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          </div>
        )}
      </main>

      <AnimatePresence>
        {selected && (
          <DetailPanel
            contract={selected}
            onClose={() => { setSelected(null); setHighlightedId(null); }}
            onViewPortfolio={(nit) => {
              setSupplierFilter(nit);
              setView("contracts");
              setSelected(null);
              setHighlightedId(null);
            }}
          />
        )}
      </AnimatePresence>
    </div>
  );
}
