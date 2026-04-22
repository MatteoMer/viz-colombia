import { useControl } from "react-map-gl/maplibre";
import { MapboxOverlay } from "@deck.gl/mapbox";
import type { MapboxOverlayProps } from "@deck.gl/mapbox";

export function DeckGLOverlay(props: MapboxOverlayProps) {
  const overlay = useControl(() => new MapboxOverlay({ interleaved: false }));
  overlay.setProps(props);
  return null;
}
