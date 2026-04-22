import { useState, useRef, useCallback } from "react";
import { createPortal } from "react-dom";

export function Tooltip({
  text,
  children,
}: {
  text: string;
  children: React.ReactNode;
}) {
  const [show, setShow] = useState(false);
  const [pos, setPos] = useState({ top: 0, left: 0 });
  const ref = useRef<HTMLSpanElement>(null);

  const handleEnter = useCallback(() => {
    if (!ref.current) return;
    const rect = ref.current.getBoundingClientRect();
    setPos({ top: rect.top - 6, left: rect.left + rect.width / 2 });
    setShow(true);
  }, []);

  return (
    <span
      ref={ref}
      className="inline-flex"
      onMouseEnter={handleEnter}
      onMouseLeave={() => setShow(false)}
    >
      {children}
      {show &&
        createPortal(
          <span
            className="fixed px-2.5 py-1.5 bg-[#111116] border border-[#1a1a22] text-[#d0d0d8] font-mono text-[10px] leading-snug w-[220px] text-left shadow-lg pointer-events-none"
            style={{
              top: pos.top,
              left: pos.left,
              transform: "translate(-50%, -100%)",
              zIndex: 99999,
            }}
          >
            {text}
            <span className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-[#111116]" />
          </span>,
          document.body,
        )}
    </span>
  );
}
