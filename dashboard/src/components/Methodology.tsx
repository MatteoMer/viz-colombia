import { useMemo } from "react";
import { marked } from "marked";

export function Methodology({ content }: { content: string }) {
  const html = useMemo(() => marked.parse(content) as string, [content]);

  return (
    <div
      className="bg-[#0a0a0f] border border-[#1a1a22] p-8 max-w-[860px] flex-1 overflow-y-auto prose prose-invert prose-sm
        prose-headings:text-[#d0d0d8] prose-headings:font-display
        prose-h1:text-[16px] prose-h1:font-bold prose-h1:mb-2 prose-h1:uppercase prose-h1:tracking-wide
        prose-h2:text-[13px] prose-h2:font-bold prose-h2:mt-8 prose-h2:mb-3 prose-h2:pb-1.5 prose-h2:border-b prose-h2:border-[#1a1a22] prose-h2:uppercase prose-h2:tracking-wide
        prose-h3:text-[12px] prose-h3:font-bold prose-h3:mt-5 prose-h3:mb-2 prose-h3:uppercase prose-h3:tracking-wide
        prose-p:text-[#555560] prose-p:leading-relaxed prose-p:text-[12px] prose-p:font-sans
        prose-li:text-[#555560] prose-li:text-[12px] prose-li:font-sans
        prose-code:text-[#6fd4f5] prose-code:bg-[#111116] prose-code:px-1 prose-code:py-0.5 prose-code:text-[10px] prose-code:font-mono prose-code:before:content-none prose-code:after:content-none
        prose-pre:bg-[#0a0a0f] prose-pre:text-[#888890] prose-pre:border prose-pre:border-[#1a1a22] prose-pre:text-[10px] prose-pre:font-mono
        prose-table:text-[10px] prose-table:font-mono
        prose-th:bg-[#111116] prose-th:px-3 prose-th:py-1.5 prose-th:font-medium prose-th:text-[#888890] prose-th:text-[9px] prose-th:uppercase prose-th:tracking-[0.08em]
        prose-td:px-3 prose-td:py-1.5 prose-td:text-[#555560] prose-td:border-b prose-td:border-[#1a1a22]
        prose-strong:text-[#888890]
        prose-hr:border-[#1a1a22]
      "
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
}
