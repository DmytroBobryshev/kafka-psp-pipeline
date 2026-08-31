import { useCallback, useRef, useState } from "react";

/**
 * Clipboard with a visible outcome. navigator.clipboard exists only in secure contexts and can
 * still reject (permissions, focus) - the old silent call was why "copy id doesn't work": it
 * either threw away the rejection or the API wasn't there at all. This falls back to the
 * execCommand textarea trick and, either way, tells the user what happened.
 */
async function copyText(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    /* fall through to the legacy path */
  }
  try {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return ok;
  } catch {
    return false;
  }
}

export function useCopy() {
  const [copiedKey, setCopiedKey] = useState<string | null>(null);
  const timer = useRef<number | undefined>(undefined);

  const copy = useCallback(async (text: string, key: string = text) => {
    const ok = await copyText(text);
    window.clearTimeout(timer.current);
    setCopiedKey(ok ? key : null);
    timer.current = window.setTimeout(() => setCopiedKey(null), 1500);
    return ok;
  }, []);

  return { copy, copiedKey };
}
