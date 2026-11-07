// Read-only admitted history. Never modifies proposal receipt caches or authority.
export function installAdmissionInbox({ api, session }) {
  const $ = (id) => document.getElementById(id);
  let owner = null, generation = 0, busy = false, next = null;
  const id = (v) => typeof v === "string" && v.length > 0 && v.length <= 256;
  const cursor = (v) => typeof v === "string" && /^[1-9][0-9]{0,18}$/.test(v) && BigInt(v) <= 9223372036854775807n;
  function receipt(value) {
    if (!value || !id(value.task_id) || !id(value.review_id) || typeof value.binding_digest !== "string" || !/^[0-9a-f]{64}$/.test(value.binding_digest) || value.state !== "ADMITTED" || value.execution_authorized !== false) throw new Error("Receipt unavailable");
    return { task_id: value.task_id, review_id: value.review_id, binding_digest: value.binding_digest };
  }
  function message(text, warning = false) {
    $("inbox-message").textContent = text;
    $("inbox-message").setAttribute("role", warning ? "alert" : "status");
    $("inbox-message").className = warning ? "notice proposal-warning" : "notice";
  }
  function render() {
    $("admission-inbox").hidden = !session();
    $("inbox-load").disabled = busy || !session();
    $("inbox-older").disabled = busy || !next || !session();
    for (const button of $("inbox-items").querySelectorAll("button")) button.disabled = busy || !session();
  }
  function reset() {
    generation += 1; owner = null; busy = false; next = null;
    $("inbox-items").replaceChildren();
    message("Load a protected journal page to discover admitted tasks.");
    $("admission-inbox").hidden = true;
  }
  function sync() {
    if (owner !== session()) { reset(); owner = session(); }
    render();
  }
  async function read(before = null, expected = null) {
    const identity = session(), version = generation;
    if (!identity || busy) return;
    busy = true; render();
    message(expected ? "Reading this exact admission receipt…" : "Reading protected admission history…");
    try {
      const value = await api(`/api/proposals/${expected ? "receipt" : "inbox"}`, expected || { before });
      if (session() !== identity || generation !== version) return;
      if (expected) {
        const actual = receipt(value);
        if (JSON.stringify(actual) !== JSON.stringify(expected)) throw new Error("Receipt changed");
        message(`Journal confirms admission for ${actual.task_id}. Execution and external effects require separate evidence.`);
      } else {
        if (!value || value.scope !== "ADMITTED_HISTORY_ONLY" || value.execution_authorized !== false || value.scan_limit !== 200 || !Array.isArray(value.items) || value.items.length > 200 || (value.next_before !== null && (!cursor(value.next_before) || (before !== null && BigInt(value.next_before) >= BigInt(before))))) throw new Error("Inbox unavailable");
        const seen = new Set();
        const rows = value.items.map((item) => {
          const ids = receipt(item);
          if (!id(item.name) || seen.has(ids.task_id)) throw new Error("Inbox identity unavailable");
          seen.add(ids.task_id);
          const row = document.createElement("li"), name = document.createElement("strong"), task = document.createElement("code"), button = document.createElement("button");
          name.textContent = item.name; task.textContent = ids.task_id;
          button.type = "button"; button.className = "subtle"; button.textContent = "Confirm receipt";
          button.addEventListener("click", () => read(null, ids));
          row.append(name, task, button);
          return row;
        });
        $("inbox-items").replaceChildren(...rows); next = value.next_before;
        message(rows.length ? "Loaded admitted history. Confirm a receipt to recheck current authority. Execution remains separate." : (next ? "No visible admissions in this scanned page. Older records may contain matches." : "No visible admissions in this scanned page. This does not settle an uncertain admission request."));
      }
    } catch {
      if (session() !== identity || generation !== version) return;
      next = null; $("inbox-items").replaceChildren();
      message("Admission history or receipt is unavailable. Preserve known task identities; no admission was replayed. Load again for a fresh read.", true);
    } finally {
      if (session() === identity && generation === version) { busy = false; render(); }
    }
  }
  $("inbox-load").addEventListener("click", () => read());
  $("inbox-older").addEventListener("click", () => { if (next) read(next); });
  return { sync, reset };
}
