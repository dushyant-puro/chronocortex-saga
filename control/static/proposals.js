// Presentation/receipt recovery only. The server owns evidence and authority.
export function installProposalReview({ api, session, available, ready }) {
  const $ = (id) => document.getElementById(id);
  let owner = null, key = null, view = null, ids = null;
  let busy = false, attempted = false, admitted = false, blocked = false, generation = 0;
  const validId = (v) => typeof v === "string" && v.length > 0 && v.length <= 256;
  function identifiers(value) {
    if (!value || !validId(value.review_id) || !validId(value.task_id) || typeof value.digest !== "string" || !/^[0-9a-f]{64}$/.test(value.digest)) throw new Error("Invalid receipt identity.");
    return { review_id: value.review_id, task_id: value.task_id, digest: value.digest };
  }
  function checked(value, expected) {
    if (!value || typeof value.transcript !== "string" || typeof value.current !== "boolean" || typeof value.expired !== "boolean" || typeof value.admitted !== "boolean" || value.execution_authorized !== false) throw new Error("Review response unavailable.");
    const next = identifiers(value.binding);
    if (expected && JSON.stringify(next) !== JSON.stringify(expected)) throw new Error("Review identity changed.");
    if (!Array.isArray(value.binding.arguments) || !value.binding.source || !value.binding.reviewer) throw new Error("Review evidence unavailable.");
    return value;
  }
  function message(text, warning = false) {
    const node = $("proposal-message");
    node.setAttribute("role", warning ? "alert" : "status");
    node.className = warning ? "notice proposal-warning" : "notice";
    node.textContent = text;
  }
  function wipe() {
    view = ids = null; busy = attempted = admitted = blocked = false; generation += 1;
    $("proposal-panel").hidden = true;
    $("proposal-evidence").hidden = true;
    for (const id of ["proposal-transcript", "proposal-arguments", "proposal-metadata", "proposal-message"]) $(id).replaceChildren();
    $("proposal-task-id").value = "";
    $("proposal-confirmed").checked = false;
  }
  function render() {
    $("proposal-start").hidden = !available();
    $("proposal-start").disabled = busy || !ready() || attempted || blocked;
    $("proposal-task-id").value = ids?.task_id || "";
    $("proposal-evidence").hidden = !view;
    if (view) {
      $("proposal-transcript").textContent = view.transcript;
      $("proposal-arguments").textContent = JSON.stringify(view.binding.arguments, null, 2);
      $("proposal-metadata").replaceChildren();
      for (const [label, value] of [
        ["Task", view.binding.task_name], ["Workflow digest", view.binding.workflow_digest],
        ["Source adapter · identity unverified", view.binding.source.source_id],
        ["Turn / revision", `${view.binding.source.turn} / ${view.binding.source.revision}`],
        ["Review expires", new Date(view.binding.expires_ms).toLocaleString()],
        ["Review principal", view.binding.reviewer.principal_id], ["Binding digest", view.binding.digest],
      ]) {
        const term = document.createElement("dt"), detail = document.createElement("dd");
        term.textContent = label; detail.textContent = String(value);
        $("proposal-metadata").append(term, detail);
      }
    }
    $("proposal-confirmed").disabled = busy || attempted || admitted || blocked || !view;
    $("proposal-admit").disabled = !ready() || busy || attempted || admitted || blocked || !view?.current || view?.expired || !$("proposal-confirmed").checked;
    $("proposal-status").disabled = busy || !ids || !session();
    $("proposal-finish").disabled = busy || (attempted && !admitted);
  }
  function sync() {
    const current = session();
    if (current && owner !== current) {
      wipe(); owner = current; key = `ccs:admission:${encodeURIComponent(current.principal_id)}`;
      try {
        const stored = sessionStorage.getItem(key);
        if (stored) {
          attempted = true;
          ids = identifiers(JSON.parse(stored));
          $("proposal-panel").hidden = false;
          message("A receipt identity was retained in this tab. Read authoritative admission status before another admission.", true);
        }
      } catch {
        blocked = true;
        $("proposal-panel").hidden = false;
        message("Receipt storage is unavailable or invalid. Admission is paused; preserve any known task ID for journal inspection.", true);
      }
    }
    render();
    if (blocked && !view) $("proposal-start").disabled = true;
  }
  function remember() {
    const encoded = JSON.stringify(identifiers(ids));
    sessionStorage.setItem(key, encoded);
    if (sessionStorage.getItem(key) !== encoded) throw new Error("Receipt identity could not be retained.");
  }
  async function perform(kind) {
    const identity = session(), version = generation;
    if (!identity || busy || (kind !== "status" && !ready())) return;
    if (kind === "admit" && (attempted || admitted || blocked || !view?.current || view?.expired || !$("proposal-confirmed").checked)) return;
    if (kind === "review" && (attempted || blocked)) return;
    if (kind === "status" && !ids) return;
    busy = true; render();
    $("proposal-panel").hidden = false;
    try {
      if (kind === "admit") {
        try { remember(); } catch {
          blocked = true;
          message("Receipt storage could not be verified. No admission request was sent.", true);
          return;
        }
        attempted = true;
        message("Admission sent. Keep this task identity until its outcome is confirmed.", true);
      }
      const result = await api(`/api/proposals/${kind}`, kind === "review" ? {} : { review_id: ids.review_id, binding_digest: ids.digest });
      if (session() !== identity || generation !== version) return;
      if (kind === "admit") {
        if (result?.state !== "ADMITTED" || result.execution_authorized !== false || result.task_id !== ids.task_id || result.binding_digest !== ids.digest) throw new Error("Admission receipt unavailable.");
        admitted = true;
        message("Task admitted. Execution requires separate operation authority. You may close this review.");
      } else {
        view = checked(result, kind === "status" ? ids : null);
        ids = identifiers(view.binding);
        admitted = view.admitted;
        $("proposal-confirmed").checked = false;
        if (admitted) message("Journal confirms this task was admitted. Execution authority remains separate.");
        else if (attempted) message("Admission remains unconfirmed. Inspect this exact task in the journal; no retry was sent.", true);
        else if (!view.current || view.expired) message("This review is stale or expired. Request a current proposal review before admission.", true);
        else message("Inspect the transcript and exact arguments, then explicitly accept this task.");
        $("proposal-title").focus();
      }
    } catch (error) {
      if (session() !== identity || generation !== version) return;
      blocked = true;
      message(attempted ? "Admission outcome is unconfirmed. Keep this task ID and read admission status. No retry was sent." : `Review unavailable. ${error.network ? "No admission was sent." : error.message}`, true);
    } finally {
      if (session() === identity && generation === version) { busy = false; render(); }
    }
  }
  $("proposal-start").addEventListener("click", () => perform("review"));
  $("proposal-status").addEventListener("click", () => perform("status"));
  $("proposal-form").addEventListener("submit", (event) => { event.preventDefault(); perform("admit"); });
  $("proposal-confirmed").addEventListener("change", render);
  $("proposal-finish").addEventListener("click", () => {
    if (busy || (attempted && !admitted)) return;
    try { if (key) sessionStorage.removeItem(key); } catch { message("Receipt storage could not be cleared. Preserve the task ID.", true); return; }
    wipe(); render();
  });
  return { sync, reset() { wipe(); owner = key = null; $("proposal-start").hidden = true; } };
}
