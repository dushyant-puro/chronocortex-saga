// Durable evidence inspection only. This module has no admission callback.
export function installReviewDeliveries({ api, session, canReauthorize, requestReauthorization }) {
  const $ = (id) => document.getElementById(id);
  const id = (v) => typeof v === 'string' && v.length > 0 && v.length <= 256;
  let owner = null, generation = 0, busy = false;
  let selected = null;
  function message(text, warning = false) {
    $('delivery-message').textContent = text;
    $('delivery-message').setAttribute('role', warning ? 'alert' : 'status');
    $('delivery-message').className = warning ? 'notice proposal-warning' : 'notice';
  }
  function clear() {
    selected = null;
    $('delivery-items').replaceChildren();
    $('delivery-detail').hidden = true;
    $('delivery-transcript').textContent = $('delivery-arguments').textContent = $('delivery-identity').textContent = '';
  }
  function render() {
    $('review-deliveries').hidden = !session();
    $('delivery-load').disabled = busy || !session();
    for (const button of $('delivery-items').querySelectorAll('button')) button.disabled = busy || !session();
    $('delivery-reauthorize').hidden = !selected;
    $('delivery-reauthorize').disabled = busy || !selected || !canReauthorize(selected);
  }
  function reset() {
    owner = null; generation += 1; busy = false; clear();
    message('Durable review retention is opt-in. Load to inspect retained evidence.');
    $('review-deliveries').hidden = true;
  }
  function sync() {
    if (owner !== session()) { reset(); owner = session(); }
    render();
  }
  async function read(reviewId = null) {
    const identity = session(), version = generation;
    if (!identity || busy) return;
    busy = true; render();
    $('delivery-detail').hidden = true;
    selected = null;
    message('Reading protected review evidence…');
    try {
      const value = await api('/api/proposals/deliveries', { review_id: reviewId });
      if (session() !== identity || generation !== version) return;
      if (!value || value.scope !== 'INSPECTION_ONLY' || value.admission_authorized !== false || value.execution_authorized !== false) throw new Error('Delivery authority changed');
      if (reviewId === null) {
        if (!Array.isArray(value.items) || value.items.length > 128 || value.capacity !== 128) throw new Error('Delivery capacity changed');
        const seen = new Set();
        const rows = value.items.map((item) => {
          if (!id(item.review_id) || !id(item.task_id) || !id(item.name) || !Number.isSafeInteger(item.expires_ms) || seen.has(item.review_id)) throw new Error('Invalid delivery identity');
          seen.add(item.review_id);
          const row = document.createElement('li'), label = document.createElement('strong'), task = document.createElement('code'), button = document.createElement('button');
          label.textContent = item.name; task.textContent = item.task_id;
          button.type = 'button'; button.className = 'subtle'; button.textContent = 'Inspect retained review';
          button.addEventListener('click', () => read(item.review_id));
          row.append(label, task, button); return row;
        });
        $('delivery-items').replaceChildren(...rows);
        message(rows.length ? 'Retained unadmitted reviews. These are historical evidence; current voice authority has not been established.' : 'No visible retained unadmitted reviews. This does not settle an uncertain admission.');
      } else {
        const b = value.binding;
        if (!b || b.review_id !== reviewId || !id(b.task_id) || typeof b.digest !== 'string' || !/^[0-9a-f]{64}$/.test(b.digest) || !Array.isArray(b.arguments) || typeof value.transcript !== 'string' || typeof value.admitted !== 'boolean' || value.current_evidence_verified !== false) throw new Error('Invalid retained review');
        $('delivery-identity').textContent = `${b.task_id} · ${b.digest}`;
        $('delivery-transcript').textContent = value.transcript;
        $('delivery-arguments').textContent = JSON.stringify(b.arguments, null, 2);
        $('delivery-detail').hidden = false;
        selected = value.admitted ? null : { review_id: b.review_id, task_id: b.task_id, binding_digest: b.digest };
        message(value.admitted ? 'This exact task has admission evidence. Inspect its durable receipt separately; no execution authority was granted.' : 'Inspection only. Stored evidence cannot establish the current voice proposal. This view cannot admit or replay a task.');
      }
    } catch {
      if (session() !== identity || generation !== version) return;
      clear(); message('Retained evidence is unavailable, expired or changed. Preserve known task identities; no admission was sent.', true);
    } finally {
      if (session() === identity && generation === version) { busy = false; render(); }
    }
  }
  $('delivery-load').addEventListener('click', () => read());
  $('delivery-reauthorize').addEventListener('click', () => {
    if (!busy && selected && canReauthorize(selected)) requestReauthorization(selected);
  });
  return { sync, reset };
}
