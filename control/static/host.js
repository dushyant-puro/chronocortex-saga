/* A point-in-time recovery observation, never an execution/restart control. */
export function installHostStatus({ api, session, available }) {
  const panel = document.getElementById('host-status-panel');
  const button = document.getElementById('host-status-check');
  const message = document.getElementById('host-status-message');
  let owner = null, generation = 0, busy = false;
  function reset() {
    owner = null; generation += 1; busy = false;
    panel.hidden = true;
    message.textContent = 'No current host check. A successful check cannot authorize dispatch or establish provider recovery.';
  }
  function sync() {
    if (owner !== session()) { reset(); owner = session(); }
    panel.hidden = !session() || !available();
    button.disabled = busy || panel.hidden;
  }
  button.addEventListener('click', async () => {
    const identity = session(), version = generation;
    if (!identity || busy || !available()) return;
    busy = true; sync();
    message.textContent = 'Checking current local lifecycle, lease and worker authority…';
    try {
      const result = await api('/api/host-status');
      if (session() !== identity || generation !== version) return;
      if (!result || result.schema !== 1 || result.mode !== 'RECOVERY_ONLY' || result.ready !== true || result.state !== 'READY' ||
          result.provider !== 'NOT_CONFIGURED' || result.voice !== 'NOT_CONFIGURED' ||
          result.dispatch_authorized !== false || result.restore_authorized !== false ||
          result.provider_reconciled !== false || result.freshness_verified !== false) throw new Error('Invalid host observation');
      message.textContent = 'Recovery host passed the last check. No executors, provider or live voice are configured. This observation cannot authorize dispatch, verify snapshot freshness or prove provider reconciliation.';
    } catch {
      if (session() !== identity || generation !== version) return;
      message.textContent = 'Recovery host check unavailable. Preserve evidence and inspect the foreground host. No restart, retry or recovery activation was requested.';
    } finally {
      if (session() === identity && generation === version) { busy = false; sync(); }
    }
  });
  reset();
  return { reset, sync };
}
