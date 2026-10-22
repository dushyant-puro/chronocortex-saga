const $ = (selector) => document.querySelector(selector);
const state = {
  session: null,
  snapshot: null,
  view: "overview",
  fresh: false,
  busy: false,
  detail: null,
  decision: null,
  polling: null,
  failures: 0,
};
const titles = {
  overview: ["Operator overview", "Decisions grounded in durable evidence."],
  tasks: [
    "Durable tasks",
    "Checkpoint intent and remote effect evidence remain distinct.",
  ],
  actions: [
    "Action evidence",
    "Inspect dispatch, uncertainty, and compensation history.",
  ],
  approvals: [
    "Operation approvals",
    "Exact scope. Named principals. Bounded, single-use authority.",
  ],
  decisions: [
    "Decision history",
    "Your durable control requests. Recording is not provider execution.",
  ],
};

function element(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (className) node.className = className;
  return node;
}

function badge(value) {
  const style = /UNKNOWN|STALE|IN_FLIGHT|REQUESTED|NEEDS|EXPIRED/.test(value)
    ? "warn"
    : /COMMITTED|COMPLETED|COMPENSATED/.test(value)
      ? "good"
      : /REVOKED|FAILED|INVALIDATED/.test(value)
        ? "bad"
        : /AVAILABLE|PENDING|RUNNING|RECORDED/.test(value)
          ? "violet"
          : "";
  return element("span", value.replaceAll("_", " "), `badge ${style}`);
}

function time(value) {
  if (!value) return "Not recorded";
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? "Unknown time"
    : date.toLocaleString(undefined, {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      });
}

function notice(message, error = false) {
  const node = $("#notice");
  node.textContent = message;
  node.className = error ? "notice error-notice" : "notice";
  node.hidden = false;
}

async function api(path, body) {
  let response;
  try {
    response = await fetch(path, {
      method: body === undefined ? "GET" : "POST",
      credentials: "same-origin",
      cache: "no-store",
      headers:
        body === undefined
          ? {}
          : {
              "Content-Type": "application/json",
              "X-CCS-CSRF": state.session?.csrf_token || "",
            },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(10000),
    });
  } catch {
    const error = new Error(
      body === undefined
        ? "The journal could not be read."
        : "The decision response is unavailable. Read decision history before another request. No automatic retry was sent.",
    );
    error.network = true;
    throw error;
  }
  const data = response.status === 204 ? null : await response.json();
  if (!response.ok) {
    const error = new Error(
      data?.error?.message || "The request could not be completed.",
    );
    error.status = response.status;
    error.code = data?.error?.code;
    if (response.status === 401 && path !== "/api/login") disconnected();
    throw error;
  }
  return data;
}

function freshness(current) {
  state.fresh = current;
  $("#stale-banner").hidden = current;
  $("#sync-state").classList.toggle("stale", !current);
  $("#sync-state").textContent = current
    ? "Journal read current"
    : "Stale · controls paused";
  document.querySelectorAll("[data-control]").forEach((button) => {
    button.disabled =
      !current ||
      state.busy ||
      (button.closest("#detail") !== null && state.detail?.stale);
  });
}

function disconnected() {
  clearTimeout(state.polling);
  $("#decision-dialog").close();
  state.session = null;
  state.snapshot = null;
  state.fresh = false;
  state.detail = null;
  state.decision = null;
  state.inspection = (state.inspection || 0) + 1;
  $("#record-content").replaceChildren();
  $("#detail").replaceChildren();
  $("#detail").hidden = true;
  $("#content-grid").classList.remove("with-detail");
  $("#metrics").replaceChildren();
  $("#principal").textContent = "";
  $("#session-expiry").textContent = "";
  $("#read-time").textContent = "No completed read yet.";
  $("#notice").textContent = "";
  $("#notice").hidden = true;
  $("#lookup-id").value = "";
  $("#decision-evidence").textContent = "";
  $("#executor").value = "";
  $("#console").hidden = true;
  $("#connect").hidden = false;
}

function connected(identity) {
  state.session = identity;
  $("#connect").hidden = true;
  $("#console").hidden = false;
  $("#principal").textContent = identity.principal_id;
  $("#session-expiry").textContent =
    `Session ends ${time(identity.expires_ms)}`;
}

async function refresh() {
  if (!state.session || state.reading) return;
  state.reading = true;
  clearTimeout(state.polling);
  $("#refresh").disabled = true;
  const identity = state.session;
  try {
    const snapshot = await api("/api/snapshot");
    if (state.session !== identity) return;
    if (state.detail) {
      const { kind, record } = state.detail;
      const latest = snapshot[kind].find((row) => row.id === record.id);
      const previousTask = state.snapshot?.tasks.find(
        (row) => row.id === record.task_id,
      );
      const latestTask = snapshot.tasks.find(
        (row) => row.id === record.task_id,
      );
      if (
        (latest && latest.version !== record.version) ||
        previousTask?.version !== latestTask?.version
      ) {
        state.detail.stale = true;
        if (!state.busy) {
          $("#decision-dialog").close();
          state.decision = null;
        }
        notice(
          "Selected evidence changed. Refresh the evidence panel before deciding.",
          true,
        );
      }
    }
    state.snapshot = snapshot;
    state.failures = 0;
    render();
    freshness(true);
    $("#read-time").textContent =
      `Server read · ${time(state.snapshot.server_time_ms)}`;
  } catch (error) {
    state.failures += 1;
    freshness(false);
    if (state.session) notice(error.message, true);
  } finally {
    state.reading = false;
    $("#refresh").disabled = false;
    if (state.session && !document.hidden)
      state.polling = setTimeout(
        refresh,
        Math.min(30000, 5000 * 2 ** state.failures),
      );
  }
}

function recordButton(kind, id, title) {
  const button = element("button", title || id, "record-link");
  button.title = id;
  button.addEventListener("click", () => inspect(kind, id));
  return button;
}

function panel(title, description, columns, rows) {
  const section = element("section", undefined, "panel");
  const heading = element("div", undefined, "panel-heading");
  const copy = element("div");
  copy.append(element("h2", title), element("p", description));
  heading.append(
    copy,
    element("span", `${rows.length} in view`, "panel-count"),
  );
  section.append(heading);
  if (!rows.length) {
    const empty = element("div", undefined, "empty");
    empty.append(
      element("strong", "No matching records in this journal window."),
      element(
        "span",
        "Evidence will appear after authorized runtime activity. No sample data is generated.",
      ),
    );
    section.append(empty);
    return section;
  }
  const wrap = element("div", undefined, "table-wrap");
  const table = element("table");
  table.append(
    element(
      "caption",
      `${title}: recent records permitted by your current grants.`,
      "sr-only",
    ),
  );
  const head = element("thead");
  const header = element("tr");
  columns.forEach((column) => {
    const th = element("th", column);
    th.scope = "col";
    header.append(th);
  });
  head.append(header);
  const body = element("tbody");
  rows.forEach((cells) => {
    const row = element("tr");
    cells.forEach((content) => {
      const cell = element("td");
      cell.append(
        content instanceof Node
          ? content
          : document.createTextNode(String(content)),
      );
      row.append(cell);
    });
    body.append(row);
  });
  table.append(head, body);
  wrap.append(table);
  section.append(wrap);
  return section;
}

function recordCell(kind, id, title) {
  const cell = element("div");
  cell.append(recordButton(kind, id, title), element("span", id, "record-id"));
  return cell;
}

function decisionCell(command) {
  const cell = element("div");
  const operation = element("span", command.operation_id, "record-id");
  operation.title = command.operation_id;
  cell.append(element("strong", command.kind), operation);
  return cell;
}

function render() {
  if (!state.snapshot) return;
  const snapshot = state.snapshot;
  const unresolved = snapshot.actions.filter(
    (action) =>
      /IN_FLIGHT|UNKNOWN|STALE/.test(action.state) ||
      /IN_FLIGHT|UNKNOWN|NOT_APPLIED/.test(action.compensation_state),
  );
  const metrics = [
    ["Tasks in view", snapshot.tasks.length, "Durable checkpoints"],
    [
      "Unresolved effects",
      unresolved.length,
      "Investigation or authority needed",
    ],
    [
      "Available approvals",
      snapshot.approvals.filter((approval) => approval.status === "AVAILABLE")
        .length,
      "Server-validated at read time",
    ],
    [
      "Recent decisions",
      snapshot.commands.length,
      "Your recorded control requests",
    ],
  ];
  $("#metrics").replaceChildren(
    ...metrics.map(([label, value, note], index) => {
      const card = element(
        "div",
        undefined,
        `metric ${index === 1 && value ? "warn" : ""}`,
      );
      card.append(
        element("div", label, "metric-label"),
        element("div", String(value), "metric-value"),
        element("div", note, "metric-note"),
      );
      return card;
    }),
  );
  $("#worker-state").textContent = snapshot.worker.state.replaceAll("_", " ");
  $("#worker-state").title =
    "A current lease is execution authority, not proof of worker liveness.";
  $("#view-title").textContent = titles[state.view][0];
  $("#view-description").textContent = titles[state.view][1];
  const content = $("#record-content");
  content.replaceChildren();
  if (["overview", "tasks"].includes(state.view))
    content.append(
      panel(
        "Durable tasks",
        "Checkpoint intent. Inspect effects before taking action.",
        ["Task", "Checkpoint state", "Progress"],
        snapshot.tasks.map((task) => [
          recordCell("tasks", task.id, task.name),
          badge(task.state),
          `${task.current_step} / ${task.total_steps} steps`,
        ]),
      ),
    );
  if (["overview", "actions"].includes(state.view))
    content.append(
      panel(
        "Action evidence",
        "Dispatch records are distinct from task and conversation state.",
        ["Operation", "Effect state", "Compensation", "Epoch"],
        snapshot.actions.map((action) => [
          recordCell("actions", action.id, action.tool),
          badge(action.state),
          badge(action.compensation_state),
          String(action.epoch),
        ]),
      ),
    );
  if (state.view === "approvals")
    content.append(
      panel(
        "Approval ledger",
        "Approval does not dispatch an executor or roll back an effect.",
        [
          "Operation / direction",
          "Executor",
          "Authority",
          "Expires",
          "Control",
        ],
        snapshot.approvals.map((approval) => {
          const revoke = element("button", "Revoke", "subtle");
          revoke.dataset.control = "";
          revoke.disabled =
            !state.fresh ||
            !state.session.permissions.includes("ccs:approve") ||
            approval.status !== "AVAILABLE";
          if (
            approval.status !== "AVAILABLE" ||
            !state.session.permissions.includes("ccs:approve")
          )
            revoke.removeAttribute("data-control");
          revoke.addEventListener("click", () =>
            decision(
              "REVOKE",
              approval,
              "Revoke approval",
              "Revocation blocks future use. It does not reverse an already authorized or completed provider effect.",
            ),
          );
          const operation = element("div");
          operation.append(
            recordButton("actions", approval.action_id, approval.direction),
            element("span", approval.id, "record-id"),
          );
          return [
            operation,
            approval.executor,
            badge(approval.status),
            time(approval.expires_ms),
            revoke,
          ];
        }),
      ),
    );
  if (state.view === "decisions")
    content.append(
      panel(
        "Your decision history",
        "These are local control outcomes. Provider evidence remains on the action.",
        ["Decision", "Target", "Recorded state", "Execution", "Time"],
        snapshot.commands.map((command) => [
          decisionCell(command),
          command.target_id,
          badge(command.result.state),
          command.result.execution ||
            command.result.remote_effects ||
            "UNCHANGED",
          time(command.created_ms),
        ]),
      ),
    );
}

function detailsList(values) {
  const list = element("dl", undefined, "details-list");
  values.forEach(([key, value]) =>
    list.append(
      element("dt", key),
      element("dd", value == null ? "Not recorded" : String(value)),
    ),
  );
  return list;
}

function controlButton(label, enabled, action) {
  const button = element("button", label, "subtle");
  button.disabled = !enabled || !state.fresh;
  if (enabled) button.dataset.control = "";
  button.addEventListener("click", action);
  return button;
}

async function inspect(kind, id) {
  const inspection = (state.inspection = (state.inspection || 0) + 1);
  const identity = state.session;
  try {
    const record = await api(`/api/records/${kind}/${encodeURIComponent(id)}`);
    if (state.inspection !== inspection || state.session !== identity) return;
    state.detail = { kind, record };
    const detail = $("#detail");
    detail.replaceChildren();
    const top = element("div", undefined, "detail-top");
    const title = element("h2", kind === "tasks" ? record.name : record.tool);
    title.tabIndex = -1;
    const close = element("button", "×", "icon-button");
    close.setAttribute("aria-label", "Close evidence panel");
    close.addEventListener("click", () => {
      detail.hidden = true;
      $("#content-grid").classList.remove("with-detail");
      state.detail = null;
      $("#lookup-id").focus();
    });
    top.append(title, close);
    detail.append(
      top,
      element("span", record.id, "record-id"),
      badge(record.state),
      element(
        "p",
        "Evidence at this review. Refresh this panel before deciding if the journal has changed.",
        "detail-note",
      ),
    );
    const actions = element("div", undefined, "detail-actions");
    if (kind === "actions") {
      detail.append(
        detailsList([
          ["Epoch", record.epoch],
          ["Generation", record.generation],
          ["Compensation", record.compensation_state],
          ["Chain", record.chain_id],
          ["Workflow", record.workflow_digest],
          [
            "Provider scope",
            record.provider_scopes.join(", ") || "Unverified / not recorded",
          ],
          ["Operation key", record.idempotency_key],
          [
            "Required grants",
            record.policy?.required_permissions.join(", ") ||
              "Unverified / none recorded",
          ],
          ["Confirmation required", record.policy?.requires_confirmation],
        ]),
      );
      detail.append(
        element("h3", "Exact staged payload"),
        element("pre", JSON.stringify(record.args, null, 2), "evidence"),
      );
      detail.append(
        element("h3", "Recorded receipt"),
        element("pre", JSON.stringify(record.receipt, null, 2), "evidence"),
      );
      actions.append(
        controlButton(
          "Approve forward operation",
          record.can_approve_forward,
          () =>
            decision(
              "APPROVE",
              record,
              "Approve forward operation",
              "Approve this exact staged payload for one execution principal. Recording approval does not dispatch it.",
              "FORWARD",
            ),
        ),
      );
      actions.append(
        controlButton(
          "Approve compensation",
          record.can_approve_compensation,
          () =>
            decision(
              "APPROVE",
              record,
              "Approve compensation",
              "This is a separate mutation against the recorded receipt. Approval does not execute undo.",
              "COMPENSATION",
            ),
        ),
      );
      actions.append(
        controlButton(
          record.reconciliation_requested
            ? "Reconciliation already requested"
            : "Request reconciliation",
          record.can_request_reconciliation,
          () =>
            decision(
              "RECONCILE",
              record,
              "Request reconciliation",
              "Record a request to investigate this unresolved effect. No reconciler is connected. This request will not retry or repair the mutation.",
            ),
        ),
      );
      const events = element("ol", undefined, "event-list");
      record.events.forEach((event) => {
        const item = element("li", `${event.version} · ${event.kind}`);
        item.append(element("time", time(event.created_utc)));
        events.append(item);
      });
      detail.append(element("h3", "Recent action events"), events);
    } else {
      detail.append(
        detailsList([
          ["Checkpoint", record.version],
          ["Progress", `${record.current_step} / ${record.total_steps}`],
          ["Unresolved in window", record.unresolved_effects],
          ["Effect records", record.total_effect_records],
          ["Workflow", record.workflow?.workflow_id],
          ["Revision", record.workflow?.revision],
        ]),
      );
      if (record.error)
        detail.append(element("p", record.error, "detail-note"));
      detail.append(
        element("h3", "Recent owned effects"),
        element("pre", JSON.stringify(record.effects, null, 2), "evidence"),
      );
      actions.append(
        controlButton("Record cancellation intent", record.can_cancel, () =>
          decision(
            "CANCEL",
            record,
            "Record cancellation intent",
            "This closes the task chain and fences future dispatch. It does not stop an in-flight request, undo committed effects, or establish that an unknown effect is absent.",
          ),
        ),
      );
    }
    const refreshEvidence = element("button", "Refresh evidence", "subtle");
    refreshEvidence.addEventListener("click", () => inspect(kind, id));
    actions.append(refreshEvidence);
    detail.append(actions);
    detail.hidden = false;
    $("#content-grid").classList.add("with-detail");
    title.focus();
    if (window.innerWidth < 1150) detail.scrollIntoView({ block: "start" });
  } catch (error) {
    notice(error.message, true);
    if (error.network || error.status >= 500) freshness(false);
  }
}

function decision(kind, record, title, description, direction) {
  if (!state.fresh || state.busy) return;
  const body = {
    operation_id: crypto.randomUUID(),
    review_digest: record.review_digest,
  };
  if (kind === "APPROVE") Object.assign(body, { direction });
  state.decision = { kind, target: record.id, body };
  $("#decision-title").textContent = title;
  $("#decision-description").textContent = description;
  $("#decision-evidence").textContent = JSON.stringify(
    {
      record_id: record.id,
      state: record.state || record.status,
      direction: direction || kind,
      payload: record.args,
      policy: record.policy,
      receipt: direction === "COMPENSATION" ? record.receipt : undefined,
      workflow: record.workflow_digest || record.workflow?.workflow_id,
      provider_scope: record.provider_scopes,
      checkpoint_version: record.version,
      task_name: record.name,
      owned_effects: record.effects,
      total_effect_records: record.total_effect_records,
      action_id: record.action_id,
      approval_executor: record.executor,
      approval_issuer: record.issuer,
      approval_expires_ms: record.expires_ms,
      operator: state.session.principal_id,
      remote_effects: "Not changed by this decision",
    },
    null,
    2,
  );
  $("#approval-fields").hidden = kind !== "APPROVE";
  $("#executor").required = kind === "APPROVE";
  $("#executor").value = "";
  $("#reviewed").checked = false;
  $("#decision-error").textContent = "";
  $("#confirm-decision").disabled = false;
  $("#decision-dialog").showModal();
}

$("#decision-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!state.fresh || state.busy || !state.decision) return;
  const current = state.decision;
  const identity = state.session;
  if (current.kind === "APPROVE")
    Object.assign(current.body, {
      executor: $("#executor").value.trim(),
      ttl_ms: Number($("#approval-lifetime").value),
    });
  state.busy = true;
  freshness(state.fresh);
  $("#dismiss-dialog").disabled = true;
  $("#cancel-dialog").disabled = true;
  try {
    const result = await api(
      `/api/commands/${current.kind.toLowerCase()}/${encodeURIComponent(current.target)}`,
      current.body,
    );
    if (state.session !== identity) return;
    $("#decision-dialog").close();
    const messages = {
      APPROVE: "Approval recorded. No executor was dispatched.",
      REVOKE: "Approval revocation recorded. Remote effects remain unchanged.",
      CANCEL:
        "Cancellation intent recorded. Inspect the action evidence for unresolved or retained effects.",
      RECONCILE:
        "Reconciliation request recorded. No reconciler is connected; the effect remains unresolved.",
    };
    notice(
      `${messages[current.kind]}${result.replayed ? " This operation had already been recorded." : ""}`,
    );
    await refresh();
    if (state.detail) await inspect(state.detail.kind, state.detail.record.id);
  } catch (error) {
    if (state.session !== identity) return;
    $("#decision-error").textContent = error.message;
    if (error.network || error.code === "stale_review" || error.status >= 500) {
      $("#decision-dialog").close();
      state.decision = null;
      notice(
        `${error.message} Operation ID: ${current.body.operation_id}`,
        true,
      );
      freshness(false);
    }
  } finally {
    state.busy = false;
    $("#dismiss-dialog").disabled = false;
    $("#cancel-dialog").disabled = false;
    freshness(state.fresh);
  }
});

$("#connect-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  $("#connect-button").disabled = true;
  $("#connect-error").textContent = "";
  try {
    const identity = await api("/api/login", {
      credential: $("#credential").value,
    });
    connected(identity);
    await refresh();
  } catch (error) {
    $("#connect-error").textContent = error.message;
  } finally {
    $("#credential").value = "";
    $("#connect-button").disabled = false;
  }
});
$("#navigation").addEventListener("click", (event) => {
  const button = event.target.closest("button[data-view]");
  if (!button) return;
  state.view = button.dataset.view;
  document
    .querySelectorAll("#navigation button")
    .forEach((node) => node.removeAttribute("aria-current"));
  button.setAttribute("aria-current", "page");
  render();
});
$("#lookup").addEventListener("submit", (event) => {
  event.preventDefault();
  inspect($("#lookup-kind").value, $("#lookup-id").value.trim());
});
$("#refresh").addEventListener("click", refresh);
$("#sign-out").addEventListener("click", async () => {
  try {
    await api("/api/logout", {});
    disconnected();
  } catch (error) {
    notice(error.message, true);
  }
});
for (const id of ["#dismiss-dialog", "#cancel-dialog"])
  $(id).addEventListener("click", () => {
    if (!state.busy) $("#decision-dialog").close();
  });
$("#decision-dialog").addEventListener("cancel", (event) => {
  if (state.busy) event.preventDefault();
});
document.addEventListener("visibilitychange", () => {
  clearTimeout(state.polling);
  if (state.session) {
    freshness(false);
    if (!document.hidden) refresh();
  }
});
try {
  connected(await api("/api/session"));
  await refresh();
} catch {
  disconnected();
}
