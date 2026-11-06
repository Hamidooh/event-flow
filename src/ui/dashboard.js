/**
 * EventFlow — Minimalist Stages Controller v2
 * Production-hardened: null-safe DOM, XSS-escaped output, debounced polling,
 * exponential-backoff WS reconnect, in-flight request guard, live ticker,
 * and full error boundary on every async path.
 */

(function () {
  'use strict';

  /* ─────────────────────────── State ─────────────────────────── */
  let workflows        = [];
  let instances        = [];
  let selectedInstId   = null;    // currently selected workflow instance id
  let selectedStepId   = null;    // currently inspected step id
  let currentInstance  = null;    // full instance object (kept for step inspector)
  let ws               = null;
  let wsReconnectDelay = 1000;    // grows with each failed reconnect (backoff)
  let isRunning        = false;   // guard against double-submit
  let eventsLog        = [];      // capped at MAX_LOG
  let feedPaused       = false;   // let the user freeze the feed scroll
  let pollingTimer     = null;    // only active while a run is RUNNING/PAUSED
  const MAX_LOG        = 200;
  const POLL_INTERVAL  = 1500;   // ms

  /* ────────────────────── Utility: safe DOM ───────────────────── */
  function $(id) {
    const el = document.getElementById(id);
    if (!el) console.warn('[EventFlow] Missing DOM element:', id);
    return el;
  }

  /* XSS-safe text setter (never use innerHTML for user data) */
  function setText(el, text) {
    if (el) el.textContent = String(text ?? '');
  }

  /* Safe attribute setter */
  function setAttr(el, attr, val) {
    if (el) el.setAttribute(attr, val);
  }

  /* Escape HTML for use inside innerHTML where needed */
  function esc(str) {
    return String(str ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  /* ────────────────────── Element references ─────────────────── */
  const elWsStatus         = $('ws-status');
  const elWorkflowSelect   = $('workflow-select');
  const elWorkflowInput    = $('workflow-input');
  const elBtnRun           = $('btn-run');
  const elBtnTogglePayload = $('btn-toggle-payload');
  const elPayloadDrawer    = $('payload-drawer');
  const elBtnClosePayload  = $('btn-close-payload');
  const elBtnCopyJson      = $('btn-copy-json');

  const elActiveTitle      = $('active-workflow-title');
  const elActiveBadge      = $('active-instance-badge');
  const elRunMeta          = $('run-meta-text');
  const elProgressBar      = $('overall-progress-bar');
  const elProgressCount    = $('progress-steps-count');
  const elProgressPct      = $('progress-percentage');

  const elControls         = $('instance-controls');
  const elBtnPause         = $('btn-pause');
  const elBtnResume        = $('btn-resume');
  const elBtnCancel        = $('btn-cancel');
  const elBtnRefresh       = $('btn-refresh');

  const elInstancesList    = $('instances-list');

  const elStage1Cards      = $('stage-1-cards');
  const elStage2Cards      = $('stage-2-cards');
  const elStage3Cards      = $('stage-3-cards');
  const elStage1Sub        = $('stage-1-sub');
  const elStage2Sub        = $('stage-2-sub');
  const elStage3Sub        = $('stage-3-sub');

  const elInspectorTitle   = $('inspector-step-title');
  const elInspectorStatus  = $('inspector-step-status');
  const elJsonViewer       = $('json-viewer');
  const elFeedStream       = $('terminal-logs');
  const elGanttChart       = $('gantt-chart');

  const elMetricWorkers    = $('metric-active-workers');
  const elMetricQueued     = $('metric-queued-tasks');
  const elMetricTotal      = $('metric-total-workflows');
  const elMetricActive     = $('metric-active-workflows');
  const elMetricSuccess    = $('metric-success-rate');
  const elMetricFailed     = $('metric-failed-count');
  const elMetricAvgDur     = $('metric-avg-duration');
  const elMetricRetried    = $('metric-retried-count');

  /* ─────────────────────── Payload drawer ────────────────────── */
  if (elBtnTogglePayload && elPayloadDrawer) {
    elBtnTogglePayload.addEventListener('click', () => {
      const hidden = elPayloadDrawer.style.display === 'none';
      elPayloadDrawer.style.display = hidden ? 'block' : 'none';
    });
  }
  if (elBtnClosePayload && elPayloadDrawer) {
    elBtnClosePayload.addEventListener('click', () => {
      elPayloadDrawer.style.display = 'none';
    });
  }

  /* ─────────────────────── Copy JSON button ──────────────────── */
  if (elBtnCopyJson && elJsonViewer) {
    elBtnCopyJson.addEventListener('click', async () => {
      const text = elJsonViewer.textContent || '';
      try {
        await navigator.clipboard.writeText(text);
        const originalHtml = elBtnCopyJson.innerHTML;
        elBtnCopyJson.innerHTML = '<span>✓</span> Copied!';
        setTimeout(() => { elBtnCopyJson.innerHTML = originalHtml; }, 1800);
      } catch {
        showToast('Unable to copy to clipboard', 'warn');
      }
    });
  }

  /* ─────────────────────── Tab switching ─────────────────────── */
  document.querySelectorAll('.tab-item').forEach((tab) => {
    tab.addEventListener('click', () => {
      document.querySelectorAll('.tab-item').forEach((t) => t.classList.remove('active'));
      document.querySelectorAll('.tab-pane').forEach((p) => p.classList.remove('active'));
      tab.classList.add('active');
      const target = tab.getAttribute('data-tab');
      if (target) document.getElementById(target)?.classList.add('active');
    });
  });

  /* ───────────────── Feed controls bar (pause + clear) ───────── */
  if (elFeedStream) {
    const bar = document.createElement('div');
    bar.className = 'feed-controls-bar';

    const pauseBtn = document.createElement('button');
    pauseBtn.className = 'feed-pause-btn';
    pauseBtn.textContent = '⏸ Pause Feed';
    pauseBtn.addEventListener('click', () => {
      feedPaused = !feedPaused;
      pauseBtn.textContent = feedPaused ? '▶ Resume Feed' : '⏸ Pause Feed';
      pauseBtn.classList.toggle('paused', feedPaused);
    });

    const clearBtn = document.createElement('button');
    clearBtn.className = 'feed-clear-btn';
    clearBtn.textContent = '🗑 Clear';
    clearBtn.addEventListener('click', () => {
      eventsLog = [];
      elFeedStream.innerHTML = '<div class="feed-empty">Feed cleared.</div>';
    });

    bar.appendChild(pauseBtn);
    bar.appendChild(clearBtn);
    elFeedStream.parentElement?.insertBefore(bar, elFeedStream);
  }

  /* ─────────────────────── WebSocket ─────────────────────────── */
  function setWsStatus(connected) {
    if (!elWsStatus) return;
    const dot  = elWsStatus.querySelector('.status-dot');
    const text = elWsStatus.querySelector('.status-text');
    if (connected) {
      elWsStatus.style.background = 'var(--badge-green-bg)';
      elWsStatus.style.color      = 'var(--badge-green-text)';
      if (dot)  dot.style.background  = 'var(--badge-green-text)';
      if (text) text.textContent = 'Live Sync';
      wsReconnectDelay = 1000;   // reset backoff on success
    } else {
      elWsStatus.style.background = 'var(--badge-amber-bg)';
      elWsStatus.style.color      = 'var(--badge-amber-text)';
      if (dot)  dot.style.background  = 'var(--badge-amber-text)';
      if (text) text.textContent = 'Reconnecting…';
    }
  }

  function initWebSocket() {
    if (ws && ws.readyState <= 1) return; // already open / connecting

    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    ws = new WebSocket(`${protocol}//${location.host}/ws`);

    ws.onopen = () => {
      setWsStatus(true);
    };

    ws.onclose = () => {
      setWsStatus(false);
      ws = null;
      wsReconnectDelay = Math.min(wsReconnectDelay * 1.5, 30000);
      setTimeout(initWebSocket, wsReconnectDelay);
    };

    ws.onerror = (err) => {
      console.warn('[EventFlow] WebSocket error:', err);
    };

    ws.onmessage = (event) => {
      let msg;
      try { msg = JSON.parse(event.data); }
      catch (e) { console.warn('[EventFlow] Bad WS frame', e); return; }

      if (msg.type === 'INIT_STATE') {
        workflows = Array.isArray(msg.workflows) ? msg.workflows : [];
        instances = Array.isArray(msg.instances) ? msg.instances : [];
        renderWorkflowDropdown();
        renderHistoryList();
        if (msg.metrics) updateMetrics(msg.metrics);
        // Auto-select the most recent instance or show template
        if (!selectedInstId && instances.length > 0) {
          selectInstance(instances[instances.length - 1].id);
        } else if (workflows.length > 0 && !selectedInstId) {
          renderWorkflowTemplate(workflows[0]?.id);
        }

      } else if (msg.type === 'WORKFLOW_EVENT') {
        if (msg.event)   appendFeedEvent(msg.event);
        if (msg.metrics) updateMetrics(msg.metrics);

        // Debounced refresh for active instance to avoid request spam on rapid events
        if (msg.event?.workflowInstanceId === selectedInstId) {
          scheduleInstanceRefresh(selectedInstId);
        } else {
          scheduleHistoryRefresh(); // debounced background refresh for history list
        }
      }
    };
  }

  /* ───────────────── Debounced UI Refreshes ───────────────────── */
  let instanceRefreshTimer = null;
  function scheduleInstanceRefresh(instanceId) {
    if (instanceRefreshTimer) clearTimeout(instanceRefreshTimer);
    instanceRefreshTimer = setTimeout(() => {
      selectInstance(instanceId);
    }, 40);
  }

  let historyRefreshTimer = null;
  function scheduleHistoryRefresh() {
    if (historyRefreshTimer) clearTimeout(historyRefreshTimer);
    historyRefreshTimer = setTimeout(() => {
      fetchInstances();
    }, 120);
  }

  /* ───────────────────── Live polling guard ───────────────────── */
  function startPolling(instanceId) {
    stopPolling();
    pollingTimer = setInterval(async () => {
      if (!instanceId) { stopPolling(); return; }
      try {
        const res  = await fetch(`/api/instances/${instanceId}`);
        if (!res.ok) return;
        const inst = await res.json();
        // Stop polling once the workflow is in a terminal state
        if (!inst || ['COMPLETED','FAILED','CANCELLED'].includes(inst.status)) {
          stopPolling();
        }
        applyInstanceToUI(inst);
      } catch { /* network hiccup - keep polling */ }
    }, POLL_INTERVAL);
  }

  function stopPolling() {
    if (pollingTimer) { clearInterval(pollingTimer); pollingTimer = null; }
  }

  /* ───────────────────────── Metrics ─────────────────────────── */
  function updateMetrics(m) {
    if (!m) return;
    setText(elMetricTotal,   m.totalWorkflowsExecuted ?? 0);
    setText(elMetricActive,  `${m.activeWorkflows ?? 0} running`);
    const wp = m.workerPool;
    if (wp) {
      setText(elMetricWorkers, `${wp.activeWorkers} / ${wp.totalCapacity}`);
      setText(elMetricQueued,  `${wp.queuedTasks} queued`);
      setText(elMetricAvgDur,  `${wp.averageExecutionTimeMs}ms`);
      setText(elMetricRetried, `${wp.retriedTasks} retried`);
    }
    const done  = (m.successfulWorkflows || 0) + (m.failedWorkflows || 0);
    const rate  = done > 0 ? Math.round((m.successfulWorkflows / done) * 100) : 100;
    setText(elMetricSuccess, `${rate}%`);
    setText(elMetricFailed,  `${m.failedWorkflows ?? 0} failures`);
  }

  /* ─────────────────── Workflow dropdown ─────────────────────── */
  let dropdownListenerAttached = false;
  function updateDefaultPayload(workflowId) {
    if (!elWorkflowInput) return;
    if (workflowId === 'ecommerce-checkout-saga') {
      elWorkflowInput.value = JSON.stringify({
        customerId: 'CUST-9421',
        simulatePaymentFailure: false,
      }, null, 2);
    } else if (workflowId === 'etl-data-pipeline') {
      elWorkflowInput.value = JSON.stringify({
        batchId: 'BATCH-101',
        recordsCount: 50,
      }, null, 2);
    }
  }

  function renderWorkflowDropdown() {
    if (!elWorkflowSelect) return;
    elWorkflowSelect.innerHTML = workflows
      .map((w) => `<option value="${esc(w.id)}">${esc(w.name)}</option>`)
      .join('');

    if (!dropdownListenerAttached) {
      elWorkflowSelect.addEventListener('change', () => {
        const id = elWorkflowSelect.value;
        if (id) {
          selectedInstId = null;
          currentInstance = null;
          selectedStepId = null;
          updateDefaultPayload(id);
          renderWorkflowTemplate(id);
        }
      });
      dropdownListenerAttached = true;
    }

    // Only show template preview when no live instance is currently selected
    if (workflows.length > 0 && !selectedInstId) {
      updateDefaultPayload(workflows[0]?.id);
      renderWorkflowTemplate(workflows[0]?.id);
    }
  }

  /* ─────────────────── History sidebar ───────────────────────── */
  async function fetchInstances() {
    try {
      const res  = await fetch('/api/instances');
      if (!res.ok) return;
      const data = await res.json();
      instances  = Array.isArray(data.instances) ? data.instances : [];
      renderHistoryList();
    } catch { /* silent */ }
  }

  function renderHistoryList() {
    if (!elInstancesList) return;
    if (instances.length === 0) {
      elInstancesList.innerHTML = '<div class="empty-card-placeholder">No executions yet.</div>';
      return;
    }

    const html = instances
      .slice()
      .reverse()
      .map((inst) => {
        const isSelected = inst.id === selectedInstId;
        const time = new Date(inst.startTime).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        const dur  = inst.durationMs ? `${inst.durationMs}ms` : 'running';
        return `
          <div class="history-item${isSelected ? ' active' : ''}" data-id="${esc(inst.id)}" role="button" tabindex="0"
               aria-label="${esc(inst.workflowName)} — ${inst.status}">
            <div class="history-item-top">
              <span class="history-item-title">${esc(inst.workflowName)}</span>
              <span class="status-pill pill-${inst.status.toLowerCase()}">${esc(inst.status)}</span>
            </div>
            <div class="history-item-meta">
              <span>${esc(time)}</span>
              <span>${esc(dur)}</span>
            </div>
          </div>`;
      })
      .join('');

    elInstancesList.innerHTML = html;
    elInstancesList.querySelectorAll('.history-item').forEach((item) => {
      const handler = () => selectInstance(item.getAttribute('data-id'));
      item.addEventListener('click', handler);
      item.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') handler(); });
    });
  }

  /* ─────────────────── Select / load instance ─────────────────── */
  async function selectInstance(instanceId) {
    if (!instanceId) return;
    selectedInstId = instanceId;
    renderHistoryList();

    try {
      const res  = await fetch(`/api/instances/${instanceId}`);
      if (!res.ok) { console.warn('[EventFlow] Instance fetch', res.status); return; }
      const inst = await res.json();

      // Guard against race condition: only apply if this is still the selected instance
      if (selectedInstId !== instanceId) return;

      applyInstanceToUI(inst);

      // Start live-polling for active workflows (supplement WS)
      if (inst && (inst.status === 'RUNNING' || inst.status === 'PAUSED')) {
        startPolling(instanceId);
      } else {
        stopPolling();
      }
    } catch (e) {
      console.error('[EventFlow] selectInstance error:', e);
    }
  }

  /* ─────────────────── Workflow template preview ──────────────── */
  function renderWorkflowTemplate(workflowId) {
    const wf = workflows.find((w) => w.id === workflowId);
    if (!wf) return;

    const stepCount = wf.steps?.length ?? wf.stepsCount ?? 0;
    setText(elActiveTitle,   wf.name);
    setText(elRunMeta,       `${stepCount} steps configured. Click "Run Workflow" to begin.`);
    setActiveBadge('READY', 'idle');
    setProgress(0, 0, stepCount, 'IDLE');
    if (elControls) elControls.style.display = 'none';

    // Render step cards with no execution data (template mode)
    partitionAndRender(wf.steps || [], null);
  }

  /* ─────────────────── Apply instance to entire UI ───────────── */
  function applyInstanceToUI(inst) {
    if (!inst) return;
    currentInstance = inst;

    setText(elActiveTitle,  inst.workflowName);
    setActiveBadge(inst.status, inst.status.toLowerCase());

    const timeStr = new Date(inst.startTime).toLocaleTimeString();
    const durStr  = inst.durationMs ? `in ${inst.durationMs}ms` : 'in progress…';
    setText(elRunMeta,
      `Run #${inst.id.slice(0, 8)} · started ${timeStr} · ${durStr}` +
      (inst.error ? ` · Error: ${inst.error}` : '')
    );

    // Progress
    const stepArr    = Object.values(inst.stepStates || {});
    const total      = stepArr.length;
    const done       = stepArr.filter((s) => s.status === 'COMPLETED').length;
    setProgress(done, total, total, inst.status);

    // Controls
    renderControls(inst.status);

    // Board
    partitionAndRender(stepArr, inst);

    // Waterfall
    renderWaterfall(inst);

    // Update the inspector if a step is currently selected
    if (selectedStepId && inst.stepStates?.[selectedStepId]) {
      openStepInspector(selectedStepId, inst);
    }
  }

  function setActiveBadge(label, cssClass) {
    if (!elActiveBadge) return;
    elActiveBadge.textContent = label;
    // Allow passing a raw status (e.g. 'WAITING_DEPS') and auto-normalise
    const cls = (cssClass || label).toLowerCase().replace(/_/g, '-');
    elActiveBadge.className = `status-pill pill-${cls}`;
  }

  function setProgress(done, total, outOf, status = 'RUNNING') {
    const pct = outOf > 0 ? Math.round((done / outOf) * 100) : 0;
    if (elProgressBar) {
      elProgressBar.style.width = `${pct}%`;
      elProgressBar.className = 'progress-bar-fill';
      if (status === 'FAILED') elProgressBar.classList.add('progress-failed');
      else if (status === 'CANCELLED') elProgressBar.classList.add('progress-cancelled');
      else if (status === 'COMPLETED') elProgressBar.classList.add('progress-completed');
    }
    if (elProgressCount) elProgressCount.textContent = `${done} of ${total} steps completed`;
    if (elProgressPct)   elProgressPct.textContent   = `${pct}%`;
  }

  function renderControls(status) {
    if (!elControls) return;
    elControls.style.display = 'flex';
    const show = (el, v) => { if (el) el.style.display = v ? 'inline-flex' : 'none'; };
    show(elBtnPause,  status === 'RUNNING');
    show(elBtnResume, status === 'PAUSED');
    show(elBtnCancel, status === 'RUNNING' || status === 'PAUSED');
    if (status === 'COMPLETED' || status === 'FAILED' || status === 'CANCELLED') {
      elControls.style.display = 'none';
    }
  }

  /* ─────────────────── Step icon mapping ─────────────────────── */
  function stepIcon(stepId, name) {
    const t = `${stepId} ${name}`.toLowerCase();
    if (t.match(/extract|source|fetch|ingest|read/)) return '📥';
    if (t.match(/validate|cleanse|schema|verify|check/)) return '🔍';
    if (t.match(/geo|ip|region|network|location/)) return '🌐';
    if (t.match(/fraud|risk|ai|ml|score|model|inference/)) return '🧠';
    if (t.match(/pay|card|stripe|billing|charge/)) return '💳';
    if (t.match(/stock|inventory|reserve|lock/)) return '🔒';
    if (t.match(/ship|courier|fedex|dispatch|deliver/)) return '📦';
    if (t.match(/aggregate|load|warehouse|db|storage|save|write/)) return '💾';
    if (t.match(/notify|email|webhook|alert/)) return '🔔';
    if (t.match(/transform|map|filter|enrich/)) return '⚙️';
    return '⚡';
  }

  /* ──────────── Partition steps into 3 stage columns ─────────── */
  function partitionAndRender(rawSteps, inst) {
    // Normalise: handles both StepDefinition (from workflow template) and StepExecutionRecord
    const steps = rawSteps.map((s) => {
      const id   = s.id ?? s.stepId ?? '?';
      const rec  = inst?.stepStates?.[id] ?? null;
      return {
        stepId:       id,
        name:         s.name ?? id,
        dependencies: s.dependencies ?? [],
        status:       rec?.status   ?? 'PENDING',
        durationMs:   rec?.durationMs ?? null,
        attempt:      rec?.attempt   ?? 0,
        output:       rec?.output    ?? inst?.stepOutputs?.[id] ?? null,
        error:        rec?.error     ?? null,
        logs:         rec?.logs      ?? [],
      };
    });

    if (!steps.length) return;

    // Build dependency sets for classification
    const isDepOf = new Set(); // ids that appear AS a dependency of another step
    steps.forEach((s) => s.dependencies.forEach((d) => isDepOf.add(d)));

    const s1 = steps.filter((s) => s.dependencies.length === 0);
    const s3 = steps.filter((s) => s.dependencies.length > 0 && !isDepOf.has(s.stepId));
    const s2 = steps.filter((s) => !s1.includes(s) && !s3.includes(s));

    // Edge-case: all steps are linear and nothing classifies as a leaf
    if (s3.length === 0 && steps.length > 1) {
      const last = steps[steps.length - 1];
      s3.push(last);
      const idx = s2.indexOf(last);
      if (idx !== -1) s2.splice(idx, 1);
    }

    renderColumn(elStage1Cards, elStage1Sub, s1, inst);
    renderColumn(elStage2Cards, elStage2Sub, s2, inst);
    renderColumn(elStage3Cards, elStage3Sub, s3, inst);
  }

  function friendlyPreview(s) {
    if (s.error) return `⚠️ ${s.error}`;
    if (s.output) {
      const o = s.output;
      if (typeof o.recordsCount  === 'number') return `${o.recordsCount} records extracted`;
      if (typeof o.cleansedRecords === 'number') return `${o.cleansedRecords} records validated`;
      if (typeof o.averageRiskScore === 'number') return `Fraud score: ${o.averageRiskScore}`;
      if (Array.isArray(o.regions)) return `Enriched for: ${o.regions.join(', ')}`;
      if (typeof o.loadedPartitions === 'number') return `Loaded ${o.loadedPartitions} warehouse partitions`;
      if (o.reservationId) return `Stock reserved: ${o.reservationId}`;
      if (o.trackingNumber) return `Waybill: ${o.trackingNumber}`;
      if (o.transactionId)  return `Charged: ${o.transactionId}`;
      if (o.verified)       return 'Customer identity verified ✓';
      return 'Output produced ✓';
    }
    if (s.status === 'RUNNING')  return 'Currently executing…';
    if (s.status === 'RETRYING') return `Retrying (attempt ${s.attempt})…`;
    if (s.dependencies.length)   return `Waiting for: ${s.dependencies.join(', ')}`;
    return 'Trigger / root step';
  }

  function renderColumn(container, subEl, steps, inst) {
    if (!container || !subEl) return;
    setText(subEl, `${steps.length} step${steps.length !== 1 ? 's' : ''}`);

    if (!steps.length) {
      container.innerHTML = '<div class="empty-card-placeholder">No steps in this stage.</div>';
      return;
    }

    container.innerHTML = steps.map((s) => {
      const icon     = stepIcon(s.stepId, s.name);
      const dur      = s.durationMs != null ? `${s.durationMs}ms`
                     : s.status === 'RUNNING'  ? '⏱ running…'
                     : s.status === 'RETRYING' ? '🔁 retrying…'
                     : '—';
      const preview  = friendlyPreview(s);
      const depsText = s.dependencies.length ? `Needs: ${s.dependencies.join(', ')}` : 'Root';
      const sel      = s.stepId === selectedStepId ? ' active-selected' : '';
      const retryBadge = s.attempt > 1
        ? `<span class="retry-badge">retry #${s.attempt}</span>` : '';
      // Normalize status for CSS: WAITING_DEPS → waiting-deps
      const statusCls = s.status.toLowerCase().replace(/_/g, '-');

      return `
        <div class="step-card status-${esc(s.status)}${sel}" data-step="${esc(s.stepId)}"
             role="button" tabindex="0" aria-label="${esc(s.name)} — ${esc(s.status)}">
          <div class="step-card-top">
            <div class="step-title-wrap">
              <span class="step-icon" aria-hidden="true">${icon}</span>
              <span class="step-title">${esc(s.name)}</span>
            </div>
            <div class="step-badges">
              <span class="status-pill pill-${esc(statusCls)}">${esc(s.status)}</span>
              ${retryBadge}
            </div>
          </div>
          <div class="step-preview">${esc(preview)}</div>
          <div class="step-footer">
            <span class="step-dep-label">${esc(depsText)}</span>
            <span class="step-dur">${esc(dur)}</span>
          </div>
        </div>`;
    }).join('');

    container.querySelectorAll('.step-card').forEach((card) => {
      const handler = () => openStepInspector(card.getAttribute('data-step'), inst ?? currentInstance);
      card.addEventListener('click', handler);
      card.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') handler(); });
    });
  }

  /* ─────────────────── Step Inspector (bottom drawer) ─────────── */
  function openStepInspector(stepId, inst) {
    if (!stepId) return;
    selectedStepId = stepId;

    // Highlight selected card
    document.querySelectorAll('.step-card').forEach((c) => {
      c.classList.toggle('active-selected', c.getAttribute('data-step') === stepId);
    });

    // Switch to details tab
    document.querySelector('[data-tab="tab-details"]')?.click();

    const step = inst?.stepStates?.[stepId] ?? null;
    if (step) {
      setText(elInspectorTitle,  step.name);
      setActivePillOn(elInspectorStatus, step.status);
      if (elJsonViewer) elJsonViewer.textContent = JSON.stringify(step, null, 2);
    } else {
      const currentWfId = elWorkflowSelect?.value;
      const wf = workflows.find((w) => w.id === currentWfId);
      const stepDef = wf?.steps?.find((s) => s.id === stepId);
      setText(elInspectorTitle, stepDef?.name || stepId);
      if (elInspectorStatus) {
        elInspectorStatus.textContent = 'CONFIGURED';
        elInspectorStatus.className   = 'status-pill pill-ready';
      }
      if (elJsonViewer) {
        if (stepDef) {
          elJsonViewer.textContent = JSON.stringify({
            stepId: stepDef.id,
            name: stepDef.name,
            dependencies: stepDef.dependencies || [],
            status: 'CONFIGURED',
            info: 'Step definition configured in workflow template. Click "Run Workflow" to execute.',
          }, null, 2);
        } else {
          elJsonViewer.textContent = '// Step has not started yet in this run.';
        }
      }
    }
  }

  function setActivePillOn(el, status) {
    if (!el) return;
    el.textContent = status;
    // Normalize status to safe CSS class name (e.g. WAITING_DEPS → waiting-deps)
    const cls = status.toLowerCase().replace(/_/g, '-');
    el.className = `status-pill pill-${cls}`;
  }

  /* ──────────────────── Waterfall Timeline ───────────────────── */
  function renderWaterfall(inst) {
    if (!elGanttChart) return;
    const steps = Object.values(inst?.stepStates || {}).filter((s) => s.startTime);
    if (!steps.length) {
      elGanttChart.innerHTML = '<div class="feed-empty">Timeline renders automatically when steps start executing.</div>';
      return;
    }

    // Sort chronologically by execution start time
    steps.sort((a, b) => (a.startTime || 0) - (b.startTime || 0));

    const minT  = Math.min(...steps.map((s) => s.startTime));
    const now   = Date.now();
    const maxT  = Math.max(...steps.map((s) => s.endTime || now), minT + 50);
    const span  = maxT - minT || 1;

    elGanttChart.innerHTML = steps.map((step) => {
      const offset = ((step.startTime - minT) / span) * 100;
      const end    = step.endTime || now;
      const width  = Math.max(3, ((end - step.startTime) / span) * 100);
      const dur    = step.durationMs ? `${step.durationMs}ms` : `${end - step.startTime}ms`;
      const color  = step.status === 'FAILED'    ? 'var(--badge-rose-text)'
                   : step.status === 'RETRYING'  ? 'var(--badge-amber-text)'
                   : step.status === 'COMPLETED' ? 'var(--badge-green-text)'
                   : 'var(--color-brand)';
      return `
        <div class="waterfall-row">
          <span class="waterfall-label" title="${esc(step.name)}">${esc(step.name)}</span>
          <div class="waterfall-track">
            <div class="waterfall-fill" style="left:${offset}%;width:${width}%;background:${color};"></div>
          </div>
          <span class="waterfall-dur">${esc(dur)}</span>
        </div>`;
    }).join('');
  }

  /* ─────────────────── Live Activity Feed ────────────────────── */
  function appendFeedEvent(evt) {
    if (feedPaused) return;

    eventsLog.unshift(evt);
    if (eventsLog.length > MAX_LOG) eventsLog.length = MAX_LOG;

    if (!elFeedStream) return;

    // Remove placeholder text
    elFeedStream.querySelector('.feed-empty')?.remove();

    const time = new Date(evt.timestamp).toLocaleTimeString([], {
      hour: '2-digit', minute: '2-digit', second: '2-digit',
    });

    const type = String(evt.type ?? '');
    let pillCls = 'running';
    if (type.includes('FAIL') || type.includes('COMPENS'))    pillCls = 'failed';
    if (type.includes('COMPLET') || type.includes('SUCCESS')) pillCls = 'completed';
    if (type.includes('RETRYING'))                            pillCls = 'retrying';
    if (type.includes('PAUSED'))                              pillCls = 'paused';

    let msg = evt.payload?.message || '';
    if (!msg) {
      if (type === 'STEP_STARTED')           msg = `Execution started`;
      else if (type === 'STEP_COMPLETED')    msg = `Completed in ${evt.payload?.durationMs ?? 0}ms`;
      else if (type === 'STEP_FAILED')       msg = `Failed — ${evt.payload?.error ?? 'unknown error'}`;
      else if (type === 'STEP_RETRYING')     msg = `Retrying (attempt ${evt.payload?.attempt ?? '?'})`;
      else if (type === 'WORKFLOW_STARTED')  msg = 'Workflow execution started';
      else if (type === 'WORKFLOW_COMPLETED')msg = `Workflow completed`;
      else if (type === 'WORKFLOW_FAILED')   msg = `Workflow failed — ${evt.payload?.error ?? ''}`;
      else if (type === 'STEP_COMPENSATION_STARTED')   msg = '↩ Running compensation';
      else if (type === 'STEP_COMPENSATION_COMPLETED') msg = '↩ Compensation done';
      else msg = type;
    }

    const item = document.createElement('div');
    item.className = 'feed-item';
    item.innerHTML = `
      <span class="feed-time">${esc(time)}</span>
      <span class="status-pill pill-${pillCls}">${esc(evt.stepId ?? 'SYSTEM')}</span>
      <span class="feed-msg">${esc(msg)}</span>`;

    // Limit DOM size
    if (elFeedStream.children.length >= MAX_LOG) {
      elFeedStream.lastElementChild?.remove();
    }
    elFeedStream.insertBefore(item, elFeedStream.firstChild);
  }

  /* ─────────────────── Run Workflow button ───────────────────── */
  function parsePayload() {
    try {
      return JSON.parse(elWorkflowInput?.value?.trim() || '{}');
    } catch {
      return null;
    }
  }

  function showToast(msg, type = 'info') {
    // Simple non-blocking toast
    const t = document.createElement('div');
    t.className = `toast toast-${type}`;
    t.textContent = msg;
    document.body.appendChild(t);
    requestAnimationFrame(() => t.classList.add('toast-visible'));
    setTimeout(() => { t.classList.remove('toast-visible'); setTimeout(() => t.remove(), 300); }, 3500);
  }

  if (elBtnRun) {
    elBtnRun.addEventListener('click', async () => {
      if (isRunning) return;

      const workflowId = elWorkflowSelect?.value;
      if (!workflowId) { showToast('Please select a workflow first.', 'warn'); return; }

      const payload = parsePayload();
      if (payload === null) { showToast('Invalid JSON in payload. Please fix and retry.', 'warn'); return; }

      isRunning = true;
      elBtnRun.disabled = true;
      elBtnRun.innerHTML = '<span class="btn-spinner">⏳</span> Starting…';

      try {
        const res  = await fetch(`/api/workflows/${encodeURIComponent(workflowId)}/run`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        const data = await res.json();

        if (!res.ok) {
          showToast(`Server error: ${data?.error ?? res.status}`, 'error');
          return;
        }

        if (data.instanceId) {
          showToast('Workflow started! 🚀', 'success');
          await selectInstance(data.instanceId);
          startPolling(data.instanceId);
        } else {
          showToast('Workflow accepted but no instance ID returned.', 'warn');
        }
      } catch (e) {
        console.error('[EventFlow] Run error:', e);
        showToast(`Network error: ${e.message}`, 'error');
      } finally {
        isRunning = false;
        elBtnRun.disabled = false;
        elBtnRun.innerHTML = '<span class="btn-icon">▶</span> Run Workflow';
      }
    });
  }

  /* ─────────────── Pause / Resume / Cancel ───────────────────── */
  async function postAction(action, label) {
    if (!selectedInstId) return;
    try {
      const res  = await fetch(`/api/instances/${selectedInstId}/${action}`, { method: 'POST' });
      const data = await res.json();
      if (!res.ok) showToast(`Could not ${label}: ${data?.error ?? res.status}`, 'warn');
      else { showToast(`${label} successful.`, 'success'); await selectInstance(selectedInstId); }
    } catch (e) {
      showToast(`Network error on ${label}: ${e.message}`, 'error');
    }
  }

  elBtnPause?.addEventListener('click',  () => postAction('pause',  'Pause'));
  elBtnResume?.addEventListener('click', () => postAction('resume', 'Resume'));
  elBtnCancel?.addEventListener('click', () => {
    if (!confirm('Cancel this workflow execution?')) return;
    postAction('cancel', 'Cancel').then(stopPolling);
  });

  /* ─────────────────── Refresh button ────────────────────────── */
  elBtnRefresh?.addEventListener('click', async () => {
    elBtnRefresh.textContent = '⏳';
    await fetchInstances();
    elBtnRefresh.textContent = '↻';
  });

  /* ─────────────── Keyboard shortcut: R → Run ─────────────────── */
  document.addEventListener('keydown', (e) => {
    // Skip when focus is inside an input / textarea
    if (['INPUT','SELECT','TEXTAREA'].includes(e.target?.tagName)) return;
    if (e.key === 'r' || e.key === 'R') elBtnRun?.click();
    if (e.key === 'Escape') elPayloadDrawer && (elPayloadDrawer.style.display = 'none');
  });

  /* ─────────────────────── Boot ──────────────────────────────── */
  setWsStatus(false);   // show amber until WS connects
  initWebSocket();
})();
