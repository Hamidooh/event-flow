/**
 * EventFlow - Web Dashboard Client Controller
 * Real-time WebSocket streaming, topological DAG layout, Gantt timeline, and controls.
 */

(() => {
  let workflows = [];
  let instances = [];
  let selectedInstanceId = null;
  let ws = null;
  const eventsLog = [];

  // DOM Elements
  const elSystemTime = document.getElementById('system-time');
  const elWsStatus = document.getElementById('ws-status');
  const elMetricActiveWorkflows = document.getElementById('metric-active-workflows');
  const elMetricTotalWorkflows = document.getElementById('metric-total-workflows');
  const elMetricActiveWorkers = document.getElementById('metric-active-workers');
  const elMetricQueuedTasks = document.getElementById('metric-queued-tasks');
  const elMetricSuccessRate = document.getElementById('metric-success-rate');
  const elMetricFailedCount = document.getElementById('metric-failed-count');
  const elMetricAvgDuration = document.getElementById('metric-avg-duration');
  const elMetricRetriedCount = document.getElementById('metric-retried-count');

  const elWorkflowSelect = document.getElementById('workflow-select');
  const elWorkflowInput = document.getElementById('workflow-input');
  const elBtnRun = document.getElementById('btn-run');
  const elInstancesList = document.getElementById('instances-list');
  const elBtnRefresh = document.getElementById('btn-refresh');

  const elActiveWorkflowTitle = document.getElementById('active-workflow-title');
  const elActiveInstanceBadge = document.getElementById('active-instance-badge');
  const elInstanceControls = document.getElementById('instance-controls');
  const elBtnPause = document.getElementById('btn-pause');
  const elBtnResume = document.getElementById('btn-resume');
  const elBtnCancel = document.getElementById('btn-cancel');

  const elDagSvg = document.getElementById('dag-svg');
  const elDagOverlay = document.getElementById('dag-nodes-overlay');
  const elGanttChart = document.getElementById('gantt-chart');
  const elTerminalLogs = document.getElementById('terminal-logs');
  const elJsonViewer = document.getElementById('json-viewer');

  // Clock
  setInterval(() => {
    const d = new Date();
    elSystemTime.textContent = d.toTimeString().split(' ')[0];
  }, 1000);

  // Tabs
  document.querySelectorAll('.tab-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.tab-btn').forEach((b) => b.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach((c) => c.classList.remove('active'));
      btn.classList.add('active');
      const targetId = btn.getAttribute('data-tab');
      document.getElementById(targetId)?.classList.add('active');
    });
  });

  // WebSocket Connection
  function initWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws`;

    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      elWsStatus.classList.add('connected');
      elWsStatus.querySelector('.label').textContent = 'LIVE STREAM';
    };

    ws.onclose = () => {
      elWsStatus.classList.remove('connected');
      elWsStatus.querySelector('.label').textContent = 'DISCONNECTED';
      setTimeout(initWebSocket, 2000); // Reconnect
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'INIT_STATE') {
          workflows = msg.workflows || [];
          instances = msg.instances || [];
          renderWorkflowDropdown();
          renderInstancesList();
          updateMetrics(msg.metrics);
          if (!selectedInstanceId && instances.length > 0) {
            selectInstance(instances[instances.length - 1].id);
          }
        } else if (msg.type === 'WORKFLOW_EVENT') {
          handleIncomingEvent(msg.event);
          if (msg.metrics) updateMetrics(msg.metrics);
        }
      } catch (e) {
        console.error('WS Parse Error:', e);
      }
    };
  }

  function updateMetrics(metrics) {
    if (!metrics) return;
    elMetricActiveWorkflows.textContent = metrics.activeWorkflows;
    elMetricTotalWorkflows.textContent = `${metrics.totalWorkflowsExecuted} total executed`;

    const wp = metrics.workerPool;
    if (wp) {
      elMetricActiveWorkers.textContent = `${wp.activeWorkers} / ${wp.totalCapacity}`;
      elMetricQueuedTasks.textContent = `${wp.queuedTasks} tasks in queue`;
      elMetricAvgDuration.textContent = `${wp.averageExecutionTimeMs}ms`;
      elMetricRetriedCount.textContent = `${wp.retriedTasks} step retries`;
    }

    const totalFinished = (metrics.successfulWorkflows || 0) + (metrics.failedWorkflows || 0);
    const successRate = totalFinished > 0
      ? Math.round((metrics.successfulWorkflows / totalFinished) * 100)
      : 100;
    elMetricSuccessRate.textContent = `${successRate}%`;
    elMetricFailedCount.textContent = `${metrics.failedWorkflows} failures recorded`;
  }

  function renderWorkflowDropdown() {
    elWorkflowSelect.innerHTML = workflows
      .map((w) => `<option value="${w.id}">${w.name} (${w.stepsCount} steps)</option>`)
      .join('');
  }

  function renderInstancesList() {
    if (instances.length === 0) {
      elInstancesList.innerHTML = '<div class="empty-hint">No executions yet. Run a workflow above.</div>';
      return;
    }

    elInstancesList.innerHTML = instances
      .slice()
      .reverse()
      .map((inst) => {
        const isSelected = inst.id === selectedInstanceId;
        const badgeClass = `badge-${inst.status.toLowerCase()}`;
        const time = new Date(inst.startTime).toLocaleTimeString();
        return `
          <div class="instance-card ${isSelected ? 'active' : ''}" data-id="${inst.id}">
            <div class="instance-card-header">
              <span class="instance-card-title">${inst.workflowName}</span>
              <span class="badge ${badgeClass}">${inst.status}</span>
            </div>
            <div class="instance-card-meta">
              <span>${inst.id.slice(0, 8)}...</span>
              <span>${inst.durationMs ? inst.durationMs + 'ms' : time}</span>
            </div>
          </div>
        `;
      })
      .join('');

    elInstancesList.querySelectorAll('.instance-card').forEach((card) => {
      card.addEventListener('click', () => {
        const id = card.getAttribute('data-id');
        selectInstance(id);
      });
    });
  }

  async function selectInstance(instanceId) {
    selectedInstanceId = instanceId;
    renderInstancesList();

    try {
      const res = await fetch(`/api/instances/${instanceId}`);
      if (!res.ok) return;
      const instance = await res.json();
      renderActiveInstance(instance);
    } catch (e) {
      console.error('Fetch instance failed', e);
    }
  }

  function renderActiveInstance(instance) {
    if (!instance) return;

    elActiveWorkflowTitle.textContent = `${instance.workflowName} (${instance.id.slice(0, 8)})`;
    elActiveInstanceBadge.className = `badge badge-${instance.status.toLowerCase()}`;
    elActiveInstanceBadge.textContent = instance.status;

    // Controls
    elInstanceControls.style.display = 'flex';
    if (instance.status === 'RUNNING') {
      elBtnPause.style.display = 'inline-flex';
      elBtnResume.style.display = 'none';
      elBtnCancel.style.display = 'inline-flex';
    } else if (instance.status === 'PAUSED') {
      elBtnPause.style.display = 'none';
      elBtnResume.style.display = 'inline-flex';
      elBtnCancel.style.display = 'inline-flex';
    } else {
      elBtnPause.style.display = 'none';
      elBtnResume.style.display = 'none';
      elBtnCancel.style.display = 'none';
    }

    renderDAG(instance);
    renderGantt(instance);
    elJsonViewer.textContent = JSON.stringify(
      {
        instanceId: instance.id,
        status: instance.status,
        durationMs: instance.durationMs,
        initialInput: instance.initialInput,
        stepOutputs: instance.stepOutputs,
        error: instance.error,
      },
      null,
      2
    );
  }

  function renderDAG(instance) {
    elDagSvg.innerHTML = `
      <defs>
        <marker id="arrow" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
          <path d="M 0 0 L 10 5 L 0 10 z" fill="#38bdf8" />
        </marker>
      </defs>
    `;
    elDagOverlay.innerHTML = '';

    const steps = Object.values(instance.stepStates || {});
    if (steps.length === 0) return;

    // Compute topological layers for layout
    const inDegree = {};
    const adj = {};
    steps.forEach((s) => {
      inDegree[s.stepId] = (s.dependencies || []).length;
      (s.dependencies || []).forEach((dep) => {
        if (!adj[dep]) adj[dep] = [];
        adj[dep].push(s.stepId);
      });
    });

    const layers = [];
    let currentLayer = steps.filter((s) => inDegree[s.stepId] === 0).map((s) => s.stepId);
    const visited = new Set();

    while (currentLayer.length > 0) {
      layers.push(currentLayer);
      currentLayer.forEach((id) => visited.add(id));
      const nextLayer = [];
      currentLayer.forEach((nodeId) => {
        (adj[nodeId] || []).forEach((childId) => {
          if (!visited.has(childId) && !nextLayer.includes(childId)) {
            // Check all parents visited
            const stepObj = instance.stepStates[childId];
            const allParentsDone = (stepObj?.dependencies || []).every((p) => visited.has(p));
            if (allParentsDone) {
              nextLayer.push(childId);
            }
          }
        });
      });
      currentLayer = nextLayer;
    }

    // Add any remaining
    steps.forEach((s) => {
      if (!visited.has(s.stepId)) {
        layers.push([s.stepId]);
        visited.add(s.stepId);
      }
    });

    // Layout coordinates
    const nodeCoords = {};
    const layerSpacingX = 240;
    const nodeSpacingY = 90;
    const startX = 40;
    const startY = 40;

    layers.forEach((layer, layerIdx) => {
      const x = startX + layerIdx * layerSpacingX;
      layer.forEach((stepId, nodeIdx) => {
        const y = startY + nodeIdx * nodeSpacingY;
        nodeCoords[stepId] = { x, y };

        const step = instance.stepStates[stepId];
        const nodeEl = document.createElement('div');
        nodeEl.className = `dag-node status-${step.status}`;
        nodeEl.style.left = `${x}px`;
        nodeEl.style.top = `${y}px`;

        const durationText = step.durationMs ? `${step.durationMs}ms` : (step.status === 'RUNNING' ? 'running...' : '');
        const retryText = step.attempt > 1 ? ` • retry #${step.attempt}` : '';

        nodeEl.innerHTML = `
          <div class="dag-node-header">
            <span class="dag-node-title" title="${step.name}">${step.name}</span>
            <span class="badge badge-${step.status.toLowerCase()}">${step.status}</span>
          </div>
          <div class="dag-node-details">${durationText}${retryText}</div>
        `;

        nodeEl.addEventListener('click', () => {
          elJsonViewer.textContent = JSON.stringify(step, null, 2);
        });

        elDagOverlay.appendChild(nodeEl);
      });
    });

    // Draw connecting edges
    steps.forEach((step) => {
      (step.dependencies || []).forEach((depId) => {
        const from = nodeCoords[depId];
        const to = nodeCoords[step.stepId];
        if (from && to) {
          const startX = from.x + 180;
          const startY = from.y + 25;
          const endX = to.x;
          const endY = to.y + 25;
          const cX1 = startX + 30;
          const cX2 = endX - 30;

          const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
          path.setAttribute('d', `M ${startX} ${startY} C ${cX1} ${startY}, ${cX2} ${endY}, ${endX} ${endY}`);
          path.setAttribute('fill', 'none');
          path.setAttribute('stroke', '#38bdf8');
          path.setAttribute('stroke-width', '2');
          path.setAttribute('marker-end', 'url(#arrow)');
          if (step.status === 'RUNNING') {
            path.setAttribute('stroke-dasharray', '5,5');
          }
          elDagSvg.appendChild(path);
        }
      });
    });
  }

  function renderGantt(instance) {
    const steps = Object.values(instance.stepStates || {}).filter((s) => s.startTime);
    if (steps.length === 0) {
      elGanttChart.innerHTML = '<div class="empty-hint">Waiting for steps to start...</div>';
      return;
    }

    const minTime = Math.min(...steps.map((s) => s.startTime));
    const now = Date.now();
    const maxTime = Math.max(...steps.map((s) => s.endTime || now), minTime + 100);
    const totalSpan = maxTime - minTime;

    elGanttChart.innerHTML = steps
      .map((step) => {
        const startOffset = ((step.startTime - minTime) / totalSpan) * 100;
        const endTime = step.endTime || now;
        const width = Math.max(3, ((endTime - step.startTime) / totalSpan) * 100);
        const duration = step.durationMs ? `${step.durationMs}ms` : `${endTime - step.startTime}ms`;

        return `
          <div class="gantt-row">
            <span class="gantt-label" title="${step.name}">${step.name}</span>
            <div class="gantt-track">
              <div class="gantt-bar" style="left: ${startOffset}%; width: ${width}%;"></div>
            </div>
            <span class="gantt-duration">${duration}</span>
          </div>
        `;
      })
      .join('');
  }

  function handleIncomingEvent(evt) {
    eventsLog.unshift(evt);
    if (eventsLog.length > 200) eventsLog.pop();

    // Append to live terminal logs
    const time = new Date(evt.timestamp).toTimeString().split(' ')[0];
    const logEl = document.createElement('div');
    logEl.className = 'log-entry';
    logEl.innerHTML = `
      <span class="log-time">[${time}]</span>
      <span class="log-type">${evt.type}</span>
      <span class="log-msg">${evt.stepId ? `[${evt.stepId}] ` : ''}${JSON.stringify(evt.payload || '')}</span>
    `;
    elTerminalLogs.insertBefore(logEl, elTerminalLogs.firstChild);

    // Refresh active instance if matching
    if (evt.workflowInstanceId === selectedInstanceId) {
      selectInstance(selectedInstanceId);
    } else {
      // Refresh instances list in background
      fetch('/api/instances')
        .then((r) => r.json())
        .then((data) => {
          instances = data.instances || [];
          renderInstancesList();
        });
    }
  }

  // Trigger Button Click
  elBtnRun.addEventListener('click', async () => {
    const workflowId = elWorkflowSelect.value;
    if (!workflowId) return alert('Please select a workflow');

    let payload = {};
    try {
      payload = JSON.parse(elWorkflowInput.value || '{}');
    } catch {
      return alert('Invalid JSON payload');
    }

    elBtnRun.disabled = true;
    elBtnRun.innerHTML = 'Submitting...';

    try {
      const res = await fetch(`/api/workflows/${encodeURIComponent(workflowId)}/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      const data = await res.json();
      if (data.instanceId) {
        selectInstance(data.instanceId);
      }
    } catch (e) {
      alert('Failed to trigger workflow: ' + e.message);
    } finally {
      elBtnRun.disabled = false;
      elBtnRun.innerHTML = '<span class="btn-icon">▶</span> Run Workflow';
    }
  });

  // Controls Handlers
  elBtnPause.addEventListener('click', async () => {
    if (!selectedInstanceId) return;
    await fetch(`/api/instances/${selectedInstanceId}/pause`, { method: 'POST' });
    selectInstance(selectedInstanceId);
  });

  elBtnResume.addEventListener('click', async () => {
    if (!selectedInstanceId) return;
    await fetch(`/api/instances/${selectedInstanceId}/resume`, { method: 'POST' });
    selectInstance(selectedInstanceId);
  });

  elBtnCancel.addEventListener('click', async () => {
    if (!selectedInstanceId) return;
    if (confirm('Cancel this workflow execution?')) {
      await fetch(`/api/instances/${selectedInstanceId}/cancel`, { method: 'POST' });
      selectInstance(selectedInstanceId);
    }
  });

  elBtnRefresh.addEventListener('click', () => {
    fetch('/api/instances')
      .then((r) => r.json())
      .then((data) => {
        instances = data.instances || [];
        renderInstancesList();
      });
  });

  // Start WebSocket
  initWebSocket();
})();
