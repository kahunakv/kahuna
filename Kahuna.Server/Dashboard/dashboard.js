/*
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * The dashboard's poll loop. Plain JavaScript, no framework, no bundler.
 *
 * Four rules shape everything below:
 *
 *   1. Each panel owns its own timer and its own failure. One panel that cannot
 *      load must never blank the page or stop its neighbours.
 *   2. A hidden tab polls nothing. A dashboard left open on a second monitor
 *      overnight must not keep asking a production node for numbers nobody is
 *      reading.
 *   3. Rates are computed here, not on the server. The collector accumulates
 *      from process start and never resets, and the server holds no previous
 *      sample, so only the browser can subtract two readings. The division uses
 *      the served monotonic clock: a wall clock can step, and a rate measured
 *      across a step is wrong.
 *   4. Nothing is stored but the theme.
 * ─────────────────────────────────────────────────────────────────────────────
 */

(function () {
  'use strict';

  var MAX_BACKOFF_MS = 30000;
  var DEFAULT_REFRESH_MS = 5000;

  // Shortest window a rate may be measured over. Returning to a hidden tab
  // polls immediately, which can land milliseconds after a scheduled poll; a
  // counter that moved by a handful over 30ms divides out to a rate in the
  // thousands and reads as a spike that never happened. Below this the reading
  // is discarded and the previous one is kept as the baseline, so the next poll
  // measures a real interval rather than restarting from a useless one.
  var MIN_RATE_WINDOW_SECONDS = 0.25;

  // ── Theme ────────────────────────────────────────────────────────────────
  // Stamps data-theme on the root element, the same mechanism the documentation
  // site uses. The initial stamp happens in an inline script in the page so it
  // never paints the wrong theme first; this only wires the toggle.

  function saveTheme(value) {
    // A private window can throw on write. A theme that does not persist is a
    // far smaller problem than a dashboard that fails to load.
    try { localStorage.setItem('kahuna-theme', value); } catch (e) { /* ignore */ }
  }

  function currentTheme() {
    var stamped = document.documentElement.getAttribute('data-theme');
    if (stamped) return stamped;
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }

  function wireTheme() {
    var button = document.getElementById('theme-toggle');
    if (!button) return;

    function label() {
      button.textContent = currentTheme() === 'dark' ? 'Light theme' : 'Dark theme';
    }

    label();
    button.addEventListener('click', function () {
      var next = currentTheme() === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', next);
      saveTheme(next);
      label();
    });
  }

  // ── Fetch ────────────────────────────────────────────────────────────────

  function getJson(url) {
    return fetch(url, {
      credentials: 'same-origin',
      headers: { 'Accept': 'application/json' },
    }).then(function (response) {
      return response.json()
        .catch(function () { return {}; })
        .then(function (body) {
          // A node that is not ready answers 503 on the readiness route and a
          // node with no backup root answers 503 on the catalog. Both carry a
          // usable body, so the status alone must not discard it — the caller
          // decides, panel by panel, whether this status is a failure.
          return { ok: response.ok, status: response.status, body: body };
        });
    });
  }

  // ── Formatting ───────────────────────────────────────────────────────────

  function text(value) {
    return value === null || value === undefined ? '' : String(value);
  }

  function escapeHtml(value) {
    return text(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  /** Compact above four digits, so a rate never widens the column it sits in. */
  function num(value, decimals) {
    if (value === null || value === undefined || !isFinite(value)) return '—';

    var abs = Math.abs(value);
    if (abs >= 1e9) return (value / 1e9).toFixed(2) + 'G';
    if (abs >= 1e6) return (value / 1e6).toFixed(2) + 'M';
    if (abs >= 1e4) return (value / 1e3).toFixed(1) + 'k';

    var places = decimals === undefined ? (abs >= 100 || value === Math.round(value) ? 0 : 1) : decimals;
    return value.toFixed(places);
  }

  function bytes(value) {
    if (value === null || value === undefined || !isFinite(value)) return '—';

    var units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
    var index = 0;

    while (Math.abs(value) >= 1024 && index < units.length - 1) {
      value = value / 1024;
      index++;
    }

    return (index === 0 ? value.toFixed(0) : value.toFixed(1)) + ' ' + units[index];
  }

  function duration(seconds) {
    if (seconds === null || seconds === undefined || !isFinite(seconds)) return '—';

    var s = Math.max(0, Math.floor(seconds));
    var days = Math.floor(s / 86400);
    var hours = Math.floor((s % 86400) / 3600);
    var minutes = Math.floor((s % 3600) / 60);

    if (days > 0) return days + 'd ' + hours + 'h';
    if (hours > 0) return hours + 'h ' + minutes + 'm';
    if (minutes > 0) return minutes + 'm ' + (s % 60) + 's';
    return s + 's';
  }

  function clockTime(unixMs) {
    if (!unixMs) return '';
    var d = new Date(unixMs);
    return d.toLocaleTimeString();
  }

  /**
   * Marks where a long unbroken identifier may wrap. A metric name and an
   * endpoint carry no spaces, so without this the browser either overflows the
   * column or breaks mid-token — `batch_ite / ms` reads as a different name than
   * the one the node published. The breaks are offered at the separators the
   * reader already parses on, so a wrapped name still reads correctly.
   */
  function breakable(value) {
    return escapeHtml(value).replace(/([._:/-])/g, '$1<wbr />');
  }

  /** A dot is always rendered beside its word — state is never color alone. */
  function state(kind, word) {
    return '<span class="state"><span class="dot dot-' + kind + '"></span>' + escapeHtml(word) + '</span>';
  }

  function notice(title, detail) {
    return '<div class="notice-box"><div><strong>' + escapeHtml(title) + '</strong>' +
      (detail ? escapeHtml(detail) : '') + '</div></div>';
  }

  function table(headers, rows) {
    if (!rows.length) return '';

    var head = headers.map(function (h) {
      return '<th' + (h.right ? ' class="r"' : '') + '>' + escapeHtml(h.label) + '</th>';
    }).join('');

    return '<table><thead><tr>' + head + '</tr></thead><tbody>' + rows.join('') + '</tbody></table>';
  }

  // ── Panels ───────────────────────────────────────────────────────────────

  /**
   * One self-refreshing panel.
   *
   * `render` receives the payload and returns HTML for `element`. A renderer
   * that writes several regions of the page itself returns null instead, and
   * `element` then serves only as the place a failure is reported. If a
   * renderer throws, the panel degrades exactly as a network failure does, so a
   * malformed payload cannot take the page down.
   */
  function Panel(options) {
    this.url = options.url;
    this.intervalMs = options.intervalMs || DEFAULT_REFRESH_MS;
    this.render = options.render;
    this.element = options.element;
    this.accepts = options.accepts || null;
    this.onPayload = options.onPayload || null;
    this.timer = null;
    this.backoffMs = 0;
    this.stopped = false;
    this.lastGoodAt = null;
  }

  Panel.prototype.degrade = function (title, detail) {
    if (!this.element) return;

    var age = '';
    if (this.lastGoodAt !== null) {
      age = ' Last read ' + duration((Date.now() - this.lastGoodAt) / 1000) + ' ago.';
    }

    this.element.innerHTML = notice(title, (detail || '') + age);
  };

  Panel.prototype.poll = function () {
    var self = this;
    if (self.stopped || !self.element) return Promise.resolve();

    return getJson(self.url).then(function (result) {
      // A panel decides for itself which non-200 statuses carry a payload it
      // can still render — a 503 from a node that is not ready is information,
      // not a failed poll.
      var usable = result.ok || (self.accepts && self.accepts.indexOf(result.status) !== -1);

      if (!usable) {
        self.degrade('This panel could not be read.', 'The node answered ' + result.status + '.');
        return;
      }

      var html = self.render(result.body, result.status);

      if (html !== null && html !== undefined) {
        self.element.innerHTML = html;
      }

      self.lastGoodAt = Date.now();
      self.backoffMs = 0;

      if (self.onPayload) self.onPayload(result.body, result.status);
    }).catch(function (error) {
      self.backoffMs = Math.min(self.backoffMs ? self.backoffMs * 2 : self.intervalMs, MAX_BACKOFF_MS);
      self.degrade('This panel could not be read.',
        (error && error.message ? error.message + '.' : 'The request failed.'));
    });
  };

  Panel.prototype.schedule = function () {
    var self = this;
    if (self.stopped || self.timer !== null) return;

    self.timer = window.setTimeout(function () {
      self.timer = null;

      // A hidden tab skips the request but keeps the timer running, so the
      // panel is current within one interval of the tab coming back.
      if (document.hidden) {
        self.schedule();
        return;
      }

      self.poll().then(function () { self.schedule(); });
    }, self.backoffMs || self.intervalMs);
  };

  Panel.prototype.start = function () {
    var self = this;
    self.poll().then(function () { self.schedule(); });
  };

  Panel.prototype.retune = function (intervalMs) {
    if (intervalMs > 0) this.intervalMs = intervalMs;
  };

  // ── Node band ────────────────────────────────────────────────────────────

  function renderBandInto(summary) {
    var head = document.getElementById('band-head');
    var meta = document.getElementById('band-meta');
    var strip = document.getElementById('band-strip');

    var readiness = summary.ready
      ? state('ok', 'Ready')
      : (summary.initialized ? state('warn', 'Not serving') : state('warn', 'Initializing'));

    // NotMember is the one role that is a fault rather than a phase: the node
    // was evicted from the roster and no amount of waiting brings it back.
    var roleKind = summary.localRole === 'NotMember' ? 'bad'
      : (summary.localRole === 'Leaving' ? 'warn' : 'info');

    head.innerHTML =
      '<h1>' + escapeHtml(summary.localEndpoint || summary.nodeName || 'this node') + '</h1>' +
      readiness +
      state(roleKind, summary.localRole || 'unknown') +
      '<span class="pill">' + (summary.clusterMode ? 'Cluster' : 'Standalone') + '</span>';

    var parts = [];
    parts.push('<span>Node <b>' + escapeHtml(summary.nodeName) + '</b></span>');
    parts.push('<span>Version <b>' + escapeHtml(summary.version) + '</b></span>');
    parts.push('<span>Roster <b>' + summary.memberCount + '</b> at version <b>' +
      summary.membershipVersion + '</b></span>');
    parts.push('<span>Replication factor <b>' +
      (summary.replicationFactor === 0 ? 'full' : summary.replicationFactor) + '</b></span>');

    if (summary.storagePath) {
      parts.push('<span>Data <code>' + escapeHtml(summary.storagePath) + '</code></span>');
    }
    if (summary.walPath) {
      parts.push('<span>WAL <code>' + escapeHtml(summary.walPath) + '</code></span>');
    }

    meta.innerHTML = parts.join('');

    var hosted = summary.hostedPartitions;
    var total = summary.totalPartitions;

    strip.innerHTML =
      cell('Partitions hosted', hosted + (total ? ' / ' + total : ''),
        total ? 'of ' + total + ' in the map' : 'map not applied yet') +
      cell('Uptime', duration(summary.uptimeSeconds), 'since process start') +
      cell('Storage', summary.storage,
        'WAL on ' + text(summary.walStorage)) +
      cell('Managed heap', bytes(summary.heapBytes),
        summary.threadCount ? summary.threadCount + ' threads' : '');
  }

  function cell(label, value, footnote) {
    return '<div><dt>' + escapeHtml(label) + '</dt><dd>' + escapeHtml(value) + '</dd>' +
      (footnote ? '<small>' + escapeHtml(footnote) + '</small>' : '') + '</div>';
  }

  // ── Engine metrics ───────────────────────────────────────────────────────

  // What the panel shows, in the order it shows it. `how` decides the reading:
  //
  //   rate  — a counter's change per second between two readings
  //   mean  — a histogram's mean over the window between two readings, which
  //           says what the node is doing now rather than since it started
  //   total — a counter's running total, for things that should stay at zero
  //   gauge — the instant value, summed across tags (one row per partition)
  //
  var GROUPS = [
    {
      title: 'Key-value writes',
      items: [
        { metric: 'kahuna.kv.write.admitted',      label: 'Writes admitted',    how: 'rate',  unit: '/s' },
        { metric: 'kahuna.kv.write.batches',       label: 'Raft batches',       how: 'rate',  unit: '/s' },
        { metric: 'kahuna.kv.write.entries',       label: 'Entries replicated', how: 'rate',  unit: '/s' },
        { metric: 'kahuna.kv.write.batch_items',   label: 'Entries per batch',  how: 'mean',  unit: 'avg' },
        { metric: 'kahuna.kv.write.batch_bytes',   label: 'Batch size',         how: 'mean',  unit: 'avg', format: 'bytes' },
        { metric: 'kahuna.kv.write.queue_age',     label: 'Queue age',          how: 'mean',  unit: 'ms avg' },
        { metric: 'kahuna.kv.write.raft_duration', label: 'Raft call',          how: 'mean',  unit: 'ms avg' },
        { metric: 'kahuna.kv.write.rejections',    label: 'Write rejections',   how: 'total', unit: 'total' },
      ],
    },
    {
      title: 'Consensus',
      items: [
        { metric: 'raft.executor.operations_total',       label: 'Executor operations', how: 'rate',  unit: '/s' },
        { metric: 'raft.executor.operation_duration_ms',  label: 'Executor dispatch',   how: 'mean',  unit: 'ms avg' },
        { metric: 'raft.executor.client_queue_depth',     label: 'Client queue depth',  how: 'gauge', unit: 'now' },
        { metric: 'raft.wal.operations_total',            label: 'WAL operations',      how: 'rate',  unit: '/s' },
        { metric: 'raft.wal.batches_total',               label: 'WAL batches',         how: 'rate',  unit: '/s' },
        { metric: 'raft.wal.batch_size',                  label: 'WAL batch size',      how: 'mean',  unit: 'avg' },
        { metric: 'raft.wal.queue_depth',                 label: 'WAL queue depth',     how: 'gauge', unit: 'now' },
        { metric: 'raft.heartbeat_delay_ms',              label: 'Heartbeat interval',  how: 'mean',  unit: 'ms avg' },
      ],
    },
    {
      title: 'Should stay at zero',
      items: [
        { metric: 'raft.elections_started_total',                 label: 'Elections started',   how: 'total', unit: 'total' },
        { metric: 'raft.stale_completions_total',                 label: 'Stale completions',   how: 'total', unit: 'total' },
        { metric: 'raft.snapshot.transfer_failures_total',        label: 'Snapshot failures',   how: 'total', unit: 'total' },
        { metric: 'raft.backfill.no_progress_episodes_total',     label: 'Backfill stalls',     how: 'total', unit: 'total' },
        { metric: 'kahuna.placement.forwards_unresolved',         label: 'Forwards unresolved', how: 'total', unit: 'total' },
        { metric: 'kahuna.scan.abandoned_cancelled_total',        label: 'Scans abandoned',     how: 'total', unit: 'total' },
      ],
    },
    {
      title: 'Placement and scans',
      items: [
        { metric: 'kahuna.placement.forwards_resolved',                    label: 'Forwards resolved',    how: 'rate',  unit: '/s' },
        { metric: 'kahuna.placement.leader_hint_hits',                     label: 'Leader hint hits',     how: 'total', unit: 'total' },
        { metric: 'kahuna.placement.leader_hint_misses',                   label: 'Leader hint misses',   how: 'total', unit: 'total' },
        { metric: 'kahuna.scan.snapshot_prefix_rows_examined_total',       label: 'Scan rows examined',   how: 'rate',  unit: '/s' },
        { metric: 'kahuna.scan.snapshot_prefix_entries_returned_total',    label: 'Scan rows returned',   how: 'rate',  unit: '/s' },
      ],
    },
  ];

  // The previous reading, per metric. Rates and windowed means are the
  // difference between this and the last, so the very first reading of any
  // instrument can only report a total.
  var previous = {};

  /**
   * Folds the served rows into one entry per metric. An instrument tagged by
   * partition arrives as many rows; the panel is a node-level summary, so
   * counters and counts add, and min/max widen.
   */
  function foldByMetric(rows) {
    var folded = {};

    for (var i = 0; i < rows.length; i++) {
      var row = rows[i];
      var entry = folded[row.metric];

      if (!entry) {
        entry = folded[row.metric] = {
          kind: row.kind, count: 0, total: 0, last: 0,
          min: null, max: null, hasTotal: false, hasLast: false,
        };
      }

      entry.count += row.count || 0;

      if (row.total !== null && row.total !== undefined) {
        entry.total += row.total;
        entry.hasTotal = true;
      }

      if (row.last !== null && row.last !== undefined) {
        // A gauge is summed across tags: the client queue depth of a node is
        // the depth of all its partitions, not of whichever one sorted last.
        entry.last += row.last;
        entry.hasLast = true;
      }

      if (row.min !== null && row.min !== undefined) {
        entry.min = entry.min === null ? row.min : Math.min(entry.min, row.min);
      }

      if (row.max !== null && row.max !== undefined) {
        entry.max = entry.max === null ? row.max : Math.max(entry.max, row.max);
      }
    }

    return folded;
  }

  /**
   * True when an instrument's absence from the payload can be read as zero.
   *
   * A counter only ever increments, so one that has never fired has counted
   * nothing — reporting zero is the truth. A gauge or a histogram is different:
   * absence means nobody sampled it, which is not the same fact as a measured
   * zero, and the two must not be conflated on a page an operator trusts.
   *
   * The cap is the one thing that breaks the inference. If rows were dropped,
   * a missing counter might have been dropped rather than never fired, so
   * nothing is claimed.
   */
  function absenceMeansZero(item, capped) {
    return !capped && (item.how === 'total' || item.how === 'rate');
  }

  function readValue(item, entry, prior, elapsedSeconds) {
    if (!entry) return null;

    if (item.how === 'gauge') {
      return entry.hasLast ? entry.last : null;
    }

    if (item.how === 'total') {
      return entry.hasTotal ? entry.total : entry.count;
    }

    if (item.how === 'rate') {
      if (!prior || !elapsedSeconds) return null;

      var delta = entry.total - prior.total;
      // A counter cannot fall. A negative delta means the process restarted
      // under this open tab, so the window is meaningless and reports nothing
      // rather than a wrong number.
      return delta < 0 ? null : delta / elapsedSeconds;
    }

    // A windowed mean says what the node is doing now. Falling back to the
    // mean since process start on the first reading is better than a blank
    // cell, and converges to the window's answer one poll later.
    if (prior && entry.count > prior.count) {
      return (entry.total - prior.total) / (entry.count - prior.count);
    }

    return entry.count > 0 ? entry.total / entry.count : null;
  }

  function renderMetrics(payload) {
    var rows = payload.rows || [];

    if (!rows.length) {
      return notice('No instrument has recorded a sample yet.',
        ' Counters appear once the node serves its first request.');
    }

    var folded = foldByMetric(rows);
    var elapsedSeconds = 0;

    if (previous.monotonicMs && payload.monotonicMs > previous.monotonicMs) {
      elapsedSeconds = (payload.monotonicMs - previous.monotonicMs) / 1000;
    }

    var usableWindow = elapsedSeconds >= MIN_RATE_WINDOW_SECONDS;

    if (!usableWindow) {
      elapsedSeconds = 0;
    }

    var html = '<div class="metric-cols">';

    for (var g = 0; g < GROUPS.length; g++) {
      var group = GROUPS[g];
      var body = '';

      for (var i = 0; i < group.items.length; i++) {
        var item = group.items[i];
        var entry = folded[item.metric];
        var shown;

        if (entry) {
          var value = readValue(item, entry, previous.metrics && previous.metrics[item.metric], elapsedSeconds);
          shown = value === null ? '—' : (item.format === 'bytes' ? bytes(value) : num(value));
        } else if (absenceMeansZero(item, payload.omitted > 0)) {
          // A counter that has never fired counted nothing. Showing the zero is
          // the point of the "should stay at zero" group: an empty card cannot
          // be told apart from a card that failed to load.
          shown = '0';
        } else {
          // Never sampled. Dropping the row is the honest answer — a zero this
          // node never measured is the more dangerous of the two to display.
          continue;
        }

        body +=
          '<div class="metric">' +
            '<div class="name">' + escapeHtml(item.label) +
              '<small>' + breakable(item.metric) + '</small>' +
            '</div>' +
            '<div class="val">' + escapeHtml(shown) +
              '<u>' + escapeHtml(item.unit) + '</u>' +
            '</div>' +
          '</div>';
      }

      if (body) {
        html += '<div><p class="metric-group">' + escapeHtml(group.title) + '</p>' + body + '</div>';
      }
    }

    html += '</div>';

    if (payload.omitted) {
      html += '<p class="stale">' + payload.omitted + ' further rows were not sent: the payload cap was reached.</p>';
    }

    // Keep the old baseline when the window was too short to divide by, so the
    // next poll measures against a reading far enough back to mean something.
    if (usableWindow || !previous.monotonicMs) {
      previous = { metrics: folded, monotonicMs: payload.monotonicMs };
    }

    var when = document.getElementById('metrics-when');
    if (when) when.textContent = 'Sampled ' + clockTime(payload.sampledAtUnixMs);

    return html;
  }

  // ── Roster ───────────────────────────────────────────────────────────────

  function renderRoster(payload) {
    var members = payload.members || [];

    if (!members.length) {
      return notice('This node reports no roster yet.',
        ' Membership arrives when cluster initialization completes.');
    }

    var version = document.getElementById('roster-version');
    if (version) {
      version.textContent = 'Membership version ' + payload.membershipVersion +
        (payload.initialized ? '' : ' — this node has not finished initializing.');
    }

    var rows = members.map(function (m) {
      var kind = m.role === 'NotMember' ? 'bad' : (m.role === 'Leaving' ? 'warn' : 'ok');
      var isLocal = m.endpoint === payload.localEndpoint;

      return '<tr>' +
        '<td class="key' + (isLocal ? ' here' : '') + '">' + breakable(m.endpoint) +
          (isLocal ? ' <span class="dim">(this node)</span>' : '') + '</td>' +
        '<td>' + state(kind, m.role) + '</td>' +
        '<td class="r num dim">' + m.joinedVersion + '</td>' +
      '</tr>';
    });

    return table(
      [{ label: 'Endpoint' }, { label: 'Role' }, { label: 'Joined at', right: true }],
      rows);
  }

  // ── Placement ────────────────────────────────────────────────────────────

  function renderPlacement(payload) {
    var partitions = payload.partitions || [];

    var mode = document.getElementById('placement-mode');
    if (mode) {
      mode.textContent = (payload.replicationFactor === 0
        ? 'Full replication'
        : 'Replication factor ' + payload.replicationFactor) +
        (payload.rebalancerEnabled ? ' · rebalancer on' : ' · rebalancer off');
    }

    if (!partitions.length) {
      return notice('This node has applied no partition map yet.',
        ' The map arrives when cluster initialization completes.');
    }

    var rows = partitions.map(function (p) {
      var replicas = (p.replicas || []).map(function (r) {
        return breakable(r.endpoint) + ' <span class="dim">' + escapeHtml(r.role) + '</span>';
      }).join('<br />');

      return '<tr>' +
        '<td class="num">' + p.partitionId + '</td>' +
        '<td>' + (p.hostedLocally ? state('ok', 'hosted here') : state('idle', 'forwarded')) + '</td>' +
        '<td>' + escapeHtml(p.state) + '</td>' +
        '<td class="key">' + (replicas || '<span class="dim">every node</span>') + '</td>' +
        '<td class="r num dim">' + p.effectiveReplicationFactor + '</td>' +
        '<td class="r num dim">' + p.generation + '</td>' +
      '</tr>';
    });

    return table([
      { label: 'Partition' },
      { label: 'Locality' },
      { label: 'State' },
      { label: 'Replicas' },
      { label: 'RF', right: true },
      { label: 'Generation', right: true },
    ], rows);
  }

  // ── Key ranges ───────────────────────────────────────────────────────────

  function bound(value, open) {
    if (value === null || value === undefined) {
      return '<span class="bound open">' + open + '</span>';
    }
    if (value === '') {
      return '<span class="bound open">empty string</span>';
    }
    return '<span class="bound">' + breakable(value) + '</span>';
  }

  function renderRanges(payload) {
    var spaces = payload.keySpaces || [];

    var when = document.getElementById('ranges-when');
    if (when) {
      when.textContent = payload.initialized
        ? spaces.length + (spaces.length === 1 ? ' key space' : ' key spaces')
        : 'Not initialized — an empty list means "not known yet".';
    }

    if (!spaces.length) {
      return notice('No key space is under key-range routing on this node.',
        ' Every key space this node serves is routed by hash, which is the default.');
    }

    var rows = [];

    for (var i = 0; i < spaces.length; i++) {
      var space = spaces[i];
      var descriptors = space.descriptors || [];

      if (!descriptors.length) {
        rows.push('<tr>' +
          '<td class="key">' + breakable(space.keySpace) + '</td>' +
          '<td>' + escapeHtml(space.routingMode) + '</td>' +
          '<td colspan="4" class="dim">Registered, but no descriptor has been seeded yet.</td>' +
        '</tr>');
        continue;
      }

      for (var d = 0; d < descriptors.length; d++) {
        var descriptor = descriptors[d];

        rows.push('<tr>' +
          '<td class="key">' + (d === 0 ? breakable(space.keySpace) : '') + '</td>' +
          '<td>' + (d === 0 ? escapeHtml(space.routingMode) : '') + '</td>' +
          '<td>' + bound(descriptor.startKey, '−infinity') + '</td>' +
          '<td>' + bound(descriptor.endKey, '+infinity') + '</td>' +
          '<td class="r num">' + descriptor.partitionId + '</td>' +
          '<td class="r num dim">' + descriptor.generation + '</td>' +
        '</tr>');
      }
    }

    return table([
      { label: 'Key space' },
      { label: 'Routing' },
      { label: 'From (inclusive)' },
      { label: 'To (exclusive)' },
      { label: 'Partition', right: true },
      { label: 'Generation', right: true },
    ], rows);
  }

  // ── Backups ──────────────────────────────────────────────────────────────

  function renderBackups(payload, status) {
    if (status === 503) {
      return notice('Backup is not configured on this node.',
        ' Configure a backup root to build a catalog.');
    }

    var backups = Array.isArray(payload) ? payload : (payload.backups || []);

    if (!backups.length) {
      return notice('This node holds no backups.',
        ' The catalog fills once a full backup is taken.');
    }

    // Newest first: the one an operator reaches for is the most recent.
    backups.sort(function (a, b) {
      return String(b.createdAtUtc).localeCompare(String(a.createdAtUtc));
    });

    var rows = backups.slice(0, 25).map(function (b) {
      var health = b.isInvalid ? state('bad', 'invalid')
        : (b.isIncomplete ? state('warn', 'incomplete') : state('ok', 'complete'));

      return '<tr>' +
        '<td>' + escapeHtml(b.type) + '</td>' +
        '<td>' + health + '</td>' +
        '<td class="dim">' + escapeHtml(new Date(b.createdAtUtc).toLocaleString()) + '</td>' +
        '<td class="r num dim">' + b.partitionCount + '</td>' +
      '</tr>';
    });

    var html = table([
      { label: 'Kind' },
      { label: 'State' },
      { label: 'Taken' },
      { label: 'Partitions', right: true },
    ], rows);

    if (backups.length > 25) {
      html += '<p class="stale" style="padding: .6rem 1rem;">' +
        (backups.length - 25) + ' older backups are not listed.</p>';
    }

    return html;
  }

  // ── Wiring ───────────────────────────────────────────────────────────────

  function start() {
    wireTheme();

    var backups = new Panel({
      url: '/v1/backups',
      element: document.getElementById('backups-body'),
      // A node with no backup root answers 503 with a usable body. That is a
      // configuration state, not a failed poll, so the panel renders it rather
      // than reporting the node as unreadable.
      accepts: [503],
      render: renderBackups,
    });

    // The range map changes only on a split or a merge, which is far rarer than
    // a metric moves. Polling it as often would be pure waste, and the same
    // reasoning stretches the backup catalog further still.
    var slower = { ranges: 4, backups: 12 };

    var ranges = new Panel({
      url: '/v1/ranges',
      element: document.getElementById('ranges-body'),
      render: renderRanges,
    });

    var metrics = new Panel({
      url: '/v1/dashboard/metrics',
      element: document.getElementById('metrics-body'),
      render: renderMetrics,
    });

    var roster = new Panel({
      url: '/v1/cluster/membership',
      element: document.getElementById('roster-body'),
      render: renderRoster,
    });

    var placement = new Panel({
      url: '/v1/cluster/placement',
      element: document.getElementById('placement-body'),
      render: renderPlacement,
    });

    // The band is the one panel that writes several regions of the page, so its
    // renderer returns null and does the writing itself. It also carries the
    // refresh interval every other panel is meant to use, which is why it holds
    // the reference to them.
    var band = new Panel({
      url: '/v1/dashboard/summary',
      element: document.getElementById('band-strip'),
      render: function (summary) {
        renderBandInto(summary);

        var interval = (summary.refreshSeconds || 5) * 1000;

        band.retune(interval);
        metrics.retune(interval);
        roster.retune(interval);
        placement.retune(interval);
        ranges.retune(interval * slower.ranges);
        backups.retune(interval * slower.backups);

        return null;
      },
    });

    var everything = [band, metrics, roster, placement, ranges, backups];

    for (var i = 0; i < everything.length; i++) {
      everything[i].intervalMs = DEFAULT_REFRESH_MS;
      everything[i].start();
    }

    ranges.intervalMs = DEFAULT_REFRESH_MS * slower.ranges;
    backups.intervalMs = DEFAULT_REFRESH_MS * slower.backups;

    // Coming back to the tab should not mean waiting out a whole interval on a
    // page whose numbers are already stale.
    document.addEventListener('visibilitychange', function () {
      if (document.hidden) return;

      for (var j = 0; j < everything.length; j++) {
        everything[j].poll();
      }
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start);
  } else {
    start();
  }
}());
