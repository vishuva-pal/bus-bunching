// app.js

let HEADWAY_SCORES = [];
let NERD_MODE = false; // global flag

/* ---------------------- Status bar helper ---------------------- */

function setStatus(message, level = "info") {
  const bar = document.getElementById("status-bar");
  const text = document.getElementById("status-text");
  if (!bar || !text) return;

  text.textContent = message;

  bar.classList.remove(
    "bg-blue-900",
    "bg-emerald-900",
    "bg-amber-900",
    "bg-red-900"
  );

  let cls = "bg-blue-900";
  if (level === "good") cls = "bg-emerald-900";
  else if (level === "warn") cls = "bg-amber-900";
  else if (level === "error") cls = "bg-red-900";

  bar.classList.add(cls);
}

/* ---------------------- Nerd mode toggle & metric copy --------- */

function applyNerdMode(on) {
  NERD_MODE = !!on;

  // Show / hide any nerd-only blocks (e.g. "How this MVP works")
  document.querySelectorAll(".nerd-only").forEach((el) => {
    if (NERD_MODE) el.classList.remove("hidden");
    else el.classList.add("hidden");
  });

  // Switch labels + helper text between rider mode and nerd mode
  const labelMedian = document.getElementById("label-median");
  const labelMean = document.getElementById("label-mean");
  const labelStd = document.getElementById("label-std");
  const labelScore = document.getElementById("label-score");

  const helpMedian = document.getElementById("help-median");
  const helpMean = document.getElementById("help-mean");
  const helpStd = document.getElementById("help-std");
  const helpScore = document.getElementById("help-score");

  const legend = document.getElementById("nerd-legend");

  if (NERD_MODE) {
    if (labelMedian) labelMedian.textContent = "Median gap";
    if (labelMean) labelMean.textContent = "Mean gap";
    if (labelStd) labelStd.textContent = "Std Dev";
    if (labelScore) labelScore.textContent = "Health score";

    if (helpMedian)
      helpMedian.textContent = "Middle value of all gaps between buses.";
    if (helpMean)
      helpMean.textContent = "Arithmetic average of the gaps between buses.";
    if (helpStd)
      helpStd.textContent = "How spread out the gaps are over time.";
    if (helpScore)
      helpScore.textContent =
        "Score vs. target spacing (0–1+). Higher is more regular.";

    if (legend) legend.classList.remove("hidden");
  } else {
    // Rider-friendly copy
    if (labelMedian) labelMedian.textContent = "Typical wait";
    if (labelMean) labelMean.textContent = "Average wait";
    if (labelStd) labelStd.textContent = "How steady it is";
    if (labelScore) labelScore.textContent = "Reliability score";

    if (helpMedian)
      helpMedian.textContent =
        "What you’ll usually wait between buses right now.";
    if (helpMean)
      helpMean.textContent =
        "Average time between buses over all recent trips.";
    if (helpStd)
      helpStd.textContent =
        "Smaller numbers = buses spaced out more evenly.";
    if (helpScore)
      helpScore.textContent =
        "How close the service is to the planned spacing.";

    if (legend) legend.classList.add("hidden");
  }
}

document.addEventListener("DOMContentLoaded", () => {
  const nerdToggle = document.getElementById("nerd-toggle");
  if (nerdToggle) {
    nerdToggle.classList.add("peer");
    applyNerdMode(nerdToggle.checked);
    nerdToggle.addEventListener("change", () =>
      applyNerdMode(nerdToggle.checked)
    );
  }

  loadLastUpdated();   // read last_updated.txt
  loadScoresCsv();     // load CSV and populate UI
});

/* ---------------------- CSV loading ---------------------- */

function loadScoresCsv() {
  setStatus("Loading latest headway scores…");

  Papa.parse("data/headway_scores_latest.csv", {
    header: true,
    download: true,
    dynamicTyping: true,
    skipEmptyLines: "greedy",
    complete: (results) => {
      // Ignore benign "FieldMismatch" errors but fail on others
      if (results.errors && results.errors.length) {
        const serious = results.errors.filter(
          (e) => e.type !== "FieldMismatch"
        );
        if (serious.length) {
          console.error("Papa errors:", serious);
          setStatus("Error parsing scores CSV.", "error");
          return;
        }
      }

      HEADWAY_SCORES = (results.data || []).filter(
        (row) => row.route_id !== undefined && row.direction_id !== undefined
      );

      if (!HEADWAY_SCORES.length) {
        setStatus(
          "No headway scores found in CSV. Has the pipeline produced any gold data yet?",
          "warn"
        );
        return;
      }

      setStatus(
        `Loaded ${HEADWAY_SCORES.length} route–direction headway records.`,
        "good"
      );
      populateRouteSelect();
    },
    error: (err) => {
      console.error("Papa error:", err);
      setStatus("Failed to load scores CSV from data/.", "error");
    },
  });
}

/* ---------------------- Helpers ---------------------- */

function uniqueBy(arr, keyFn) {
  const seen = new Set();
  const out = [];
  for (const item of arr) {
    const key = keyFn(item);
    if (!seen.has(key)) {
      seen.add(key);
      out.push(item);
    }
  }
  return out;
}

function classifyHealth(score) {
  const s = Number(score);
  if (!isFinite(s)) {
    return {
      band: "unknown",
      label: "No score",
      chipClass: "bg-slate-800 text-slate-100",
      riderText: "We don't have enough data yet for this route and direction.",
      statusLevel: "info",
    };
  }

  if (s >= 1.05) {
    return {
      band: "very_good",
      label: "Very reliable (little or no bunching)",
      chipClass: "bg-emerald-900 text-emerald-100",
      riderText:
        "Buses are arriving close to the planned schedule. You’re unlikely to see long gaps or clumps.",
      statusLevel: "good",
    };
  } else if (s >= 0.9) {
    return {
      band: "good",
      label: "Mostly regular",
      chipClass: "bg-emerald-800 text-emerald-100",
      riderText:
        "Service is mostly regular with small variations. You might see some minor bunching at times.",
      statusLevel: "good",
    };
  } else if (s >= 0.7) {
    return {
      band: "ok",
      label: "Watch for uneven gaps",
      chipClass: "bg-amber-900 text-amber-100",
      riderText:
        "Gaps between buses are a bit uneven. You may hit a short wait followed by two buses close together.",
      statusLevel: "warn",
    };
  } else {
    return {
      band: "poor",
      label: "High bunching risk",
      chipClass: "bg-red-900 text-red-100",
      riderText:
        "Service is quite uneven right now. Expect long waits followed by clumps of buses.",
      statusLevel: "warn",
    };
  }
}

function getDirectionFriendly(dirId) {
  const d = String(dirId);
  if (d === "0") return "outbound";
  if (d === "1") return "inbound";
  return `direction ${d}`;
}

/* ---------------------- Route / direction UI ---------------------- */

function populateRouteSelect() {
  const routeSelect = document.getElementById("route-select");
  const dirSelect = document.getElementById("direction-select");
  const analyzeBtn = document.getElementById("analyze-btn");

  if (!routeSelect || !dirSelect || !analyzeBtn) return;

  routeSelect.innerHTML = "";
  dirSelect.innerHTML = "";

  const routes = uniqueBy(
    HEADWAY_SCORES,
    (r) => String(r.route_id).trim()
  ).sort((a, b) =>
    String(a.route_id).localeCompare(String(b.route_id), undefined, {
      numeric: true,
    })
  );

  routeSelect.insertAdjacentHTML(
    "beforeend",
    `<option value="">Select route</option>`
  );

  for (const row of routes) {
    const id = String(row.route_id).trim();
    routeSelect.insertAdjacentHTML(
      "beforeend",
      `<option value="${id}">${id}</option>`
    );
  }

  routeSelect.disabled = false;
  dirSelect.disabled = true;
  analyzeBtn.disabled = true;

  routeSelect.addEventListener("change", handleRouteChange);
  dirSelect.addEventListener("change", handleDirectionChange);
  analyzeBtn.addEventListener("click", handleAnalyzeClick);
}

function handleRouteChange() {
  const routeSelect = document.getElementById("route-select");
  const dirSelect = document.getElementById("direction-select");
  const analyzeBtn = document.getElementById("analyze-btn");
  if (!routeSelect || !dirSelect || !analyzeBtn) return;

  const routeId = routeSelect.value;
  dirSelect.innerHTML = "";

  if (!routeId) {
    dirSelect.disabled = true;
    analyzeBtn.disabled = true;
    return;
  }

  const dirs = uniqueBy(
    HEADWAY_SCORES.filter(
      (r) => String(r.route_id).trim() === String(routeId).trim()
    ),
    (r) => String(r.direction_id)
  ).sort((a, b) => Number(a.direction_id) - Number(b.direction_id));

  dirSelect.insertAdjacentHTML(
    "beforeend",
    `<option value="">Direction</option>`
  );

  for (const row of dirs) {
    const val = String(row.direction_id);
    const label =
      val === "0"
        ? "0 – outbound"
        : val === "1"
        ? "1 – inbound"
        : `Dir ${val}`;
    dirSelect.insertAdjacentHTML(
      "beforeend",
      `<option value="${val}">${label}</option>`
    );
  }

  dirSelect.disabled = false;
  analyzeBtn.disabled = true;
}

function handleDirectionChange() {
  const dirSelect = document.getElementById("direction-select");
  const analyzeBtn = document.getElementById("analyze-btn");
  if (!dirSelect || !analyzeBtn) return;

  analyzeBtn.disabled = !dirSelect.value;
}

/* ---------------------- Analyze & update summary ---------------------- */

function handleAnalyzeClick() {
  const routeId = document.getElementById("route-select").value;
  const dirId = document.getElementById("direction-select").value;
  if (!routeId || !dirId) return;

  const row = HEADWAY_SCORES.find(
    (r) =>
      String(r.route_id).trim() === String(routeId).trim() &&
      String(r.direction_id) === String(dirId)
  );

  if (!row) {
    setStatus(
      "No headway samples for this route/direction in the latest snapshot.",
      "warn"
    );
    document.getElementById("summary-card")?.classList.add("hidden");
    return;
  }

  const summaryCard = document.getElementById("summary-card");
  if (!summaryCard) return;

  summaryCard.classList.remove("hidden");

  const health = classifyHealth(row.headway_health_score);
  const dirFriendly = getDirectionFriendly(dirId);

  // Header text
  const routeLabel = document.getElementById("summary-route-label");
  if (routeLabel) {
    if (NERD_MODE) {
      routeLabel.textContent = `Route ${routeId}, direction ${dirId} — based on ${row.count} observed headways (expected: ${row.expected_headway_min} min).`;
    } else {
      routeLabel.textContent = `Route ${routeId}, ${dirFriendly} — live view from the last few minutes (buses should be ~${row.expected_headway_min} min apart).`;
    }
  }

  // Status chip
  const chip = document.getElementById("summary-status-chip");
  if (chip) {
    chip.textContent = health.label;
    chip.className =
      "inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold " +
      health.chipClass;
  }

  // Big numbers
  const fmt = (v, digits) =>
    isFinite(Number(v)) ? Number(v).toFixed(digits) : "–";

  const mMed = document.getElementById("metric-median");
  const mMean = document.getElementById("metric-mean");
  const mStd = document.getElementById("metric-std");
  const mScore = document.getElementById("metric-score");

  if (mMed) mMed.textContent = fmt(row.median, 1) + " min";
  if (mMean) mMean.textContent = fmt(row.mean, 1) + " min";
  if (mStd) mStd.textContent = fmt(row.std || 0, 2) + " min";

  if (mScore) {
    mScore.textContent = fmt(row.headway_health_score, 3);
    let scoreClass = "text-slate-100";
    if (health.band === "very_good" || health.band === "good")
      scoreClass = "text-emerald-300";
    else if (health.band === "ok") scoreClass = "text-amber-300";
    else if (health.band === "poor") scoreClass = "text-red-300";
    mScore.className = "text-2xl font-semibold mt-1 " + scoreClass;
  }

  const labelEl = document.getElementById("metric-label");
  if (labelEl) {
    labelEl.textContent = health.label;
  }

  const riderTextEl = document.getElementById("summary-rider-text");
  if (riderTextEl) {
    riderTextEl.textContent = health.riderText;
  }

  // Optional: tweak helpScore slightly by band (rider mode only)
  const helpScore = document.getElementById("help-score");
  if (!NERD_MODE && helpScore) {
    if (health.band === "very_good") {
      helpScore.textContent =
        "Buses are arriving very close to the planned spacing.";
    } else if (health.band === "good") {
      helpScore.textContent =
        "Mostly on track, with some small ups and downs.";
    } else if (health.band === "ok") {
      helpScore.textContent =
        "Gaps are a bit uneven; waits can vary from trip to trip.";
    } else if (health.band === "poor") {
      helpScore.textContent =
        "Spacing is uneven; buses may be clumped or far apart.";
    }
  }

  setStatus(
    `Route ${routeId}, ${dirFriendly} — based on ${row.count} observed headways.`,
    health.statusLevel
  );
}

function loadLastUpdated() {
  const tsEl = document.getElementById("last-updated");
  if (!tsEl) return;

  fetch("data/last_updated.txt", { cache: "no-store" })
    .then((res) => {
      if (!res.ok) {
        throw new Error("Failed to fetch last_updated.txt");
      }
      return res.text();
    })
    .then((txt) => {
      const raw = txt.trim();
      if (!raw) {
        tsEl.textContent = "Last updated: Unknown";
        return;
      }

      const dt = new Date(raw);
      if (isNaN(dt.getTime())) {
        tsEl.textContent = `Last updated: ${raw}`;
        return;
      }

      // Convert to EST/EDT using America/New_York
      const formattedEST = new Intl.DateTimeFormat("en-US", {
        timeZone: "America/New_York",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      }).format(dt);

      tsEl.textContent = `Last updated: ${formattedEST} EST`;
    })
    .catch((err) => {
      console.error("Error loading last_updated.txt:", err);
      tsEl.textContent = "Last updated: Unknown";
    });
}