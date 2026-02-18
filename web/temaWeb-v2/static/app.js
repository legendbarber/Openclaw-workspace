(() => {
  const BUILD_ID = "ui-2.1.0";
  const $ = (sel) => document.querySelector(sel);

  const page = (document.body?.dataset?.page || "index").toLowerCase();

  const state = {
    enableRefresh: false,
    deferredInstallPrompt: null,
    pollTimer: null,
    lastSeenRefreshId: 0,

    // filters (shared)
    excludeBigcaps: false,
    sortKey: "trade_value", // changerate | trade_value | volume
    selectedDate: "",       // ''=latest, 'yymmdd'

    // page specific
    themeRank: null,
    recordOrder: "desc",
    recordSearch: "",
  };

  const PREF_EXCLUDE = "tema.exclude_bigcaps";
  const PREF_SORT = "tema.sort";
  const PREF_DATE = "tema.date";
  const PREF_RECORD_ORDER = "tema.record.order";

  const fmt = (v) => (v === null || v === undefined) ? "" : String(v);

  // ---- number formatting helpers ----
  const NUM_RE = /[-+]?\d+(?:\.\d+)?/;

  function toNumber(v){
    const s = fmt(v).replace(/,/g, "").replace(/%/g, "").replace(/\+/g, "").trim();
    const m = s.match(NUM_RE);
    if (!m) return 0;
    const n = parseFloat(m[0]);
    return Number.isFinite(n) ? n : 0;
  }

  function commaInt(n){
    if (!Number.isFinite(n)) return "";
    return Math.round(n).toLocaleString("ko-KR");
  }

  function formatPct(v){
    const n = typeof v === "number" ? v : toNumber(v);
    if (!Number.isFinite(n)) return "";
    const s = (n > 0 ? "+" : "") + n.toFixed(2) + "%";
    return s.replace(/\.00%$/, "%"); // 1.00% -> 1%
  }

  function formatTradeChunEok(v){
    const n = toNumber(v);
    if (!n) return "";
    const chun = n / 100000; // raw -> 천억(프로젝트 기존 단위 유지)
    const rounded = Math.trunc(chun * 10) / 10; // 소수 1자리(버림)
    return rounded.toFixed(1);
  }

  function formatMarketCap(v){
    // 입력이 "억" 단위로 들어온다는 전제(기존 CSV 기준)
    const n = toNumber(v);
    if (!n) return "";
    if (n >= 10000){
      const jo = n / 10000;
      const rounded = Math.trunc(jo * 10) / 10;
      return `${rounded.toFixed(1)}조`;
    }
    return `${commaInt(n)}억`;
  }

  function formatPrice(v){
    const n = toNumber(v);
    if (!n) return fmt(v);
    return commaInt(n);
  }

  function computeAlpha(changeRate, tradeValue){
    const rate = toNumber(changeRate);
    const tvChun = formatTradeChunEok(tradeValue);
    if (!tvChun) return "";
    const denom = parseFloat(tvChun);
    if (!denom) return "";
    const a = rate / denom;
    return Number.isFinite(a) ? a.toFixed(2) : "";
  }

  function computeBeta(tradeValue, marketCap){
    const tv = toNumber(tradeValue);
    const mc = toNumber(marketCap);
    if (!tv || !mc) return "";
    const b = tv / mc;
    if (!Number.isFinite(b)) return "";
    // 작은 값이 많아서 4자리 고정
    return b.toFixed(4);
  }

  function rateClass(v){
    const n = toNumber(v);
    if (n > 0) return "pos";
    if (n < 0) return "neg";
    return "neu";
  }

  // ---- prefs / URL ----
  function loadPrefs(){
    try{ state.excludeBigcaps = localStorage.getItem(PREF_EXCLUDE) === "1"; }catch(e){}
    try{
      const v = localStorage.getItem(PREF_SORT);
      if (v) state.sortKey = v;
    }catch(e){}
    try{
      const d = localStorage.getItem(PREF_DATE);
      if (d) state.selectedDate = d;
    }catch(e){}
    try{
      const o = localStorage.getItem(PREF_RECORD_ORDER);
      if (o) state.recordOrder = o;
    }catch(e){}
  }

  function savePrefs(){
    try{
      localStorage.setItem(PREF_EXCLUDE, state.excludeBigcaps ? "1" : "0");
      localStorage.setItem(PREF_SORT, state.sortKey || "trade_value");
      localStorage.setItem(PREF_DATE, state.selectedDate || "");
      localStorage.setItem(PREF_RECORD_ORDER, state.recordOrder || "desc");
    }catch(e){}
  }

  function readUrlParams(){
    const p = new URLSearchParams(location.search);
    const rank = p.get("rank");
    const date = p.get("date");
    const sort = p.get("sort");
    const exc = p.get("exclude_bigcaps");

    if (rank && /^\d+$/.test(rank)) state.themeRank = parseInt(rank, 10);

    if (date && /^\d{6}$/.test(date)) state.selectedDate = date;
    if (sort) state.sortKey = sort;
    if (exc === "1" || exc === "0") state.excludeBigcaps = exc === "1";
  }

  function buildQuery(extra = {}){
    const p = new URLSearchParams({
      exclude_bigcaps: state.excludeBigcaps ? "1" : "0",
      sort: state.sortKey || "trade_value",
      ...(state.selectedDate ? { date: state.selectedDate } : {}),
      ...extra,
    });
    return p.toString();
  }

  function navigateToTheme(rank){
    const qs = buildQuery({ rank: String(rank) });
    location.href = `/theme?${qs}`;
  }

  // ---- UI helpers ----
  function toast(title, sub, isError=false){
    const host = $("#toastHost");
    if (!host) return;
    const t = document.createElement("div");
    t.className = "toast" + (isError ? " error" : "");
    const tt = document.createElement("div");
    tt.className = "toast-title";
    tt.textContent = title || "";
    const ts = document.createElement("div");
    ts.className = "toast-sub";
    ts.textContent = sub || "";
    t.appendChild(tt);
    t.appendChild(ts);
    host.appendChild(t);
    setTimeout(() => { try { host.removeChild(t); } catch(e) {} }, isError ? 5200 : 2500);
  }

  function showProgress(show, text){
    const el = $("#topProgress");
    if (!el) return;
    el.classList.toggle("hidden", !show);
    el.setAttribute("aria-hidden", show ? "false" : "true");
    const t = $("#topProgressText");
    if (t && text !== undefined) t.textContent = text || "불러오는 중…";
  }

  function setRefreshButtonLoading(isLoading){
    const btn = $("#btnRefresh");
    if (!btn) return;
    btn.disabled = !!isLoading || !state.enableRefresh;
    btn.textContent = isLoading ? "진행중..." : (state.enableRefresh ? "새로고침" : "새로고침(OFF)");
    btn.title = state.enableRefresh
      ? "서버에서 데이터를 다시 수집합니다"
      : "서버에서 최신화 기능이 꺼져 있습니다 (ENABLE_REFRESH=false)";
    btn.classList.toggle("is-off", !state.enableRefresh);
  }

  function setSubline(text){
    const el = $("#subline");
    if (el) el.innerHTML = text || "";
  }

  function makeChip(label, value){
    if (!value) return null;
    const el = document.createElement("span");
    el.className = "chip mono";
    el.innerHTML = `${label} <strong>${value}</strong>`;
    return el;
  }

  function safeText(v){ return (v === null || v === undefined) ? "" : String(v); }

  // ---- install UX ----
  function isStandalone(){
    return window.matchMedia("(display-mode: standalone)").matches || window.navigator.standalone === true;
  }

  function setupInstallUX(){
    const btn = $("#btnInstall");
    if (!btn) return;

    if (isStandalone()){
      btn.style.display = "none";
      return;
    }

    window.addEventListener("beforeinstallprompt", (e) => {
      e.preventDefault();
      state.deferredInstallPrompt = e;
      btn.style.display = "";
    });

    btn.addEventListener("click", async () => {
      if (!state.deferredInstallPrompt){
        toast("설치", "브라우저 메뉴에서 ‘홈 화면에 추가’를 선택해 설치할 수 있어요.", false);
        return;
      }
      state.deferredInstallPrompt.prompt();
      try{ await state.deferredInstallPrompt.userChoice; }catch(e){}
      state.deferredInstallPrompt = null;
      btn.style.display = "none";
    });

    // iOS safari hint (no beforeinstallprompt)
    const hint = $("#hintInstall");
    if (hint){
      const ua = navigator.userAgent || "";
      const isiOS = /iPhone|iPad|iPod/i.test(ua);
      const isSafari = isiOS && /Safari/i.test(ua) && !/CriOS|FxiOS|EdgiOS/i.test(ua);
      if (isSafari){
        hint.style.display = "";
        hint.textContent = "iOS: Safari 공유(⬆️) → ‘홈 화면에 추가’로 설치할 수 있어요.";
      } else {
        hint.style.display = "none";
      }
    }
  }

  // ---- controls ----
  function populateDateSelect(dates){
    const selDate = $("#selDate");
    if (!selDate) return;

    selDate.innerHTML = "";
    const optLatest = document.createElement("option");
    optLatest.value = "";
    optLatest.textContent = "(최신)";
    selDate.appendChild(optLatest);

    const list = Array.isArray(dates) ? dates.slice().sort().reverse() : [];
    for (const d of list){
      const o = document.createElement("option");
      o.value = d;
      o.textContent = d;
      selDate.appendChild(o);
    }

    if (state.selectedDate && !list.includes(state.selectedDate)){
      state.selectedDate = "";
      savePrefs();
    }
    selDate.value = state.selectedDate || "";
  }

  function setupControls(){
    const chk = $("#chkExcludeBig");
    const selSort = $("#selSort");
    const selDate = $("#selDate");

    if (chk){
      chk.checked = !!state.excludeBigcaps;
      chk.addEventListener("change", async () => {
        state.excludeBigcaps = chk.checked;
        savePrefs();
        await reloadPageData();
      });
    }

    if (selSort){
      selSort.value = state.sortKey || "trade_value";
      selSort.addEventListener("change", async () => {
        state.sortKey = selSort.value || "trade_value";
        savePrefs();
        await reloadPageData();
      });
    }

    if (selDate){
      selDate.value = state.selectedDate || "";
      selDate.addEventListener("change", async () => {
        state.selectedDate = selDate.value || "";
        savePrefs();
        await reloadPageData();
      });
    }

    const inp = $("#inpRecordSearch");
    if (inp){
      inp.value = state.recordSearch || "";
      inp.addEventListener("input", () => {
        state.recordSearch = inp.value || "";
        renderRecordTable(state._records || []);
      });
    }

    const btnSort = $("#btnRecordSort");
    if (btnSort){
      btnSort.textContent = state.recordOrder === "asc" ? "날짜 ▲" : "날짜 ▼";
      btnSort.addEventListener("click", () => {
        state.recordOrder = state.recordOrder === "asc" ? "desc" : "asc";
        savePrefs();
        btnSort.textContent = state.recordOrder === "asc" ? "날짜 ▲" : "날짜 ▼";
        renderRecordTable(state._records || []);
      });
    }

    $("#btnRecordReload")?.addEventListener("click", () => loadRecordPage());
  }

  // ---- data loading ----
  async function loadStatus(){
    const r = await fetch("/api/status", { cache: "no-store" });
    const j = await r.json();

    state.enableRefresh = !!j.enable_refresh;
    setRefreshButtonLoading(false);
    populateDateSelect(j.dates || []);
    const latest = j.latest || "";

    let head = "데이터 없음";
    if (state.selectedDate){
      head = `<b>${state.selectedDate}</b> 기준`;
      if (latest && state.selectedDate !== latest){
        head += ` · 최신 <b>${latest}</b>`;
      }
    } else if (latest){
      head = `<b>${latest}</b> 기준`;
    }

    const extra = [];
    extra.push(state.excludeBigcaps ? "삼성/하이닉스 제외" : "전체 포함");
    extra.push(`정렬 ${state.sortKey}`);
    const rf = j.refresh || {};
    if (rf.ended_at) extra.push(`마지막 최신화 ${rf.ended_at}`);
    if (!state.enableRefresh) extra.push("최신화 OFF");

    setSubline(`${head} · ${extra.join(" · ")}`);

    const rootPath = $("#rootPath");
    if (rootPath) rootPath.textContent = j.tema_root ? `데이터 폴더: ${j.tema_root}` : "";

    // polling for refresh progress
    if (rf.in_progress){
      showProgress(true, "최신화 중… (서버 갱신)");
      setRefreshButtonLoading(true);
      startPolling();
    } else {
      showProgress(false);
      setRefreshButtonLoading(false);
      stopPolling();
    }

    const rid = rf.refresh_id || 0;
    if (rid && rid !== state.lastSeenRefreshId && !rf.in_progress){
      state.lastSeenRefreshId = rid;
      if (rf.last_error){
        toast("최신화 실패", rf.last_error, true);
      } else {
        toast("최신화 완료", "데이터를 갱신했습니다.", false);
      }
    }

    return j;
  }

  // ---- refresh ----
  function startPolling(){
    if (state.pollTimer) return;
    state.pollTimer = setInterval(async () => {
      try{
        const r = await fetch("/api/status", { cache: "no-store" });
        const j = await r.json();
        const rf = j.refresh || {};
        if (!rf.in_progress){
          stopPolling();
          showProgress(false);
          setRefreshButtonLoading(false);
          await loadStatus();
          await reloadPageData();
        } else {
          showProgress(true, "최신화 중… (서버 갱신)");
          setRefreshButtonLoading(true);
        }
      }catch(e){}
    }, 1000);
  }

  function stopPolling(){
    if (state.pollTimer){
      clearInterval(state.pollTimer);
      state.pollTimer = null;
    }
  }

  async function runRefresh(){
    if (!state.enableRefresh){
      toast("최신화 불가", "서버에서 ENABLE_REFRESH=false 입니다.", true);
      return;
    }
    showProgress(true, "최신화 요청…");
    setRefreshButtonLoading(true);

    try{
      const r = await fetch("/api/refresh", { method: "POST" });
      if (r.status === 409){
        startPolling();
        return;
      }
      if (!r.ok){
        const t = await r.text();
        throw new Error(t);
      }
      startPolling();
    }catch(e){
      showProgress(false);
      setRefreshButtonLoading(false);
      toast("최신화 요청 실패", e.message || String(e), true);
    }
  }

  // ---- rendering: index ----
  function buildThemeCard(theme){
    const card = document.createElement("div");
    card.className = "card";
    card.addEventListener("click", () => navigateToTheme(theme.rank));

    const head = document.createElement("div");
    head.className = "card-head";

    const title = document.createElement("div");
    title.className = "card-title";
    title.textContent = safeText(theme.title);

    const badges = document.createElement("div");
    badges.className = "card-badges";

    const b1 = document.createElement("div");
    b1.className = "badge";
    b1.textContent = `#${theme.rank}`;
    badges.appendChild(b1);

    if (theme.trade_sum !== undefined && theme.trade_sum !== null && theme.trade_sum !== ""){
      const b2 = document.createElement("div");
      b2.className = "badge muted mono";
      b2.textContent = `합 ${safeText(theme.trade_sum)}`;
      badges.appendChild(b2);
    }

    head.appendChild(title);
    head.appendChild(badges);

    const rows = document.createElement("div");
    rows.className = "rows";

    const preview = Array.isArray(theme.preview) ? theme.preview : [];
    for (let i=0; i<Math.min(6, preview.length); i++){
      rows.appendChild(buildStockPreviewRow(preview[i]));
    }

    card.appendChild(head);
    card.appendChild(rows);
    return card;
  }

  function buildStockPreviewRow(r){
    const row = document.createElement("div");
    row.className = "srow";

    const left = document.createElement("div");
    left.className = "sleft";

    const name = document.createElement("div");
    name.className = "sname";
    name.textContent = safeText(r.name);

    const sub = document.createElement("div");
    sub.className = "ssub";

    // keep these concise on preview
    const cap = formatMarketCap(r.market_cap);
    const tv = formatTradeChunEok(r.trade_value);
    const beta = computeBeta(r.trade_value, r.market_cap);

    const chips = [
      makeChip("시총", cap),
      makeChip("거래", tv ? `${tv}천억` : ""),
      makeChip("β", beta),
    ].filter(Boolean);

    chips.slice(0, 3).forEach(c => sub.appendChild(c));

    left.appendChild(name);
    left.appendChild(sub);

    const right = document.createElement("div");
    right.className = "sright";

    const rate = document.createElement("div");
    rate.className = "rate mono " + rateClass(r.change_rate);
    rate.textContent = safeText(r.change_rate) || "";

    const code = document.createElement("div");
    code.className = "small mono";
    code.textContent = safeText(r.code);

    right.appendChild(rate);
    right.appendChild(code);

    row.appendChild(left);
    row.appendChild(right);
    return row;
  }

  function renderInsights(summary){
    const meta = $("#insightMeta");
    const grid = $("#insightGrid");
    if (!meta || !grid) return;

    const ds = summary?.dates || [];
    const hottest = summary?.hottest || [];
    const rising = summary?.rising || [];
    meta.textContent = ds.length ? `${ds[0]} ~ ${ds[ds.length-1]} / 최근 ${summary.lookback}일 분석` : "분석 데이터 없음";

    const hotHtml = hottest.slice(0,10).map((x,i)=>
      `<div class='srow'><div class='sleft'><div class='sname'>#${i+1} ${x.title}</div><div class='ssub'><span class='chip mono'>출현 <strong>${x.freq}회</strong></span><span class='chip mono'>평균랭크 <strong>${x.avg_rank}</strong></span></div></div><div class='sright'><div class='small mono'>최근 ${x.last_seen}</div><div class='rate'>#${x.last_rank}</div></div></div>`
    ).join("");

    const riseHtml = rising.slice(0,10).map((x,i)=>
      `<div class='srow'><div class='sleft'><div class='sname'>#${i+1} ${x.title}</div><div class='ssub'><span class='chip mono'>개선폭 <strong>${x.improvement}</strong></span><span class='chip mono'>최근평균 <strong>${x.recent_avg_rank}</strong></span></div></div><div class='sright'><div class='small mono'>이전 ${x.prev_avg_rank}</div></div></div>`
    ).join("");

    grid.innerHTML = `
      <div class='card'><div class='card-title'>🔥 반복 출현 상위 테마</div>${hotHtml || '<div class="record-empty">데이터 없음</div>'}</div>
      <div class='card'><div class='card-title'>📈 최근 순위 개선 테마</div>${riseHtml || '<div class="record-empty">데이터 없음</div>'}</div>
    `;
  }

  async function loadInsights(){
    const grid = $("#insightGrid");
    if (!grid) return;
    try{
      const r = await fetch(`/api/insights/summary?lookback=20&top_n=10&exclude_bigcaps=${state.excludeBigcaps?1:0}`, { cache: "no-store" });
      if (!r.ok) throw new Error(await r.text());
      const j = await r.json();
      renderInsights(j);
    }catch(e){
      const meta = $("#insightMeta");
      if (meta) meta.textContent = "인사이트 로드 실패";
    }
  }

  async function loadThemeHistoryByInput(){
    const inp = $("#inpThemeHistory");
    const out = $("#insightHistory");
    if (!inp || !out) return;
    const q = (inp.value || "").trim();
    if (!q){ out.textContent = "테마명을 입력하세요."; return; }

    out.textContent = "조회 중...";
    try{
      const r = await fetch(`/api/insights/theme-history?title=${encodeURIComponent(q)}&lookback=90&exclude_bigcaps=${state.excludeBigcaps?1:0}`, { cache: "no-store" });
      if (!r.ok) throw new Error(await r.text());
      const j = await r.json();
      const rows = j.rows || [];
      if (!rows.length){ out.textContent = "검색 결과가 없습니다."; return; }
      out.textContent = rows.map(x=>`${x.date} | #${x.rank} | ${x.title} | 거래합 ${x.trade_sum}`).join("\n");
    }catch(e){
      out.textContent = "히스토리 조회 실패";
    }
  }

  async function loadIndexPage(){
    const grid = $("#grid");
    if (!grid) return;

    showProgress(true, "테마 불러오는 중…");
    try{
      const r = await fetch(`/api/themes?${buildQuery({ limit: "4", preview_n: "6" })}`, { cache: "no-store" });
      if (!r.ok) throw new Error(await r.text());
      const j = await r.json();

      grid.innerHTML = "";
      const themes = j.themes || [];
      if (!themes.length){
        grid.innerHTML = `<div class="record-empty">표시할 테마가 없습니다. (01~04 CSV 파일을 확인하세요)</div>`;
      } else {
        for (const th of themes){
          grid.appendChild(buildThemeCard(th));
        }
      }

      await loadInsights();
    }catch(e){
      toast("불러오기 실패", e.message || String(e), true);
    }finally{
      showProgress(false);
    }
  }

  // ---- theme detail page ----
  function buildDetailRow(row, ctx){
    const el = document.createElement("div");
    el.className = "drow";

    const left = document.createElement("div");
    left.className = "dleft";

    const name = document.createElement("div");
    name.className = "dname";
    name.textContent = safeText(row.name);

    const chips = document.createElement("div");
    chips.className = "dchips";

    const cap = formatMarketCap(row.market_cap);
    const tv = formatTradeChunEok(row.trade_value);
    const alpha = computeAlpha(row.change_rate, row.trade_value);
    const beta = computeBeta(row.trade_value, row.market_cap);

    [
      makeChip("코드", safeText(row.code)),
      makeChip("현재", formatPrice(row.price)),
      makeChip("시총", cap),
      makeChip("거래", tv ? `${tv}천억` : ""),
      makeChip("α", alpha),
      makeChip("β", beta),
    ].filter(Boolean).forEach(c => chips.appendChild(c));

    left.appendChild(name);
    left.appendChild(chips);

    // D+1 block (when available)
    if (row.d1_next_close || row.d1_next_high || row.d1_close_rate || row.d1_high_rate){
      const flow = document.createElement("div");
      flow.className = "dflow mono";

      const head = (ctx?.forward?.ok && ctx.forward.base_trade_date && ctx.forward.next_trade_date)
        ? `D+1(${ctx.forward.base_trade_date}→${ctx.forward.next_trade_date})`
        : "D+1";

      const parts = [];
      if (row.d1_next_close) parts.push(`익일종가 ${formatPrice(row.d1_next_close)}`);
      if (row.d1_next_high) parts.push(`익일고가 ${formatPrice(row.d1_next_high)}`);
      if (row.d1_close_rate) parts.push(`종가수익률 ${safeText(row.d1_close_rate)}`);
      if (row.d1_high_rate) parts.push(`고가수익률 ${safeText(row.d1_high_rate)}`);

      flow.textContent = `${head} · ${parts.join(" · ")}`;
      left.appendChild(flow);
    }

    const right = document.createElement("div");
    right.className = "dright";

    const rate = document.createElement("div");
    rate.className = "rate mono " + rateClass(row.change_rate);
    rate.textContent = safeText(row.change_rate) || "";
    right.appendChild(rate);

    const actions = document.createElement("div");
    actions.className = "action-row";

    if (row.chart_url){
      const a = document.createElement("a");
      a.className = "link";
      a.href = row.chart_url;
      a.target = "_blank";
      a.rel = "noreferrer";
      a.textContent = "차트";
      actions.appendChild(a);
    }

    const btnRec = document.createElement("button");
    btnRec.className = "btn ghost";
    btnRec.textContent = "기록";
    btnRec.addEventListener("click", async (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      await saveRecord(ctx, row, btnRec);
    });
    actions.appendChild(btnRec);

    right.appendChild(actions);

    el.appendChild(left);
    el.appendChild(right);
    return el;
  }

  async function saveRecord(detailJson, row, btn){
    if (!row || !row.code){
      toast("기록 실패", "종목코드가 없습니다.", true);
      return;
    }
    if (btn){
      btn.disabled = true;
      btn.textContent = "저장중…";
    }

    try{
      const payload = {
        date: detailJson?.date || state.selectedDate || "",
        theme_rank: detailJson?.rank || state.themeRank || "",
        theme_title: detailJson?.title || "",
        theme_filename: detailJson?.filename || "",
        chart_url: row.chart_url || "",
        name: row.name || "",
        code: row.code || "",
        market_cap: row.market_cap || "",
        trade_value: row.trade_value || "",
        change_rate: row.change_rate || "",
        alpha: computeAlpha(row.change_rate, row.trade_value),
        beta: computeBeta(row.trade_value, row.market_cap),
        // forward values (if server provided)
        next_date: detailJson?.forward?.next_trade_date || "",
        next_close: row.d1_next_close || "",
        next_high: row.d1_next_high || "",
        d1_close_rate: row.d1_close_rate || "",
        d1_high_rate: row.d1_high_rate || "",
      };

      const r = await fetch("/api/record", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!r.ok){
        throw new Error(await r.text());
      }
      toast("기록 저장", `${payload.date} · ${payload.name} (${payload.code})`, false);

      if (btn){
        btn.textContent = "저장됨";
      }
    }catch(e){
      toast("기록 실패", e.message || String(e), true);
      if (btn) btn.textContent = "기록";
    }finally{
      if (btn) setTimeout(() => { btn.disabled = false; if (btn.textContent === "저장됨") btn.textContent = "기록"; }, 900);
    }
  }

  async function loadThemePage(){
    const rank = state.themeRank;
    if (!rank){
      toast("테마 상세", "rank 파라미터가 없습니다. (예: /theme?rank=1)", true);
      return;
    }

    showProgress(true, "테마 상세 불러오는 중…");
    try{
      const r = await fetch(`/api/themes/${rank}?${buildQuery()}`, { cache: "no-store" });
      if (!r.ok) throw new Error(await r.text());
      const j = await r.json();

      $("#detailTitle").textContent = `#${j.rank} ${j.title}`;
      const metaParts = [];
      if (j.date) metaParts.push(j.date);
      if (j.trade_sum !== undefined && j.trade_sum !== null && j.trade_sum !== "") metaParts.push(`거래대금합 ${j.trade_sum}`);
      metaParts.push(state.excludeBigcaps ? "삼성/하이닉스 제외" : "전체 포함");
      metaParts.push(`정렬 ${state.sortKey}`);
      if (j.forward && j.forward.ok && j.forward.base_trade_date && j.forward.next_trade_date){
        metaParts.push(`D+1(${j.forward.base_trade_date}→${j.forward.next_trade_date})`);
      }
      $("#detailMeta").textContent = metaParts.join(" · ");

      const wrap = $("#detailList");
      wrap.innerHTML = "";
      for (const row of (j.rows || [])){
        wrap.appendChild(buildDetailRow(row, j));
      }
    }catch(e){
      toast("불러오기 실패", e.message || String(e), true);
    }finally{
      showProgress(false);
    }
  }

  // ---- record page ----
  function normalizeRecordRow(r){
    // tolerate older schemas
    const out = {
      id: r.id || r.record_id || r.rid || "",
      date: r.date || r.trade_date || r.날짜 || "",
      theme_rank: r.theme_rank || r.rank || r.테마랭크 || "",
      theme_title: r.theme_title || r.theme || r.테마명 || r.테마 || "",
      code: r.code || r.ticker || r.종목코드 || "",
      name: r.name || r.종목명 || r.종목 || "",
      market_cap: r.market_cap || r.mcap || r.시가총액 || "",
      trade_value: r.trade_value || r.trade || r.거래대금 || "",
      change_rate: r.change_rate || r.chg || r.등락률 || "",
      alpha: r.alpha || r.알파값 || "",
      beta: r.beta || r.베타값 || "",
      next_date: r.next_date || r.익일 || "",
      next_close: r.next_close || r.익일종가 || "",
      next_high: r.next_high || r.익일고가 || "",
      d1_close_rate: r.d1_close_rate || r.익일종가수익률 || "",
      d1_high_rate: r.d1_high_rate || r.익일고가수익률 || "",
      chart_url: r.chart_url || r.차트링크 || "",
    };
    return out;
  }

  function recordMatches(r, q){
    if (!q) return true;
    const s = q.toLowerCase().trim();
    const hay = [
      r.date, r.theme_title, r.name, r.code,
    ].map(x => safeText(x).toLowerCase()).join(" ");
    return hay.includes(s);
  }

  function sortByDate(rows){
    const dir = state.recordOrder === "asc" ? 1 : -1;
    const key = (d) => {
      const m = safeText(d).match(/\d{6,8}/);
      return m ? m[0] : "";
    };
    return rows.slice().sort((a,b) => {
      const da = key(a.date);
      const db = key(b.date);
      if (da === db) return 0;
      return da > db ? dir : -dir;
    });
  }

  function buildRecordTable(rows){
    const table = document.createElement("table");
    table.className = "record-table mono";

    const thead = document.createElement("thead");
    const trh = document.createElement("tr");
    const headers = [
      { k:"date", t:"날짜", cls:"clickable" },
      { k:"theme", t:"테마" },
      { k:"stock", t:"종목" },
      { k:"mcap", t:"시총" , num:true},
      { k:"trade", t:"거래대금", num:true },
      { k:"chg", t:"등락률", num:true },
      { k:"a", t:"α", num:true },
      { k:"b", t:"β", num:true },
      { k:"next", t:"익일" },
      { k:"nc", t:"익일종가", num:true },
      { k:"nh", t:"익일고가", num:true },
      { k:"chart", t:"차트" },
      { k:"del", t:"삭제" },
    ];

    headers.forEach(h => {
      const th = document.createElement("th");
      th.textContent = h.t;
      if (h.cls) th.classList.add(h.cls);
      if (h.k === "date"){
        th.addEventListener("click", () => {
          state.recordOrder = state.recordOrder === "asc" ? "desc" : "asc";
          savePrefs();
          const btnSort = $("#btnRecordSort");
          if (btnSort) btnSort.textContent = state.recordOrder === "asc" ? "날짜 ▲" : "날짜 ▼";
          renderRecordTable(state._records || []);
        });
      }
      trh.appendChild(th);
    });

    thead.appendChild(trh);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");

    for (const r0 of rows){
      const r = normalizeRecordRow(r0);
      const tr = document.createElement("tr");

      // date
      const tdDate = document.createElement("td");
      tdDate.textContent = safeText(r.date);
      tr.appendChild(tdDate);

      // theme
      const tdTheme = document.createElement("td");
      if (r.theme_rank){
        const a = document.createElement("a");
        a.className = "record-theme";
        a.href = `/theme?${buildQuery({ rank: safeText(r.theme_rank), date: safeText(r.date) })}`;
        a.textContent = r.theme_title ? `#${r.theme_rank} ${r.theme_title}` : `#${r.theme_rank}`;
        tdTheme.appendChild(a);
      } else {
        tdTheme.textContent = safeText(r.theme_title);
      }
      tr.appendChild(tdTheme);

      // stock
      const tdStock = document.createElement("td");
      const st = document.createElement("div");
      st.className = "record-stock";
      st.textContent = safeText(r.name);
      const sub = document.createElement("div");
      sub.className = "record-sub mono";
      sub.textContent = safeText(r.code);
      tdStock.appendChild(st);
      tdStock.appendChild(sub);
      tr.appendChild(tdStock);

      // mcap
      const tdMcap = document.createElement("td");
      tdMcap.className = "td-num";
      tdMcap.textContent = formatMarketCap(r.market_cap);
      tr.appendChild(tdMcap);

      // trade
      const tdTrade = document.createElement("td");
      tdTrade.className = "td-num";
      const tv = formatTradeChunEok(r.trade_value);
      tdTrade.textContent = tv ? `${tv}천억` : safeText(r.trade_value);
      tr.appendChild(tdTrade);

      // chg
      const tdChg = document.createElement("td");
      tdChg.className = "td-num";
      const chg = safeText(r.change_rate);
      tdChg.innerHTML = `<span class="rate ${rateClass(chg)}">${chg}</span>`;
      tr.appendChild(tdChg);

      // alpha
      const tdA = document.createElement("td");
      tdA.className = "td-num";
      tdA.textContent = safeText(r.alpha) || computeAlpha(r.change_rate, r.trade_value);
      tr.appendChild(tdA);

      // beta
      const tdB = document.createElement("td");
      tdB.className = "td-num";
      tdB.textContent = safeText(r.beta) || computeBeta(r.trade_value, r.market_cap);
      tr.appendChild(tdB);

      // next date
      const tdNext = document.createElement("td");
      tdNext.textContent = safeText(r.next_date);
      tr.appendChild(tdNext);

      // next close/high
      const tdNc = document.createElement("td");
      tdNc.className = "td-num";
      tdNc.textContent = formatPrice(r.next_close);
      tr.appendChild(tdNc);

      const tdNh = document.createElement("td");
      tdNh.className = "td-num";
      tdNh.textContent = formatPrice(r.next_high);
      tr.appendChild(tdNh);

      // chart
      const tdChart = document.createElement("td");
      if (r.chart_url){
        const a = document.createElement("a");
        a.className = "record-link";
        a.href = r.chart_url;
        a.target = "_blank";
        a.rel = "noreferrer";
        a.textContent = "차트";
        tdChart.appendChild(a);
      } else {
        tdChart.textContent = "";
      }
      tr.appendChild(tdChart);

      // delete
      const tdDel = document.createElement("td");
      const btn = document.createElement("button");
      btn.className = "btn danger btn-del";
      btn.textContent = "삭제";
      btn.addEventListener("click", async () => {
        await deleteRecord(r0);
      });
      tdDel.appendChild(btn);
      tr.appendChild(tdDel);

      tbody.appendChild(tr);
    }

    table.appendChild(tbody);
    return table;
  }

  function renderRecordTable(raw){
    const wrap = $("#recordWrap");
    if (!wrap) return;

    let rows = Array.isArray(raw) ? raw.slice() : [];
    rows = sortByDate(rows);

    const q = (state.recordSearch || "").trim();
    if (q) rows = rows.filter(r => recordMatches(normalizeRecordRow(r), q));

    wrap.innerHTML = "";
    if (!rows.length){
      wrap.innerHTML = `<div class="record-empty">기록이 없습니다.</div>`;
      return;
    }
    wrap.appendChild(buildRecordTable(rows));

    const meta = $("#recordMeta");
    if (meta){
      meta.textContent = `${rows.length}개 · ${state.recordOrder === "asc" ? "오름차순" : "내림차순"}`;
    }
  }

  async function deleteRecord(r0){
    const row = normalizeRecordRow(r0);
    if (!row.id){
      toast("삭제 실패", "record id가 없습니다.", true);
      return;
    }

    try{
      const r = await fetch(`/api/record/${encodeURIComponent(row.id)}`, { method: "DELETE" });
      if (!r.ok) throw new Error(await r.text());
      toast("삭제 완료", `${row.date} · ${row.name}`, false);
      await loadRecordPage();
    }catch(e){
      toast("삭제 실패", e.message || String(e), true);
    }
  }

  async function loadRecordPage(){
    showProgress(true, "기록 불러오는 중…");
    try{
      const r = await fetch("/api/record/json", { cache: "no-store" });
      if (!r.ok) throw new Error(await r.text());
      const j = await r.json();
      const rows = j.records || j.rows || j || [];
      state._records = Array.isArray(rows) ? rows : [];
      renderRecordTable(state._records);

      const meta = $("#recordMeta");
      if (meta){
        const base = j.base_date ? `기준 ${j.base_date}` : "";
        meta.textContent = `${state._records.length}개 · ${state.recordOrder === "asc" ? "오름차순" : "내림차순"}${base ? " · "+base : ""}`;
      }
    }catch(e){
      toast("불러오기 실패", e.message || String(e), true);
    }finally{
      showProgress(false);
    }
  }

  // ---- router ----
  async function reloadPageData(){
    if (page === "index") return await loadIndexPage();
    if (page === "theme") return await loadThemePage();
    if (page === "record") return await loadRecordPage();
  }

  // ---- init ----
  async function init(){
    console.log("[TEMA]", BUILD_ID, "page=", page);

    loadPrefs();
    readUrlParams();
    savePrefs(); // normalize

    setupInstallUX();
    setupControls();

    $("#btnRefresh")?.addEventListener("click", () => runRefresh());
    $("#btnThemeHistory")?.addEventListener("click", () => loadThemeHistoryByInput());
    $("#inpThemeHistory")?.addEventListener("keydown", (e) => { if (e.key === "Enter") loadThemeHistoryByInput(); });

    try{
      await loadStatus();
      await reloadPageData();
    }catch(e){
      toast("초기화 실패", e.message || String(e), true);
    }

    // SW register (network-only)
    if ("serviceWorker" in navigator){
      try{ await navigator.serviceWorker.register("/static/sw.js"); }catch(e){}
    }
  }

  document.addEventListener("DOMContentLoaded", init);
})();