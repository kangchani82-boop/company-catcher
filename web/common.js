/**
 * common.js — Company Catcher 공통 유틸리티
 * 모든 페이지에서 공유하는 함수, 상수, UI 컴포넌트
 */

const API = (location.hostname === 'localhost' || location.hostname === '127.0.0.1')
  ? `http://${location.host}` : '';

// ── HTML 이스케이프 ──────────────────────────────────────────────────────────
function escHtml(s) {
  if (!s) return '';
  return String(s)
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}

// ── 날짜 포맷 ────────────────────────────────────────────────────────────────
function fmtDate(dt) {
  if (!dt) return '';
  return String(dt).substring(0, 16).replace('T', ' ');
}

function fmtDateShort(dt) {
  if (!dt) return '';
  return String(dt).substring(0, 10);
}

// ── 심각도 뱃지 ──────────────────────────────────────────────────────────────
function severityBadge(sev) {
  const map = {
    5: { cls: 'sev5', label: '⭐⭐⭐ 즉시취재' },
    4: { cls: 'sev4', label: '⭐⭐ 당일검토' },
    3: { cls: 'sev3', label: '⭐ 주간검토' },
  };
  const m = map[sev] || { cls: 'sev3', label: `sev${sev}` };
  return `<span class="badge ${m.cls}">${m.label}</span>`;
}

// ── 리드 유형 한글 ────────────────────────────────────────────────────────────
const LEAD_TYPE_KO = {
  strategy_change: '신사업진출',
  market_shift:    '시장변화',
  risk_alert:      '리스크경보',
  numeric_change:  '수치급변',
  supply_chain:    '공급망변화',
};
function leadTypeKo(t) { return LEAD_TYPE_KO[t] || t; }

// ── 기사 상태 뱃지 ────────────────────────────────────────────────────────────
const ARTICLE_STATUS = {
  draft:     { cls: 'st-draft',     label: '📝 초안' },
  editing:   { cls: 'st-editing',   label: '✏️ 편집중' },
  ready:     { cls: 'st-ready',     label: '✅ 게재가능' },
  published: { cls: 'st-published', label: '🗞️ 게재완료' },
  rejected:  { cls: 'st-rejected',  label: '❌ 반려' },
};
function articleStatusBadge(s) {
  const m = ARTICLE_STATUS[s] || { cls: 'st-draft', label: s };
  return `<span class="badge ${m.cls}">${m.label}</span>`;
}

// ── 취재단서 상태 뱃지 ────────────────────────────────────────────────────────
const LEAD_STATUS = {
  new:       { cls: 'ls-new',       label: '🆕 신규' },
  reviewing: { cls: 'ls-reviewing', label: '🔍 검토중' },
  drafted:   { cls: 'ls-drafted',   label: '✍ 초안완료' },
  published: { cls: 'ls-published', label: '🗞️ 게재완료' },
  archived:  { cls: 'ls-archived',  label: '📦 보관' },
};
function leadStatusBadge(s) {
  const m = LEAD_STATUS[s] || { cls: 'ls-new', label: s };
  return `<span class="badge ${m.cls}">${m.label}</span>`;
}

// ── 구글/네이버 검색 링크 ────────────────────────────────────────────────────
function newsSearchLinks(corpName, keyword) {
  const q = encodeURIComponent(`${corpName} ${keyword}`);
  return `
    <a href="https://www.google.com/search?q=${q}&tbm=nws" target="_blank"
       class="news-link g-link" title="구글 뉴스 보도 확인">G</a>
    <a href="https://search.naver.com/search.naver?where=news&query=${q}" target="_blank"
       class="news-link n-link" title="네이버 뉴스 보도 확인">N</a>
  `;
}

// ── 페이지네이션 렌더링 ───────────────────────────────────────────────────────
function renderPagination(containerId, total, page, perPage, onPage) {
  const container = document.getElementById(containerId);
  if (!container) return;
  const pages = Math.ceil(total / perPage);
  if (pages <= 1) { container.innerHTML = ''; return; }

  const delta = 2; // 현재 페이지 앞뒤 2개
  let html = '<div class="pagination">';

  // ◀ 이전
  if (page > 1) html += `<button class="pg-btn" onclick="(${onPage})(${page-1})">◀</button>`;
  else          html += `<button class="pg-btn" disabled>◀</button>`;

  // 페이지 번호
  const showPages = new Set([1, pages]);
  for (let i = Math.max(2, page - delta); i <= Math.min(pages - 1, page + delta); i++) showPages.add(i);
  let prev = 0;
  for (const p of [...showPages].sort((a,b)=>a-b)) {
    if (prev && p - prev > 1) html += `<span class="pg-ellipsis">…</span>`;
    if (p === page) html += `<button class="pg-btn active">${p}</button>`;
    else html += `<button class="pg-btn" onclick="(${onPage})(${p})">${p}</button>`;
    prev = p;
  }

  // ▶ 다음
  if (page < pages) html += `<button class="pg-btn" onclick="(${onPage})(${page+1})">▶</button>`;
  else              html += `<button class="pg-btn" disabled>▶</button>`;

  html += `<span class="pg-info">총 ${total.toLocaleString()}건 / ${pages}페이지</span>`;
  html += '</div>';
  container.innerHTML = html;
}

// ── 공통 CSS (페이지별로 동적 삽입) ──────────────────────────────────────────
const COMMON_CSS = `
  :root {
    --bg1: #0f1117; --bg2: #1a1d27; --bg3: #22253a; --bg4: #2a2d42;
    --txt1: #e8eaf0; --txt2: #8b90a0; --border: #2e3248;
    --acc: #5b6af0; --acc2: #4a58d8;
    --sev5: #e74c3c; --sev4: #e67e22; --sev3: #f1c40f;
    --green: #2ecc71; --red: #e74c3c;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg1); color: var(--txt1); font-family: -apple-system, 'Noto Sans KR', sans-serif; font-size: 13px; }

  /* 네비게이션 */
  .top-nav {
    background: var(--bg2); border-bottom: 1px solid var(--border);
    padding: 0 20px; height: 46px; display: flex; align-items: center; gap: 4px;
    position: sticky; top: 0; z-index: 100;
  }
  .top-nav .brand { font-size: 14px; font-weight: 700; color: var(--acc); margin-right: 16px; text-decoration: none; }
  .top-nav a { color: var(--txt2); text-decoration: none; padding: 6px 12px; border-radius: 6px; font-size: 12px; transition: all .15s; }
  .top-nav a:hover { background: var(--bg3); color: var(--txt1); }
  .top-nav a.active { background: var(--acc); color: #fff; }

  /* 페이지 레이아웃 */
  .page-wrap { max-width: 1100px; margin: 0 auto; padding: 20px 16px; }
  .page-title { font-size: 18px; font-weight: 700; margin-bottom: 16px; display: flex; align-items: center; gap: 8px; }
  .page-title .count { font-size: 12px; color: var(--txt2); font-weight: 400; }

  /* 검색/정렬 바 */
  .search-bar {
    display: flex; gap: 8px; margin-bottom: 16px; align-items: center; flex-wrap: wrap;
  }
  .search-bar input {
    flex: 1; min-width: 200px; background: var(--bg3); border: 1px solid var(--border);
    color: var(--txt1); border-radius: 8px; padding: 8px 14px; font-size: 13px; outline: none;
  }
  .search-bar input:focus { border-color: var(--acc); }
  .search-bar select {
    background: var(--bg3); border: 1px solid var(--border); color: var(--txt1);
    border-radius: 8px; padding: 8px 10px; font-size: 12px; cursor: pointer; outline: none;
  }

  /* 카드 리스트 */
  .card-list { display: flex; flex-direction: column; gap: 8px; margin-bottom: 16px; }
  .card {
    background: var(--bg2); border: 1px solid var(--border); border-radius: 10px;
    padding: 14px 16px; cursor: pointer; transition: all .15s;
  }
  .card:hover { background: var(--bg3); border-color: var(--acc); }
  .card-header { display: flex; align-items: center; gap: 6px; margin-bottom: 6px; flex-wrap: wrap; }
  .card-title { font-size: 14px; font-weight: 600; color: var(--txt1); margin-bottom: 4px; line-height: 1.4; }
  .card-sub { font-size: 12px; color: var(--txt2); margin-bottom: 4px; }
  .card-preview { font-size: 12px; color: var(--txt2); line-height: 1.6; margin-bottom: 6px; }
  .card-footer { display: flex; align-items: center; gap: 8px; font-size: 11px; color: var(--txt2); }
  .card-actions { margin-left: auto; display: flex; gap: 4px; }

  /* 배지 */
  .badge { display: inline-block; font-size: 10px; border-radius: 4px; padding: 2px 7px; font-weight: 600; white-space: nowrap; }
  .sev5 { background: rgba(231,76,60,.2); color: #e74c3c; border: 1px solid rgba(231,76,60,.4); }
  .sev4 { background: rgba(230,126,34,.2); color: #e67e22; border: 1px solid rgba(230,126,34,.4); }
  .sev3 { background: rgba(241,196,15,.2); color: #f1c40f; border: 1px solid rgba(241,196,15,.4); }
  .corp-badge { background: var(--acc); color: #fff; border-radius: 4px; padding: 2px 8px; font-size: 11px; }
  .type-badge { background: var(--bg4); color: var(--txt2); border-radius: 4px; padding: 2px 7px; font-size: 10px; }
  .st-draft    { background: rgba(91,106,240,.15); color: #8b9cf8; border: 1px solid rgba(91,106,240,.3); }
  .st-editing  { background: rgba(230,126,34,.15); color: #e67e22; border: 1px solid rgba(230,126,34,.3); }
  .st-ready    { background: rgba(46,204,113,.15); color: #2ecc71; border: 1px solid rgba(46,204,113,.3); }
  .st-published{ background: rgba(52,152,219,.15); color: #3498db; border: 1px solid rgba(52,152,219,.3); }
  .st-rejected { background: rgba(231,76,60,.15);  color: #e74c3c; border: 1px solid rgba(231,76,60,.3); }
  .ls-new      { background: rgba(91,106,240,.15); color: #8b9cf8; border: 1px solid rgba(91,106,240,.3); }
  .ls-reviewing{ background: rgba(230,126,34,.15); color: #e67e22; border: 1px solid rgba(230,126,34,.3); }
  .ls-drafted  { background: rgba(46,204,113,.15); color: #2ecc71; border: 1px solid rgba(46,204,113,.3); }
  .ls-published{ background: rgba(52,152,219,.15); color: #3498db; border: 1px solid rgba(52,152,219,.3); }
  .ls-archived { background: rgba(100,100,100,.2); color: #888;    border: 1px solid rgba(100,100,100,.3); }

  /* 뉴스 검색 링크 */
  .news-link {
    font-size: 10px; border-radius: 3px; padding: 1px 5px; text-decoration: none;
    font-weight: 600; display: inline-block;
  }
  .g-link { color: #4285f4; border: 1px solid #4285f4; }
  .g-link:hover { background: rgba(66,133,244,.1); }
  .n-link { color: #03c75a; border: 1px solid #03c75a; }
  .n-link:hover { background: rgba(3,199,90,.1); }

  /* 액션 버튼 */
  .act-btn {
    font-size: 10px; border-radius: 3px; padding: 2px 7px; border: 1px solid;
    background: none; cursor: pointer; font-weight: 600; white-space: nowrap;
  }
  .act-btn-draft  { color: #e67e22; border-color: #e67e22; }
  .act-btn-draft:hover { background: rgba(230,126,34,.1); }
  .act-btn-primary{ color: var(--acc); border-color: var(--acc); }
  .act-btn-primary:hover { background: rgba(91,106,240,.1); }

  /* 페이지네이션 */
  .pagination { display: flex; align-items: center; gap: 4px; justify-content: center; padding: 12px 0; flex-wrap: wrap; }
  .pg-btn {
    min-width: 32px; height: 32px; border-radius: 6px; border: 1px solid var(--border);
    background: var(--bg3); color: var(--txt1); cursor: pointer; font-size: 12px;
    display: flex; align-items: center; justify-content: center; padding: 0 6px;
  }
  .pg-btn:hover:not(:disabled) { background: var(--acc); border-color: var(--acc); color: #fff; }
  .pg-btn.active { background: var(--acc); border-color: var(--acc); color: #fff; font-weight: 700; }
  .pg-btn:disabled { opacity: .4; cursor: default; }
  .pg-ellipsis { color: var(--txt2); padding: 0 4px; }
  .pg-info { font-size: 11px; color: var(--txt2); margin-left: 8px; }

  /* 로딩 / 빈 상태 */
  .loading { text-align: center; color: var(--txt2); padding: 40px; font-size: 13px; }
  .empty   { text-align: center; color: var(--txt2); padding: 60px; }
  .empty-icon { font-size: 40px; margin-bottom: 12px; }

  /* 모달 */
  .modal-overlay {
    position: fixed; top:0; left:0; width:100%; height:100%; z-index:9000;
    background: rgba(0,0,0,.7); display:flex; align-items:center; justify-content:center;
  }
  .modal-box {
    background: var(--bg2); border-radius: 12px; width: 720px; max-width: 96vw;
    max-height: 90vh; overflow-y: auto; box-shadow: 0 20px 60px rgba(0,0,0,.5);
  }
  .modal-header {
    padding: 16px 20px; border-bottom: 1px solid var(--border);
    display: flex; align-items: center; gap: 10px;
    position: sticky; top: 0; background: var(--bg2); z-index: 1;
  }
  .modal-title { font-size: 15px; font-weight: 700; }
  .modal-close { margin-left: auto; background: none; border: none; color: var(--txt2); font-size: 20px; cursor: pointer; padding: 0; }
  .modal-close:hover { color: var(--txt1); }
  .modal-body { padding: 20px; }

  /* 스크롤바 */
  ::-webkit-scrollbar { width: 6px; height: 6px; }
  ::-webkit-scrollbar-track { background: var(--bg1); }
  ::-webkit-scrollbar-thumb { background: var(--bg4); border-radius: 3px; }
`;

// ── 공통 네비게이션 HTML 생성 ─────────────────────────────────────────────────
function buildNav(activePage) {
  const items = [
    { href: '/',                  key: 'home',        label: '🏠 기업분석' },
    { href: '/comparisons.html',  key: 'comparisons', label: '📊 비교분석' },
    { href: '/leads.html',        key: 'leads',       label: '🚨 취재단서' },
    { href: '/articles.html',     key: 'articles',    label: '✍ 기사초안' },
  ];
  return `<nav class="top-nav">
    <a href="/" class="brand">CompanyCatcher</a>
    ${items.map(it => `
      <a href="${it.href}" class="${activePage===it.key?'active':''}">${it.label}</a>
    `).join('')}
  </nav>`;
}

// ── 페이지 초기화 헬퍼 ────────────────────────────────────────────────────────
function initPage(activePage) {
  // CSS 주입
  const style = document.createElement('style');
  style.textContent = COMMON_CSS;
  document.head.appendChild(style);
  // 네비 삽입
  const nav = document.createElement('div');
  nav.innerHTML = buildNav(activePage);
  document.body.insertBefore(nav.firstElementChild, document.body.firstChild);
}

// ── 마크다운 경량 렌더러 (강조/링크/헤딩만) ─────────────────────────────────
function renderMarkdown(text) {
  if (!text) return '';
  return escHtml(text)
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/^#{1,3} (.+)$/gm, '<h4 style="margin:8px 0 4px;color:var(--txt1)">$1</h4>')
    .replace(/\n\n/g, '<br><br>')
    .replace(/\n/g, '<br>');
}

// ── 기사 초안 모달 (공통 — articles/leads 양쪽에서 사용) ─────────────────────
function showArticleModal(draft, corpName) {
  const existing = document.getElementById('_article_modal');
  if (existing) existing.remove();
  const kw = (() => { try { return JSON.parse(draft.keywords||'[]').join(', '); } catch { return ''; } })();
  const overlay = document.createElement('div');
  overlay.id = '_article_modal';
  overlay.className = 'modal-overlay';
  overlay.innerHTML = `
    <div class="modal-box">
      <div class="modal-header" style="background:linear-gradient(135deg,#e67e22,#d35400);border-radius:12px 12px 0 0;">
        <span style="font-size:18px;">✍</span>
        <div>
          <div class="modal-title" style="color:#fff;">${escHtml(draft.headline||'기사 초안')}</div>
          <div style="font-size:11px;color:rgba(255,255,255,.8);">${escHtml(corpName)} · ${escHtml(draft.model||'')} · 파이낸스코프 고종민 기자</div>
        </div>
        <button class="modal-close" style="color:#fff;" onclick="document.getElementById('_article_modal').remove()">✕</button>
      </div>
      <div class="modal-body">
        ${draft.subheadline ? `<div style="background:var(--bg3);border-radius:8px;padding:10px 14px;margin-bottom:10px;font-size:13px;color:var(--txt2);">${escHtml(draft.subheadline)}</div>` : ''}
        <div style="background:var(--bg3);border-radius:8px;padding:14px;margin-bottom:10px;">
          <div id="_draft_body" style="font-size:13px;color:var(--txt1);line-height:1.9;white-space:pre-wrap;"
               contenteditable="true">${escHtml(draft.content||'')}</div>
        </div>
        ${draft.editor_note ? `<div style="background:rgba(255,193,7,.08);border:1px solid rgba(255,193,7,.25);border-radius:6px;padding:8px 12px;font-size:11px;margin-bottom:10px;"><b style="color:#f39c12;">📋 편집 메모</b> ${escHtml(draft.editor_note)}</div>` : ''}
        <div style="display:flex;gap:8px;justify-content:flex-end;">
          <button class="act-btn act-btn-primary" onclick="copyArticleText()">📋 복사</button>
          <button class="act-btn" style="color:var(--txt2);border-color:var(--border);"
            onclick="document.getElementById('_article_modal').remove()">닫기</button>
        </div>
      </div>
    </div>`;
  document.body.appendChild(overlay);
  overlay.addEventListener('click', e => { if (e.target===overlay) overlay.remove(); });
}

function copyArticleText() {
  const body = document.getElementById('_draft_body')?.innerText || '';
  navigator.clipboard.writeText(body).then(() => alert('클립보드에 복사되었습니다.'));
}

// ── 기사 초안 생성 (API 호출) ────────────────────────────────────────────────
async function generateDraftFromLead(leadId, corpName, btn) {
  if (btn) { btn.disabled = true; btn.textContent = '⏳...'; }
  try {
    const resp = await fetch(`${API}/api/leads/${leadId}/draft`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Api-Key': 'local' }
    });
    const d = await resp.json();
    if (!resp.ok) throw new Error(d.error || '서버 오류');
    showArticleModal(d.draft, corpName);
  } catch(e) {
    alert(`기사 초안 생성 오류: ${e.message}`);
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = '✍ 초안'; }
  }
}
