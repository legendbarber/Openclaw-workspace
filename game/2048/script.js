const SIZE = 4;
const ANIM_MS = 130;

let grid = [];
let score = 0;
let best = Number(localStorage.getItem('best-2048') || 0);
let hasWon = false;
let animating = false;

const boardEl = document.getElementById('board');
const scoreEl = document.getElementById('score');
const bestEl = document.getElementById('best');
const overlayEl = document.getElementById('overlay');
const overlayTitleEl = document.getElementById('overlayTitle');
const overlayMsgEl = document.getElementById('overlayMsg');

let bgLayer;
let tileLayer;

function setupBoardDOM() {
  boardEl.innerHTML = '';

  bgLayer = document.createElement('div');
  bgLayer.className = 'bg-layer';

  tileLayer = document.createElement('div');
  tileLayer.className = 'tile-layer';

  for (let i = 0; i < SIZE * SIZE; i++) {
    const c = document.createElement('div');
    c.className = 'bg-cell';
    bgLayer.appendChild(c);
  }

  boardEl.appendChild(bgLayer);
  boardEl.appendChild(tileLayer);
}

function initGame() {
  grid = Array.from({ length: SIZE }, () => Array(SIZE).fill(0));
  score = 0;
  hasWon = false;
  animating = false;
  hideOverlay();
  addRandomTile();
  addRandomTile();
  renderTiles(grid);
  renderScore();
}

function renderScore() {
  scoreEl.textContent = score;
  if (score > best) {
    best = score;
    localStorage.setItem('best-2048', String(best));
  }
  bestEl.textContent = best;
}

function getGeom() {
  const style = getComputedStyle(boardEl);
  const pad = parseFloat(style.getPropertyValue('--pad')) || 10;
  const gap = parseFloat(style.getPropertyValue('--gap')) || 10;
  const w = boardEl.clientWidth;
  const cell = (w - pad * 2 - gap * (SIZE - 1)) / SIZE;
  return { pad, gap, cell };
}

function pos(r, c) {
  const { pad, gap, cell } = getGeom();
  return {
    x: pad + c * (cell + gap),
    y: pad + r * (cell + gap),
    size: cell
  };
}

function tileClass(v) {
  if (v > 2048) return 'v-big';
  return `v-${v}`;
}

function createTile(value, r, c, extraClass = '') {
  const el = document.createElement('div');
  const p = pos(r, c);
  el.className = `tile ${tileClass(value)} ${extraClass}`.trim();
  el.textContent = value;
  el.style.width = `${p.size}px`;
  el.style.height = `${p.size}px`;
  el.style.left = `${p.x}px`;
  el.style.top = `${p.y}px`;
  return el;
}

function renderTiles(state, spawnSet = new Set()) {
  tileLayer.innerHTML = '';
  for (let r = 0; r < SIZE; r++) {
    for (let c = 0; c < SIZE; c++) {
      const v = state[r][c];
      if (!v) continue;
      const key = `${r}-${c}`;
      const el = createTile(v, r, c, spawnSet.has(key) ? 'spawn' : '');
      tileLayer.appendChild(el);
    }
  }
}

function addRandomTileToState(state) {
  const empties = [];
  for (let r = 0; r < SIZE; r++) {
    for (let c = 0; c < SIZE; c++) {
      if (state[r][c] === 0) empties.push([r, c]);
    }
  }
  if (!empties.length) return null;
  const [r, c] = empties[Math.floor(Math.random() * empties.length)];
  state[r][c] = Math.random() < 0.9 ? 2 : 4;
  return { r, c, value: state[r][c] };
}

function addRandomTile() {
  addRandomTileToState(grid);
}

function cloneGrid(g) {
  return g.map(row => [...row]);
}

function analyzeLine(values) {
  const entries = [];
  for (let i = 0; i < SIZE; i++) {
    if (values[i] !== 0) entries.push({ value: values[i], from: i });
  }

  const out = Array(SIZE).fill(0);
  const moves = [];
  let gain = 0;
  let t = 0;
  let i = 0;

  while (i < entries.length) {
    if (i + 1 < entries.length && entries[i].value === entries[i + 1].value) {
      const mergedVal = entries[i].value * 2;
      out[t] = mergedVal;
      gain += mergedVal;
      moves.push({ from: entries[i].from, to: t, value: entries[i].value, merged: true });
      moves.push({ from: entries[i + 1].from, to: t, value: entries[i + 1].value, merged: true });
      i += 2;
    } else {
      out[t] = entries[i].value;
      moves.push({ from: entries[i].from, to: t, value: entries[i].value, merged: false });
      i += 1;
    }
    t += 1;
  }

  return { out, moves, gain };
}

function computeMove(direction, state) {
  const next = Array.from({ length: SIZE }, () => Array(SIZE).fill(0));
  const moves = [];
  let gain = 0;

  for (let line = 0; line < SIZE; line++) {
    let values;

    if (direction === 'left' || direction === 'right') {
      values = state[line].slice();
      if (direction === 'right') values.reverse();
    } else {
      values = [];
      for (let r = 0; r < SIZE; r++) values.push(state[r][line]);
      if (direction === 'down') values.reverse();
    }

    const analyzed = analyzeLine(values);
    gain += analyzed.gain;

    for (let i = 0; i < SIZE; i++) {
      let v = analyzed.out[i];
      let idx = i;
      if (direction === 'right' || direction === 'down') idx = SIZE - 1 - i;

      if (direction === 'left' || direction === 'right') {
        next[line][idx] = v;
      } else {
        next[idx][line] = v;
      }
    }

    for (const m of analyzed.moves) {
      let fromIdx = m.from;
      let toIdx = m.to;
      if (direction === 'right' || direction === 'down') {
        fromIdx = SIZE - 1 - fromIdx;
        toIdx = SIZE - 1 - toIdx;
      }

      if (direction === 'left' || direction === 'right') {
        moves.push({
          fromR: line,
          fromC: fromIdx,
          toR: line,
          toC: toIdx,
          value: m.value,
          merged: m.merged
        });
      } else {
        moves.push({
          fromR: fromIdx,
          fromC: line,
          toR: toIdx,
          toC: line,
          value: m.value,
          merged: m.merged
        });
      }
    }
  }

  const moved = JSON.stringify(state) !== JSON.stringify(next);
  return { moved, next, gain, moves };
}

function canMove() {
  for (let r = 0; r < SIZE; r++) {
    for (let c = 0; c < SIZE; c++) {
      if (grid[r][c] === 0) return true;
      if (c < SIZE - 1 && grid[r][c] === grid[r][c + 1]) return true;
      if (r < SIZE - 1 && grid[r][c] === grid[r + 1][c]) return true;
    }
  }
  return false;
}

function showOverlay(title, msg, showRetry = true) {
  overlayTitleEl.textContent = title;
  overlayMsgEl.textContent = msg;
  document.getElementById('retryBtn').style.display = showRetry ? 'inline-block' : 'none';
  overlayEl.classList.remove('hidden');
}

function hideOverlay() {
  overlayEl.classList.add('hidden');
}

function animateMove(result) {
  tileLayer.innerHTML = '';

  const movingEls = [];

  for (const m of result.moves) {
    const el = createTile(m.value, m.fromR, m.fromC);
    el.style.transition = `transform ${ANIM_MS}ms ease`;
    tileLayer.appendChild(el);

    const fromP = pos(m.fromR, m.fromC);
    const toP = pos(m.toR, m.toC);
    const dx = toP.x - fromP.x;
    const dy = toP.y - fromP.y;

    movingEls.push({ el, dx, dy });
  }

  requestAnimationFrame(() => {
    for (const m of movingEls) {
      m.el.style.transform = `translate(${m.dx}px, ${m.dy}px)`;
    }
  });
}

function move(direction) {
  if (animating) return;

  const result = computeMove(direction, grid);
  if (!result.moved) return;

  animating = true;
  score += result.gain;
  animateMove(result);

  setTimeout(() => {
    grid = cloneGrid(result.next);

    const spawn = addRandomTileToState(grid);
    const spawnSet = new Set();
    if (spawn) spawnSet.add(`${spawn.r}-${spawn.c}`);

    renderTiles(grid, spawnSet);
    renderScore();

    if (!hasWon) {
      outer: for (let r = 0; r < SIZE; r++) {
        for (let c = 0; c < SIZE; c++) {
          if (grid[r][c] === 2048) {
            hasWon = true;
            showOverlay('2048 달성!', '계속해서 더 높은 점수에 도전해봐!', false);
            break outer;
          }
        }
      }
    }

    if (!canMove()) {
      showOverlay('게임 오버', '더 이상 움직일 수 없어. 다시 도전!', true);
    }

    animating = false;
  }, ANIM_MS + 10);
}

window.addEventListener('keydown', (e) => {
  const map = {
    ArrowLeft: 'left',
    ArrowRight: 'right',
    ArrowUp: 'up',
    ArrowDown: 'down'
  };

  if (!map[e.key]) return;
  e.preventDefault();
  move(map[e.key]);
});

let sx = 0;
let sy = 0;
boardEl.addEventListener('touchstart', (e) => {
  const t = e.changedTouches[0];
  sx = t.clientX;
  sy = t.clientY;
}, { passive: true });

boardEl.addEventListener('touchend', (e) => {
  const t = e.changedTouches[0];
  const dx = t.clientX - sx;
  const dy = t.clientY - sy;
  const absX = Math.abs(dx);
  const absY = Math.abs(dy);

  if (Math.max(absX, absY) < 24) return;

  if (absX > absY) move(dx > 0 ? 'right' : 'left');
  else move(dy > 0 ? 'down' : 'up');
}, { passive: true });

window.addEventListener('resize', () => {
  renderTiles(grid);
});

document.getElementById('newGameBtn').addEventListener('click', initGame);
document.getElementById('retryBtn').addEventListener('click', initGame);

bestEl.textContent = best;
setupBoardDOM();
initGame();
