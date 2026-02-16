const SIZE = 4;
let grid = [];
let score = 0;
let best = Number(localStorage.getItem('best-2048') || 0);
let hasWon = false;

const boardEl = document.getElementById('board');
const scoreEl = document.getElementById('score');
const bestEl = document.getElementById('best');
const overlayEl = document.getElementById('overlay');
const overlayTitleEl = document.getElementById('overlayTitle');
const overlayMsgEl = document.getElementById('overlayMsg');

function initGame() {
  grid = Array.from({ length: SIZE }, () => Array(SIZE).fill(0));
  score = 0;
  hasWon = false;
  hideOverlay();
  addRandomTile();
  addRandomTile();
  render();
}

function addRandomTile() {
  const empties = [];
  for (let r = 0; r < SIZE; r++) {
    for (let c = 0; c < SIZE; c++) {
      if (grid[r][c] === 0) empties.push([r, c]);
    }
  }
  if (!empties.length) return;
  const [r, c] = empties[Math.floor(Math.random() * empties.length)];
  grid[r][c] = Math.random() < 0.9 ? 2 : 4;
}

function render() {
  boardEl.innerHTML = '';
  for (let r = 0; r < SIZE; r++) {
    for (let c = 0; c < SIZE; c++) {
      const v = grid[r][c];
      const cell = document.createElement('div');
      cell.className = `cell ${v > 2048 ? 'v-big' : `v-${v}`}`;
      cell.textContent = v === 0 ? '' : v;
      boardEl.appendChild(cell);
    }
  }

  scoreEl.textContent = score;
  if (score > best) {
    best = score;
    localStorage.setItem('best-2048', String(best));
  }
  bestEl.textContent = best;
}

function slideAndMergeLine(line) {
  const compressed = line.filter(v => v !== 0);
  let gained = 0;

  for (let i = 0; i < compressed.length - 1; i++) {
    if (compressed[i] === compressed[i + 1]) {
      compressed[i] *= 2;
      gained += compressed[i];
      compressed[i + 1] = 0;

      if (compressed[i] === 2048 && !hasWon) {
        hasWon = true;
        showOverlay('2048 달성!', '계속해서 더 높은 점수에 도전해봐!', false);
      }
    }
  }

  const merged = compressed.filter(v => v !== 0);
  while (merged.length < SIZE) merged.push(0);

  return { line: merged, gained };
}

function moveLeft() {
  let moved = false;
  let gainedTotal = 0;

  for (let r = 0; r < SIZE; r++) {
    const original = [...grid[r]];
    const { line, gained } = slideAndMergeLine(original);
    grid[r] = line;
    gainedTotal += gained;
    if (!arraysEqual(original, line)) moved = true;
  }

  score += gainedTotal;
  return moved;
}

function reverseRows() {
  for (let r = 0; r < SIZE; r++) grid[r].reverse();
}

function transpose() {
  const newGrid = Array.from({ length: SIZE }, () => Array(SIZE).fill(0));
  for (let r = 0; r < SIZE; r++) {
    for (let c = 0; c < SIZE; c++) {
      newGrid[c][r] = grid[r][c];
    }
  }
  grid = newGrid;
}

function move(direction) {
  let moved = false;

  if (direction === 'left') moved = moveLeft();

  if (direction === 'right') {
    reverseRows();
    moved = moveLeft();
    reverseRows();
  }

  if (direction === 'up') {
    transpose();
    moved = moveLeft();
    transpose();
  }

  if (direction === 'down') {
    transpose();
    reverseRows();
    moved = moveLeft();
    reverseRows();
    transpose();
  }

  if (moved) {
    addRandomTile();
    render();
    if (!canMove()) {
      showOverlay('게임 오버', '더 이상 움직일 수 없어. 다시 도전!', true);
    }
  }
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

function arraysEqual(a, b) {
  return a.length === b.length && a.every((v, i) => v === b[i]);
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

  if (absX > absY) {
    move(dx > 0 ? 'right' : 'left');
  } else {
    move(dy > 0 ? 'down' : 'up');
  }
}, { passive: true });

document.getElementById('newGameBtn').addEventListener('click', initGame);
document.getElementById('retryBtn').addEventListener('click', initGame);

bestEl.textContent = best;
initGame();
