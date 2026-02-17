const game = document.getElementById('game');
const scoreEl = document.getElementById('score');
const hpEl = document.getElementById('hp');
const levelEl = document.getElementById('level');
const overlay = document.getElementById('overlay');
const summary = document.getElementById('summary');

const LANE_COUNT = 3;
let laneX = [];
let playerLane = 1;
let score = 0;
let hp = 3;
let level = 1;
let speed = 3.2;
let spawnTick = 0;
let running = true;
let objects = [];

const player = document.createElement('div');
player.className = 'player';
game.appendChild(player);

function setupLanes() {
  laneX = [];
  document.querySelectorAll('.lane').forEach(x => x.remove());
  const w = game.clientWidth;
  for (let i = 0; i < LANE_COUNT; i++) {
    laneX.push((w / LANE_COUNT) * i + (w / LANE_COUNT) / 2);
    if (i > 0) {
      const line = document.createElement('div');
      line.className = 'lane';
      line.style.left = `${(w / LANE_COUNT) * i}px`;
      game.appendChild(line);
    }
  }
  updatePlayerPos();
}

function updatePlayerPos() {
  player.style.left = `${laneX[playerLane]}px`;
}

function spawn(kind) {
  const lane = Math.floor(Math.random() * LANE_COUNT);
  const el = document.createElement('div');
  el.className = kind;
  el.style.left = `${laneX[lane] - 20}px`;
  game.appendChild(el);
  objects.push({ kind, lane, y: -45, el });
}

function intersects(aY, lane) {
  const playerY = game.clientHeight - 70;
  return lane === playerLane && Math.abs(aY - playerY) < 36;
}

function loop() {
  if (!running) return;
  spawnTick++;

  if (spawnTick % Math.max(22 - level, 8) === 0) {
    spawn(Math.random() < 0.68 ? 'enemy' : 'coin');
  }

  objects.forEach(o => {
    o.y += speed;
    o.el.style.top = `${o.y}px`;

    if (intersects(o.y, o.lane)) {
      if (o.kind === 'enemy') {
        hp--;
        player.classList.add('flash');
        setTimeout(() => player.classList.remove('flash'), 120);
      } else {
        score += 10;
      }
      o.y = game.clientHeight + 60;
    }

    if (o.y > game.clientHeight + 50) {
      if (o.kind === 'enemy') score += 2;
    }
  });

  objects = objects.filter(o => {
    if (o.y > game.clientHeight + 80) {
      o.el.remove();
      return false;
    }
    return true;
  });

  score += 0.12;
  level = Math.min(15, 1 + Math.floor(score / 80));
  speed = 3.2 + level * 0.26;

  scoreEl.textContent = Math.floor(score);
  hpEl.textContent = hp;
  levelEl.textContent = level;

  if (hp <= 0) return gameOver();
  requestAnimationFrame(loop);
}

function gameOver() {
  running = false;
  summary.textContent = `점수 ${Math.floor(score)} / 레벨 ${level}`;
  overlay.classList.remove('hidden');
}

function reset() {
  objects.forEach(o => o.el.remove());
  objects = [];
  playerLane = 1;
  score = 0;
  hp = 3;
  level = 1;
  speed = 3.2;
  spawnTick = 0;
  running = true;
  overlay.classList.add('hidden');
  setupLanes();
  loop();
}

window.addEventListener('resize', setupLanes);

window.addEventListener('keydown', e => {
  if (!running) return;
  if (e.key === 'ArrowLeft' || e.key === 'a') playerLane = Math.max(0, playerLane - 1);
  if (e.key === 'ArrowRight' || e.key === 'd') playerLane = Math.min(LANE_COUNT - 1, playerLane + 1);
  updatePlayerPos();
});

let sx = 0;
game.addEventListener('touchstart', e => { sx = e.changedTouches[0].clientX; }, {passive:true});
game.addEventListener('touchend', e => {
  if (!running) return;
  const dx = e.changedTouches[0].clientX - sx;
  if (Math.abs(dx) < 18) return;
  if (dx > 0) playerLane = Math.min(LANE_COUNT - 1, playerLane + 1);
  else playerLane = Math.max(0, playerLane - 1);
  updatePlayerPos();
}, {passive:true});

document.getElementById('retryBtn').addEventListener('click', reset);
document.getElementById('restartBtn').addEventListener('click', reset);

setupLanes();
loop();