const N=5;
let level=1,moves=0,grid=[];
const board=document.getElementById('board');
const levelEl=document.getElementById('level');
const movesEl=document.getElementById('moves');
const bestEl=document.getElementById('best');
const msg=document.getElementById('msg');

function keyBest(l){return `taplights-best-${l}`}
function best(l){return Number(localStorage.getItem(keyBest(l))||0)}
function setBest(l,v){localStorage.setItem(keyBest(l),String(v))}

function emptyGrid(){return Array.from({length:N},()=>Array(N).fill(false));}

function toggle(r,c){
  [[r,c],[r-1,c],[r+1,c],[r,c-1],[r,c+1]].forEach(([y,x])=>{
    if(y>=0&&x>=0&&y<N&&x<N) grid[y][x]=!grid[y][x];
  });
}

function buildLevel(l){
  grid=emptyGrid();
  // 난이도: 레벨이 올라갈수록 섞는 횟수 증가
  const shuffles=Math.min(4+l*2, 26);
  for(let i=0;i<shuffles;i++){
    toggle(Math.floor(Math.random()*N), Math.floor(Math.random()*N));
  }
  moves=0;
  render();
}

function solved(){
  for(let r=0;r<N;r++) for(let c=0;c<N;c++) if(grid[r][c]) return false;
  return true;
}

function tap(r,c){
  toggle(r,c);
  moves++;
  render();
  if(solved()){
    const b=best(level);
    if(!b || moves<b) setBest(level,moves);
    msg.textContent=`클리어! (${moves} moves) ${(!b||moves<b)?'신기록!':''}`;
  }
}

function render(){
  levelEl.textContent=level;
  movesEl.textContent=moves;
  const b=best(level); bestEl.textContent=b?`${b} moves`:'-';

  board.innerHTML='';
  for(let r=0;r<N;r++) for(let c=0;c<N;c++){
    const d=document.createElement('button');
    d.className='cell'+(grid[r][c]?' on':'');
    d.onclick=()=>tap(r,c);
    board.appendChild(d);
  }
}

document.getElementById('restart').onclick=()=>buildLevel(level);
document.getElementById('prev').onclick=()=>{level=Math.max(1,level-1);buildLevel(level);msg.textContent='이전 레벨';};
document.getElementById('next').onclick=()=>{level=Math.min(50,level+1);buildLevel(level);msg.textContent='다음 레벨';};

buildLevel(level);