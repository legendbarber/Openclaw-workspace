const N=6;
const symbols=['★','●','▲','■'];
let board=[],turns=12,stage=1;
let mode='row'; // row|col
let selected=0;

const boardEl=document.getElementById('board');
const turnsEl=document.getElementById('turns');
const stageEl=document.getElementById('stage');
const selectedEl=document.getElementById('selected');
const msg=document.getElementById('msg');

function rnd(n){return Math.floor(Math.random()*n)}
function makeBoard(){
  board=Array.from({length:N},()=>Array.from({length:N},()=>symbols[rnd(symbols.length)]));
  // 목표 달성 직전 느낌으로 약간 조정
  for(let i=0;i<3;i++) board[1][i]='★';
}

function init(){
  turns=12; selected=0; mode='row';
  document.getElementById('rowMode').classList.add('on');
  document.getElementById('colMode').classList.remove('on');
  makeBoard(); render('★ 4개를 상하좌우로 연결하면 승리');
}

function render(text=''){
  boardEl.innerHTML='';
  for(let r=0;r<N;r++) for(let c=0;c<N;c++){
    const d=document.createElement('button');
    const isSel = mode==='row' ? (r===selected) : (c===selected);
    d.className='cell'+(isSel?' sel':'');
    d.textContent=board[r][c];
    d.onclick=()=>{selected=(mode==='row'?r:c); render();};
    boardEl.appendChild(d);
  }
  turnsEl.textContent=turns; stageEl.textContent=stage; selectedEl.textContent=selected;
  if(text) msg.textContent=text;
  checkWin();
}

function shiftRow(r,dir){
  if(dir>0){ const t=board[r][N-1]; for(let c=N-1;c>0;c--) board[r][c]=board[r][c-1]; board[r][0]=t; }
  else { const t=board[r][0]; for(let c=0;c<N-1;c++) board[r][c]=board[r][c+1]; board[r][N-1]=t; }
}
function shiftCol(c,dir){
  if(dir>0){ const t=board[N-1][c]; for(let r=N-1;r>0;r--) board[r][c]=board[r-1][c]; board[0][c]=t; }
  else { const t=board[0][c]; for(let r=0;r<N-1;r++) board[r][c]=board[r+1][c]; board[N-1][c]=t; }
}

function act(dir){
  if(turns<=0) return;
  if(mode==='row') shiftRow(selected,dir==='right'?1:-1);
  else shiftCol(selected,dir==='down'?1:-1);
  turns--; render();
  if(turns<=0) msg.textContent='턴 소진! 리셋해서 다시 도전';
}

function checkWin(){
  const seen=Array.from({length:N},()=>Array(N).fill(false));
  for(let r=0;r<N;r++) for(let c=0;c<N;c++){
    if(board[r][c]!=='★' || seen[r][c]) continue;
    const q=[[r,c]]; seen[r][c]=true; let cnt=0;
    while(q.length){
      const [y,x]=q.pop(); cnt++;
      [[1,0],[-1,0],[0,1],[0,-1]].forEach(([dy,dx])=>{const ny=y+dy,nx=x+dx; if(ny>=0&&nx>=0&&ny<N&&nx<N&&!seen[ny][nx]&&board[ny][nx]==='★'){seen[ny][nx]=true;q.push([ny,nx]);}})
    }
    if(cnt>=4){
      msg.textContent=`클리어! 남은 턴 ${turns}`;
      turns=0; turnsEl.textContent=turns;
      return true;
    }
  }
  return false;
}

document.getElementById('rowMode').onclick=()=>{mode='row'; document.getElementById('rowMode').classList.add('on'); document.getElementById('colMode').classList.remove('on'); render('가로줄 선택 모드');};
document.getElementById('colMode').onclick=()=>{mode='col'; document.getElementById('colMode').classList.add('on'); document.getElementById('rowMode').classList.remove('on'); render('세로줄 선택 모드');};

document.getElementById('left').onclick=()=> mode==='row' && act('left');
document.getElementById('right').onclick=()=> mode==='row' && act('right');
document.getElementById('up').onclick=()=> mode==='col' && act('up');
document.getElementById('down').onclick=()=> mode==='col' && act('down');
document.getElementById('reset').onclick=init;

let sx=0,sy=0;
boardEl.addEventListener('touchstart',e=>{const t=e.changedTouches[0];sx=t.clientX;sy=t.clientY;},{passive:true});
boardEl.addEventListener('touchend',e=>{const t=e.changedTouches[0];const dx=t.clientX-sx,dy=t.clientY-sy; if(Math.max(Math.abs(dx),Math.abs(dy))<18)return; if(Math.abs(dx)>Math.abs(dy)){ if(mode==='row') act(dx>0?'right':'left'); } else { if(mode==='col') act(dy>0?'down':'up'); }},{passive:true});

init();