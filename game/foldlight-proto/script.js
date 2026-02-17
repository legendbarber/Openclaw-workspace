const N=5;
let energy=20;
let board=[];
let source={r:4,c:0,dir:0}; // 고정: 오른쪽으로 발사
const cores=new Set(['0,4']);
const filters=new Set(['1,2','3,3']);
const noises=new Set(['1,4','3,1']);

const elBoard=document.getElementById('board');
const elEnergy=document.getElementById('energy');
const elLit=document.getElementById('lit');
const elLog=document.getElementById('log');

function key(r,c){return `${r},${c}`}

function init(){
  board=Array.from({length:N},()=>Array.from({length:N},()=>({rot:0,lit:false})));
  energy=20;
  autoPulse();
  render('접기 버튼만 눌러서 코어를 밝혀봐.');
}

function iconAt(r,c){
  const k=key(r,c);
  if(source.r===r && source.c===c) return '→';
  if(cores.has(k)) return board[r][c].lit?'☀':'◉';
  if(filters.has(k)) return ['▸','▾','◂','▴'][board[r][c].rot%4];
  if(noises.has(k)) return '✶';
  return '';
}

function classAt(r,c){
  const k=key(r,c);
  let cls='cell';
  if(source.r===r && source.c===c) cls+=' source';
  if(cores.has(k)) cls+=' core'+(board[r][c].lit?' lit':'');
  if(filters.has(k)) cls+=' filter';
  if(noises.has(k)) cls+=' noise';
  return cls;
}

function render(msg=''){
  elBoard.innerHTML='';
  for(let r=0;r<N;r++)for(let c=0;c<N;c++){
    const d=document.createElement('div');
    d.className=classAt(r,c);
    d.textContent=iconAt(r,c);
    elBoard.appendChild(d);
  }
  const lit=[...cores].filter(k=>{const [r,c]=k.split(',').map(Number); return board[r][c].lit;}).length;
  elEnergy.textContent=energy;
  elLit.textContent=`${lit}/1`;
  if(msg) elLog.textContent=msg;
  if(lit>=1) elLog.textContent='클리어! 👍 (이게 직관형 베이스)';
  if(energy<=0 && lit<1) elLog.textContent='에너지 소진. 리셋해서 다시.';
}

function clearLit(){
  for(const k of cores){const [r,c]=k.split(',').map(Number); board[r][c].lit=false;}
}

function autoPulse(){
  clearLit();
  let r=source.r,c=source.c,dir=source.dir,power=3;
  for(let step=0;step<16 && power>0;step++){
    if(dir===0)c++; if(dir===1)r++; if(dir===2)c--; if(dir===3)r--;
    if(r<0||c<0||r>=N||c>=N) break;
    const k=key(r,c);
    if(noises.has(k)) power--;
    if(filters.has(k)){
      const rot=board[r][c].rot%4;
      dir=(dir+rot+1)%4;
    }
    if(cores.has(k) && power>0){ board[r][c].lit=true; power--; }
  }
}

function rotateFiltersAfterFold(){
  // 접을 때 필터를 자동 회전시켜서 변화를 직관적으로 체감
  for(const k of filters){
    const [r,c]=k.split(',').map(Number);
    board[r][c].rot=(board[r][c].rot+1)%4;
  }
}

function foldH(){
  if(energy<=0) return;
  energy--;
  const next=Array.from({length:N},()=>Array.from({length:N},()=>({rot:0,lit:false})));
  for(let r=0;r<N;r++)for(let c=0;c<N;c++){
    const nr = r<2 ? r : (r===2?2: 4-r);
    next[nr][c]=board[r][c];
  }
  board=next;
  if(source.r>2) source.r=4-source.r;
  rotateFiltersAfterFold();
  autoPulse();
  render('가로 접기 완료');
}

function foldV(){
  if(energy<=0) return;
  energy--;
  const next=Array.from({length:N},()=>Array.from({length:N},()=>({rot:0,lit:false})));
  for(let r=0;r<N;r++)for(let c=0;c<N;c++){
    const nc = c<2 ? c : (c===2?2:4-c);
    next[r][nc]=board[r][c];
  }
  board=next;
  if(source.c>2) source.c=4-source.c;
  rotateFiltersAfterFold();
  autoPulse();
  render('세로 접기 완료');
}

document.getElementById('foldH').onclick=foldH;
document.getElementById('foldV').onclick=foldV;
document.getElementById('reset').onclick=init;

init();