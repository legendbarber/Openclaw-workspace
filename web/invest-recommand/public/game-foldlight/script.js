const N=5;
let energy=12;
let board=[];
let source={r:4,c:0,dir:0}; // 0→ 1↓ 2← 3↑
const anchors=new Set(['2,2']);
const cores=new Set(['0,4','2,4']);
const filters=new Set(['1,2','3,3']);
const noises=new Set(['1,4','3,1']);

const elBoard=document.getElementById('board');
const elEnergy=document.getElementById('energy');
const elLit=document.getElementById('lit');
const elLog=document.getElementById('log');

function key(r,c){return `${r},${c}`}
function init(){
  board=Array.from({length:N},()=>Array.from({length:N},()=>({rot:0,lit:false})));
  energy=12; render('시작: 코어 2개를 점등해봐.');
}

function iconAt(r,c){
  const k=key(r,c);
  if(source.r===r && source.c===c) return ['→','↓','←','↑'][source.dir];
  if(cores.has(k)) return board[r][c].lit?'☀':'◉';
  if(filters.has(k)) return ['▸','▾','◂','▴'][board[r][c].rot%4];
  if(noises.has(k)) return '✶';
  if(anchors.has(k)) return '◆';
  return '';
}

function classAt(r,c){
  const k=key(r,c);
  let cls='cell';
  if(source.r===r && source.c===c) cls+=' source';
  if(cores.has(k)) cls+=' core'+(board[r][c].lit?' lit':'');
  if(filters.has(k)) cls+=' filter';
  if(noises.has(k)) cls+=' noise';
  if(anchors.has(k)) cls+=' anchor';
  return cls;
}

function render(msg=''){
  elBoard.innerHTML='';
  for(let r=0;r<N;r++)for(let c=0;c<N;c++){
    const d=document.createElement('div');
    d.className=classAt(r,c);
    d.textContent=iconAt(r,c);
    d.onclick=()=>tap(r,c);
    elBoard.appendChild(d);
  }
  const lit=[...cores].filter(k=>{const [r,c]=k.split(',').map(Number); return board[r][c].lit;}).length;
  elEnergy.textContent=energy;
  elLit.textContent=`${lit}/2`;
  if(msg) elLog.textContent=msg;
  if(lit>=2){elLog.textContent='클리어! 독창 규칙 OK. 다음은 스테이지 확장 가능';}
  if(energy<=0 && lit<2){elLog.textContent='에너지 소진. 리셋해서 다시.';}
}

function tap(r,c){
  if(energy<=0) return;
  const k=key(r,c);
  if(filters.has(k)){board[r][c].rot=(board[r][c].rot+1)%4; energy--; render('필터 회전'); return;}
  if(source.r===r&&source.c===c){source.dir=(source.dir+1)%4; energy--; render('광원 방향 변경');}
}

function clearLit(){
  for(const k of cores){const [r,c]=k.split(',').map(Number); board[r][c].lit=false;}
}

function pulse(){
  if(energy<=0) return;
  energy--; clearLit();
  let r=source.r,c=source.c,dir=source.dir,power=3;
  for(let step=0;step<16 && power>0;step++){
    if(dir===0)c++; if(dir===1)r++; if(dir===2)c--; if(dir===3)r--;
    if(r<0||c<0||r>=N||c>=N) break;
    const k=key(r,c);
    if(noises.has(k)) power--;
    if(filters.has(k)){
      const rot=board[r][c].rot%4;
      dir=(dir+rot+1)%4; // 필터가 경로를 굴절
    }
    if(cores.has(k) && power>0){ board[r][c].lit=true; power--; }
  }
  render('빛 발사');
}

function foldH(){
  if(energy<=0) return;
  energy--;
  const next=Array.from({length:N},()=>Array.from({length:N},()=>({rot:0,lit:false})));
  for(let r=0;r<N;r++)for(let c=0;c<N;c++){
    const nr = r<2 ? r : (r===2?2: 4-r); // 아래를 위로 접기
    next[nr][c]=board[r][c];
  }
  board=next;
  if(source.r>2) source.r=4-source.r;
  render('가로 Fold 적용');
}

function foldV(){
  if(energy<=0) return;
  energy--;
  const next=Array.from({length:N},()=>Array.from({length:N},()=>({rot:0,lit:false})));
  for(let r=0;r<N;r++)for(let c=0;c<N;c++){
    const nc = c<2 ? c : (c===2?2:4-c); // 오른쪽을 왼쪽으로 접기
    next[r][nc]=board[r][c];
  }
  board=next;
  if(source.c>2) source.c=4-source.c;
  render('세로 Fold 적용');
}

document.getElementById('pulse').onclick=pulse;
document.getElementById('foldH').onclick=foldH;
document.getElementById('foldV').onclick=foldV;
document.getElementById('reset').onclick=init;

init();