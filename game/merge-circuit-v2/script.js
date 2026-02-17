const SIZE=4;
let grid=[];
let score=0;
let best=Number(localStorage.getItem('mcv2-best')||0);
let revived=false;
let combo=1.0;
let missions=[];

const boardEl=document.getElementById('board');
const scoreEl=document.getElementById('score');
const bestEl=document.getElementById('best');
const comboEl=document.getElementById('combo');
const missionsEl=document.getElementById('missions');
const overlay=document.getElementById('overlay');
const result=document.getElementById('result');

function newMissions(){
  return [
    {id:'m64', text:'64 타일 만들기', done:false},
    {id:'mcombo', text:'3콤보 달성하기', done:false},
    {id:'m5turn', text:'5턴 연속 머지', done:false, chain:0},
  ];
}

function init(){
  grid=Array.from({length:SIZE},()=>Array(SIZE).fill(0));
  score=0; combo=1.0; revived=false; missions=newMissions();
  addRandom(); addRandom();
  overlay.classList.add('hidden');
  render();
}

function addRandom(){
  const empty=[];
  for(let r=0;r<SIZE;r++) for(let c=0;c<SIZE;c++) if(grid[r][c]===0) empty.push([r,c]);
  if(!empty.length) return;
  const [r,c]=empty[Math.floor(Math.random()*empty.length)];
  grid[r][c]=Math.random()<0.9?2:4;
}

function slide(line){
  const a=line.filter(v=>v!==0);
  let merged=false, gain=0;
  for(let i=0;i<a.length-1;i++){
    if(a[i]===a[i+1]){a[i]*=2; gain+=a[i]; a[i+1]=0; merged=true;}
  }
  const out=a.filter(v=>v!==0);
  while(out.length<SIZE) out.push(0);
  return {out,gain,merged};
}

function transpose(){
  const n=Array.from({length:SIZE},()=>Array(SIZE).fill(0));
  for(let r=0;r<SIZE;r++) for(let c=0;c<SIZE;c++) n[c][r]=grid[r][c];
  grid=n;
}
function reverseRows(){for(let r=0;r<SIZE;r++) grid[r].reverse();}

function move(dir){
  const before=JSON.stringify(grid);
  let anyMerged=false, gain=0;

  const left=()=>{for(let r=0;r<SIZE;r++){const x=slide(grid[r]);grid[r]=x.out;gain+=x.gain;anyMerged=anyMerged||x.merged;}};

  if(dir==='left') left();
  if(dir==='right'){reverseRows(); left(); reverseRows();}
  if(dir==='up'){transpose(); left(); transpose();}
  if(dir==='down'){transpose(); reverseRows(); left(); reverseRows(); transpose();}

  if(before===JSON.stringify(grid)) return;

  if(anyMerged){
    combo=Math.min(1.5, +(combo+0.1).toFixed(1));
    missions.find(m=>m.id==='m5turn').chain++;
  }else{
    combo=1.0;
    missions.find(m=>m.id==='m5turn').chain=0;
  }

  score += Math.floor(gain*combo + 1);
  addRandom();
  updateMissions();
  render();

  if(!canMove()) gameOver();
}

function canMove(){
  for(let r=0;r<SIZE;r++) for(let c=0;c<SIZE;c++){
    if(grid[r][c]===0) return true;
    if(c<SIZE-1 && grid[r][c]===grid[r][c+1]) return true;
    if(r<SIZE-1 && grid[r][c]===grid[r+1][c]) return true;
  }
  return false;
}

function updateMissions(){
  let maxTile=0;
  for(const row of grid) for(const v of row) maxTile=Math.max(maxTile,v);
  const m64=missions.find(m=>m.id==='m64'); if(maxTile>=64) m64.done=true;
  const mcombo=missions.find(m=>m.id==='mcombo'); if(combo>=1.3) mcombo.done=true;
  const m5=missions.find(m=>m.id==='m5turn'); if(m5.chain>=5) m5.done=true;

  const doneCount=missions.filter(m=>m.done).length;
  score += doneCount*2;
}

function render(){
  boardEl.innerHTML='';
  for(let r=0;r<SIZE;r++) for(let c=0;c<SIZE;c++){
    const v=grid[r][c];
    const d=document.createElement('div');
    d.className='cell '+(v>2048?'vbig':'v'+v);
    d.textContent=v||'';
    boardEl.appendChild(d);
  }
  scoreEl.textContent=Math.floor(score);
  if(score>best){best=score; localStorage.setItem('mcv2-best',String(Math.floor(best)));}
  bestEl.textContent=Math.floor(best);
  comboEl.textContent='x'+combo.toFixed(1);

  missionsEl.innerHTML='<b>미션</b><ul>'+missions.map(m=>`<li class='${m.done?'done':''}'>${m.text}${m.id==='m5turn' && !m.done ? ` (${m.chain||0}/5)` : ''}</li>`).join('')+'</ul>';
}

function gameOver(){
  overlay.classList.remove('hidden');
  result.textContent=`점수 ${Math.floor(score)} / 최고 ${Math.floor(best)}`;
  document.getElementById('reviveBtn').disabled=revived;
}

function revive(){
  if(revived) return;
  revived=true;
  score*=0.9;
  // 랜덤 2칸 비우기
  const filled=[];
  for(let r=0;r<SIZE;r++) for(let c=0;c<SIZE;c++) if(grid[r][c]!==0) filled.push([r,c]);
  for(let i=0;i<2 && filled.length;i++){
    const idx=Math.floor(Math.random()*filled.length);
    const [r,c]=filled.splice(idx,1)[0];
    grid[r][c]=0;
  }
  overlay.classList.add('hidden');
  render();
}

window.addEventListener('keydown',e=>{
  const m={ArrowLeft:'left',ArrowRight:'right',ArrowUp:'up',ArrowDown:'down'};
  if(m[e.key]){e.preventDefault();move(m[e.key]);}
});

let sx=0,sy=0;
boardEl.addEventListener('touchstart',e=>{const t=e.changedTouches[0];sx=t.clientX;sy=t.clientY;},{passive:true});
boardEl.addEventListener('touchend',e=>{const t=e.changedTouches[0];const dx=t.clientX-sx;const dy=t.clientY-sy;if(Math.max(Math.abs(dx),Math.abs(dy))<20)return; if(Math.abs(dx)>Math.abs(dy)) move(dx>0?'right':'left'); else move(dy>0?'down':'up');},{passive:true});

document.getElementById('newBtn').addEventListener('click',init);
document.getElementById('retryBtn').addEventListener('click',init);
document.getElementById('reviveBtn').addEventListener('click',revive);

init();