const W=6,H=6,C=5;
let g=[],score=0,combo=1,shuffle=1;
const board=document.getElementById('board');
const scoreEl=document.getElementById('score');
const comboEl=document.getElementById('combo');
const shuffleEl=document.getElementById('shuffle');
const msg=document.getElementById('msg');

function rnd(n){return Math.floor(Math.random()*n)}
function init(){g=Array.from({length:H},()=>Array.from({length:W},()=>rnd(C)));score=0;combo=1;shuffle=1;render('탭해서 그룹 제거!');}
function inb(y,x){return y>=0&&x>=0&&y<H&&x<W}
function group(y,x){
  const color=g[y][x]; if(color===null) return [];
  const q=[[y,x]],v=new Set([`${y},${x}`]),out=[];
  while(q.length){const [cy,cx]=q.pop(); out.push([cy,cx]); [[1,0],[-1,0],[0,1],[0,-1]].forEach(([dy,dx])=>{const ny=cy+dy,nx=cx+dx,k=`${ny},${nx}`; if(inb(ny,nx)&&!v.has(k)&&g[ny][nx]===color){v.add(k);q.push([ny,nx]);}})}
  return out;
}
function collapse(){
  for(let x=0;x<W;x++){
    const col=[]; for(let y=H-1;y>=0;y--) if(g[y][x]!==null) col.push(g[y][x]);
    for(let y=H-1,i=0;y>=0;y--,i++) g[y][x]= i<col.length?col[i]:rnd(C);
  }
}
function tap(y,x){
  const gp=group(y,x);
  if(gp.length<3){combo=1;render('3개 이상 붙은 곳을 누르자');return;}
  gp.forEach(([yy,xx])=>g[yy][xx]=null);
  score += gp.length*10*combo;
  combo=Math.min(9,combo+1);
  collapse();
  render(`${gp.length}개 제거!`);
}
function hasMove(){for(let y=0;y<H;y++)for(let x=0;x<W;x++) if(group(y,x).length>=3) return true; return false;}
function doShuffle(){if(shuffle<=0)return; shuffle--; for(let y=0;y<H;y++)for(let x=0;x<W;x++) g[y][x]=rnd(C); combo=1; render('셔플 완료');}
function render(t=''){
  board.innerHTML='';
  for(let y=0;y<H;y++)for(let x=0;x<W;x++){
    const b=document.createElement('button'); b.className='cell c'+g[y][x]; b.onclick=()=>tap(y,x); board.appendChild(b);
  }
  scoreEl.textContent=score; comboEl.textContent='x'+combo; shuffleEl.textContent=shuffle;
  if(!hasMove()) msg.textContent='막힘! 셔플 버튼으로 이어가기'; else msg.textContent=t||'연쇄 노려보자';
}

document.getElementById('restart').onclick=init;
document.getElementById('shuffleBtn').onclick=doShuffle;
init();