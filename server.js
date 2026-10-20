/* MEDIEVAL CRAFT · мультиплеер-сервер (Render + GitHub).
   Мир генерируется из сида на каждом клиенте, сервер хранит только СИД и ИЗМЕНЕНИЯ блоков + пересылает игроков и чат.
   Запуск: npm i && node server.js   (Render сам задаёт PORT)
   Если рядом лежит index.html (игра) — сервер отдаёт её по адресу сайта, и адрес сервера в игре подставится сам.

   МОБЫ: ландшафт и ИИ мобов живут в самой игре, поэтому сервер работает как «диспетчер зон»:
   · раз в 0.5 с сервер смотрит, где игроки, и назначает в каждой зоне ОДНОГО «хозяина» — его игра считает мобов;
   · хозяин шлёт состояние мобов (5 раз/с) — сервер пересылает его всем игрокам рядом, они видят мобов как обычных;
   · удары по чужим мобам идут через сервер к хозяину (сервер проверяет дистанцию и частоту ударов);
   · если хозяин вышел или игроки разошлись, зона достаётся другому игроку. */
const http=require('http'),fs=require('fs'),path=require('path'),{WebSocketServer}=require('ws');
const PORT=process.env.PORT||8080,MAXP=+process.env.MAX_PLAYERS||8,MAXB=400000,KEEP=6*3600e3,
 SAVE=path.join(__dirname,'rooms.json'),GAME=path.join(__dirname,'index.html'),CH='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const rooms=new Map();let nid=1;
const R_IN=32,R_OUT=48,SPAWNOK=new Set(['pig','zombie','skel','vil','kn','spw','sil','mar','gob']);
/* --- сохранение комнат на диск (на бесплатном Render диск стирается при перезапуске — это нормально) --- */
try{const o=JSON.parse(fs.readFileSync(SAVE));for(const[k,r]of Object.entries(o))rooms.set(k,{pub:!!r.pub,hn:r.hn||'Игрок',seed:r.seed,flat:r.flat,surv:r.surv,adm:r.adm|0,blocks:new Map(r.blocks),players:new Map(),empty:Date.now(),clk:0})}catch(e){}
const saveRooms=()=>{try{const o={};for(const[k,r]of rooms)o[k]={pub:r.pub?1:0,hn:r.hn,seed:r.seed,flat:r.flat,surv:r.surv,adm:r.adm|0,blocks:[...r.blocks]};fs.writeFileSync(SAVE,JSON.stringify(o))}catch(e){}};
setInterval(saveRooms,30000).unref();
setInterval(()=>{const n=Date.now();for(const[k,r]of rooms)if(!r.players.size&&n-r.empty>KEEP)rooms.delete(k)},60000).unref();

const srv=http.createServer((q,r)=>{
 if(q.url=='/rooms'){r.writeHead(200,{'Content-Type':'application/json','Access-Control-Allow-Origin':'*','Cache-Control':'no-store'});
  return r.end(JSON.stringify([...rooms.values()].filter(o=>o.pub&&o.players.size).map(o=>({code:o.code,name:o.hn,n:o.players.size,max:MAXP,surv:!!o.surv,flat:!!o.flat}))))}
 if(q.url=='/health'){r.writeHead(200,{'Content-Type':'text/plain'});return r.end('ok')}
 if((q.url=='/'||q.url=='/index.html')&&fs.existsSync(GAME)){r.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-cache'});return fs.createReadStream(GAME).pipe(r)}
 r.writeHead(200,{'Content-Type':'text/plain'});r.end('Medieval Craft server ok · rooms: '+rooms.size)});
const wss=new WebSocketServer({server:srv,maxPayload:128*1024});

const okS=s=>s&&typeof s.d=='string'&&s.d.startsWith('data:image/png;base64,')&&s.d.length<90000?{d:s.d,sl:s.sl?1:0}:null;
const num=(v,a,b)=>typeof v=='number'&&isFinite(v)&&v>=a&&v<=b;
const code=()=>{let c;do{c=Array.from({length:4},()=>CH[Math.random()*CH.length|0]).join('')}while(rooms.has(c));return c};
const tx=(w,o)=>{if(w.readyState==1)w.send(JSON.stringify(o))};
const sendBlocks=(w,r)=>{const all=[...r.blocks].map(([k,v])=>{const q=k.split(',').map(Number);return[q[0],q[1],q[2],v[0],v[1],q[3]|0]});
 for(let i=0;i<all.length;i+=1500)tx(w,{t:'b',l:all.slice(i,i+1500),init:1})};
/* --- хозяева зон: в id-порядке; игрок либо примыкает к ближайшему хозяину (<R_IN), либо сам становится хозяином.
       Уже примкнувший остаётся с хозяином, пока тот ближе R_OUT (чтобы зона не «дрожала»). --- */
function assign(r){
 const ps=[...r.players.values()].filter(p=>p.hasPos).sort((a,b)=>a.id-b.id),owners=[];
 for(const p of ps){
  const d=q=>q.dm===p.dm?Math.hypot(q.x-p.x,q.z-p.z):1e9;let by=null;const cur=p.by&&r.players.get(p.by);
  if(cur&&cur!==p&&owners.includes(cur)&&d(cur)<R_OUT)by=cur;
  if(!by)for(const q of owners)if(d(q)<R_IN){by=q;break}
  if(!by){by=p;owners.push(p)}
  const own=by===p;p.own=own;if(!own)p.mobs=null;
  if(p.by!==by.id||p.ownSent!==own){p.by=by.id;p.ownSent=own;tx(p.ws,{t:'own',own,by:by.id})}}}
setInterval(()=>{for(const r of rooms.values())if(r.players.size)assign(r)},500).unref();

wss.on('connection',ws=>{
 let room=null,id=0,me=null,lastP=0;ws.alive=true;ws.on('pong',()=>ws.alive=true);
 const bc=(o,skip)=>{for(const p of room.players.values())if(p.ws!==skip)tx(p.ws,o)};
 const enter=(r,name,t,sk,cr)=>{
  if(r.players.size>=MAXP)return tx(ws,{t:'err',m:'Комната заполнена'});
  room=r;id=nid++;me={ws,id,name:String(name||'Игрок').replace(/[<>]/g,'').slice(0,14)||'Игрок',x:0,y:0,z:0,yaw:0,pitch:0,host:!r.players.size,hp:20,lh:0,ld:0,sn:0,st:0,dm:0,by:0,own:true,ownSent:true,hasPos:0,mobs:null,lmb:0,lmh:0,lsp:0,skin:okS(sk),ad:r.adm==2||(r.adm==1&&!!cr)};
  tx(ws,{t,room:r.code,id,seed:r.seed,flat:r.flat,surv:r.surv,adm:me.ad?1:0,host:me.host,
   players:[...r.players.values()].map(p=>({id:p.id,name:p.name,x:p.x,y:p.y,z:p.z,yaw:p.yaw,pitch:p.pitch,hp:p.hp,skin:p.skin}))});
  /* изменения блоков отправляем пачками — мир у присоединившегося восстановится */
  sendBlocks(ws,r);
  r.players.set(id,me);bc({t:'join',id,name:me.name,skin:me.skin},ws)};
 ws.on('message',m=>{let d;try{d=JSON.parse(m)}catch(e){return}
  if(d.t=='ping')return tx(ws,{t:'pong'});
  if(!room){
   if(d.t=='host'){const r={code:code(),pub:!!d.pub,hn:String(d.name||'Игрок').replace(/[<>]/g,'').slice(0,14)||'Игрок',seed:d.seed|0,flat:!!d.flat,surv:!!d.surv,adm:[0,1,2].includes(d.adm)?d.adm:0,blocks:new Map(),players:new Map(),empty:Date.now(),clk:0};rooms.set(r.code,r);return enter(r,d.name,'hosted',d.skin,1)}
   if(d.t=='join'){const r=rooms.get(String(d.room||'').toUpperCase().trim());return r?enter(r,d.name,'joined',d.skin):tx(ws,{t:'err',m:'Комната не найдена'})}
   return}
  if(d.t=='p'){const n=Date.now();if(n-lastP<40)return;lastP=n;
   if(!num(d.x,-3e7,3e7)||!num(d.y,-100,500)||!num(d.z,-3e7,3e7)||!num(d.yaw,-100,100)||!num(d.pitch,-10,10))return;
   me.x=d.x;me.y=d.y;me.z=d.z;me.yaw=d.yaw;me.pitch=d.pitch;me.dm=d.dm?1:0;me.hasPos=1;
   me.hp=num(d.hp,0,40)?d.hp:20;me.sn=d.sn?1:0;me.st=d.st?1:0;
   return bc({t:'p',id,x:d.x,y:d.y,z:d.z,yaw:d.yaw,pitch:d.pitch,hp:me.hp,sn:me.sn,st:me.st,dm:me.dm},ws)}
  if(d.t=='b'&&Array.isArray(d.l)){const out=[];
   for(const v of d.l.slice(0,500)){if(!Array.isArray(v))continue;const[x,y,z,b,md,dm]=v;
    if(!num(x,-3e7,3e7)||!num(y,1,87)||!num(z,-3e7,3e7)||!num(b,0,65535))continue;
    const m2=md&&typeof md=='object'&&JSON.stringify(md).length<240?md:0,k=(x|0)+','+(y|0)+','+(z|0)+(dm?',1':''),o=room.blocks.get(k);
    if(o&&o[0]==b&&JSON.stringify(o[1])==JSON.stringify(m2))continue;                 /* дубликат — не рассылаем */
    if(!o&&room.blocks.size>=MAXB)continue;
    room.blocks.set(k,[b|0,m2]);out.push([x|0,y|0,z|0,b|0,m2,dm?1:0])}
   if(out.length)bc({t:'b',l:out},ws);return}
  if(d.t=='chat'&&typeof d.m=='string'){const s=d.m.replace(/[<>]/g,'').trim().slice(0,120);if(s)bc({t:'chat',id,name:me.name,m:s});return}
  if(d.t=='sync')return sendBlocks(ws,room);                         /* игра сменила измерение — просит блоки заново */
  /* --- мобы: состояние от хозяина зоны (≤5 раз/с) пересылаем всем рядом --- */
  if(d.t=='mobs'&&Array.isArray(d.l)){const n=Date.now();if(!me.own||n-me.lmb<70)return;me.lmb=n;
   const l=d.l.slice(0,220).filter(v=>Array.isArray(v)&&v.length>=9&&num(v[2],-3e7,3e7)&&num(v[3],-100,500)&&num(v[4],-3e7,3e7));
   me.mobs=new Map(l.map(v=>[v[0]|0,v]));const s=JSON.stringify({t:'mobs',src:id,l});
   for(const p of room.players.values())if(p!==me&&p.ws.readyState==1&&(p.by===id||(p.dm===me.dm&&Math.hypot(p.x-me.x,p.z-me.z)<110)))p.ws.send(s);return}
  /* удар по мобу, которого считает другой игрок: проверяем, что моб и правда рядом, и передаём хозяину */
  if(d.t=='mhit'){const o=room.players.get(d.src|0),n=Date.now();if(!o||o===me||!o.mobs||n-me.lmh<200)return;const m=o.mobs.get(d.mid|0);if(!m)return;
   if(Math.hypot(m[2]-me.x,m[4]-me.z)>10||Math.abs(m[3]-me.y)>9)return;me.lmh=n;
   const dm=Math.max(0,Math.min(30,d.dm|0)),kx=num(d.kx,-1.1,1.1)?d.kx:0,kz=num(d.kz,-1.1,1.1)?d.kz:0;
   tx(o.ws,{t:'mhit',from:id,mid:m[0],dm,kx,kz});return}
  /* игрок не-хозяин поставил яйцо моба — просим хозяина зоны создать его */
  if(d.t=='mspawn'){const o=room.players.get(me.by),n=Date.now();if(!o||o===me||!SPAWNOK.has(d.ty)||n-me.lsp<400)return;
   if(!num(d.x,-3e7,3e7)||!num(d.y,-100,500)||!num(d.z,-3e7,3e7))return;me.lsp=n;
   tx(o.ws,{t:'mspawn',from:id,ty:d.ty,x:d.x,y:d.y,z:d.z,bk:d.bk|0,vp:d.vp|0});return}
  /* добыча с моба, убитого другим игроком — отдаём убийце */
  if(d.t=='mdrop'){const p=room.players.get(d.to|0);if(!p||p===me||!num(d.id,0,65535)||!num(d.n,1,64)||!num(d.x,-3e7,3e7)||!num(d.y,-100,500)||!num(d.z,-3e7,3e7))return;
   tx(p.ws,{t:'mdrop',id:d.id|0,n:d.n|0,x:d.x,y:d.y,z:d.z});return}
  if(d.t=='sw')return bc({t:'sw',id},ws);
  if(d.t=='skin'){const k=okS(d);if(k){me.skin=k;bc({t:'skin',id,d:k.d,sl:k.sl},ws)}return}
  /* PvP: игроки — как мобы. Сервер проверяет дистанцию и частоту ударов и пересылает урон жертве */
  if(d.t=='hit'){if(!room.surv)return;const v=room.players.get(d.id|0),n=Date.now();if(!v||v===me||n-me.lh<300)return;
   if(Math.hypot(v.x-me.x,v.z-me.z)>7||Math.abs(v.y-me.y)>6)return;me.lh=n;
   const dm=Math.max(0,Math.min(14,d.dm|0)),kx=num(d.kx,-1.1,1.1)?d.kx:0,kz=num(d.kz,-1.1,1.1)?d.kz:0;
   tx(v.ws,{t:'hurt',from:id,name:me.name,dm,kx,kz});bc({t:'hit',id:v.id},v.ws);return}
  if(d.t=='died'){const n=Date.now();if(n-me.ld<3000)return;me.ld=n;const k=room.players.get(d.by|0),m=k&&k!==me?me.name+' убит игроком '+k.name:me.name+' погиб';
   for(const p of room.players.values())tx(p.ws,{t:'chat',id:0,name:'☠',m});return}
  if(d.t=='clk'&&me.host&&num(d.c,0,1e5))return bc({t:'clk',c:d.c},ws)});
 ws.on('close',()=>{if(!room)return;room.players.delete(id);bc({t:'leave',id});
  if(me.host&&room.players.size){const h=room.players.values().next().value;h.host=true;tx(h.ws,{t:'host'})}   /* хост ушёл — им становится следующий */
  if(!room.players.size){room.empty=Date.now();saveRooms()}})});
/* Render усыпляет и рвёт «мёртвые» соединения — проверяем живость каждые 30 с */
setInterval(()=>{for(const w of wss.clients){if(!w.alive){w.terminate();continue}w.alive=false;w.ping()}},30000).unref();
srv.listen(PORT,()=>console.log('MC server on :'+PORT));
