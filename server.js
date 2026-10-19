/* MEDIEVAL CRAFT · мультиплеер-сервер (Render + GitHub).
   Мир генерируется из сида на каждом клиенте, сервер хранит только СИД и ИЗМЕНЕНИЯ блоков + пересылает игроков и чат.
   Запуск: npm i && node server.js   (Render сам задаёт PORT)
   Если рядом лежит index.html (игра) — сервер отдаёт её по адресу сайта, и адрес сервера в игре подставится сам. */
const http=require('http'),fs=require('fs'),path=require('path'),{WebSocketServer}=require('ws');
const PORT=process.env.PORT||8080,MAXP=+process.env.MAX_PLAYERS||8,MAXB=400000,KEEP=6*3600e3,
 SAVE=path.join(__dirname,'rooms.json'),GAME=path.join(__dirname,'index.html'),CH='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const rooms=new Map();let nid=1;
/* --- сохранение комнат на диск (на бесплатном Render диск стирается при перезапуске — это нормально) --- */
try{const o=JSON.parse(fs.readFileSync(SAVE));for(const[k,r]of Object.entries(o))rooms.set(k,{seed:r.seed,flat:r.flat,surv:r.surv,blocks:new Map(r.blocks),players:new Map(),empty:Date.now(),clk:0})}catch(e){}
const saveRooms=()=>{try{const o={};for(const[k,r]of rooms)o[k]={seed:r.seed,flat:r.flat,surv:r.surv,blocks:[...r.blocks]};fs.writeFileSync(SAVE,JSON.stringify(o))}catch(e){}};
setInterval(saveRooms,30000).unref();
setInterval(()=>{const n=Date.now();for(const[k,r]of rooms)if(!r.players.size&&n-r.empty>KEEP)rooms.delete(k)},60000).unref();

const srv=http.createServer((q,r)=>{
 if(q.url=='/health'){r.writeHead(200,{'Content-Type':'text/plain'});return r.end('ok')}
 if((q.url=='/'||q.url=='/index.html')&&fs.existsSync(GAME)){r.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-cache'});return fs.createReadStream(GAME).pipe(r)}
 r.writeHead(200,{'Content-Type':'text/plain'});r.end('Medieval Craft server ok · rooms: '+rooms.size)});
const wss=new WebSocketServer({server:srv,maxPayload:128*1024});

const num=(v,a,b)=>typeof v=='number'&&isFinite(v)&&v>=a&&v<=b;
const code=()=>{let c;do{c=Array.from({length:4},()=>CH[Math.random()*CH.length|0]).join('')}while(rooms.has(c));return c};
const tx=(w,o)=>{if(w.readyState==1)w.send(JSON.stringify(o))};

wss.on('connection',ws=>{
 let room=null,id=0,me=null,lastP=0;ws.alive=true;ws.on('pong',()=>ws.alive=true);
 const bc=(o,skip)=>{for(const p of room.players.values())if(p.ws!==skip)tx(p.ws,o)};
 const enter=(r,name,t)=>{
  if(r.players.size>=MAXP)return tx(ws,{t:'err',m:'Комната заполнена'});
  room=r;id=nid++;me={ws,id,name:String(name||'Игрок').replace(/[<>]/g,'').slice(0,14)||'Игрок',x:0,y:0,z:0,yaw:0,pitch:0,host:!r.players.size};
  tx(ws,{t,room:r.code,id,seed:r.seed,flat:r.flat,surv:r.surv,host:me.host,
   players:[...r.players.values()].map(p=>({id:p.id,name:p.name,x:p.x,y:p.y,z:p.z,yaw:p.yaw,pitch:p.pitch}))});
  /* изменения блоков отправляем пачками — мир у присоединившегося восстановится */
  const all=[...r.blocks].map(([k,v])=>{const[x,y,z]=k.split(',').map(Number);return[x,y,z,v[0],v[1]]});
  for(let i=0;i<all.length;i+=1500)tx(ws,{t:'b',l:all.slice(i,i+1500),init:1});
  r.players.set(id,me);bc({t:'join',id,name:me.name},ws)};
 ws.on('message',m=>{let d;try{d=JSON.parse(m)}catch(e){return}
  if(d.t=='ping')return tx(ws,{t:'pong'});
  if(!room){
   if(d.t=='host'){const r={code:code(),seed:d.seed|0,flat:!!d.flat,surv:!!d.surv,blocks:new Map(),players:new Map(),empty:Date.now(),clk:0};rooms.set(r.code,r);return enter(r,d.name,'hosted')}
   if(d.t=='join'){const r=rooms.get(String(d.room||'').toUpperCase().trim());return r?enter(r,d.name,'joined'):tx(ws,{t:'err',m:'Комната не найдена'})}
   return}
  if(d.t=='p'){const n=Date.now();if(n-lastP<40)return;lastP=n;
   if(!num(d.x,-3e7,3e7)||!num(d.y,-100,500)||!num(d.z,-3e7,3e7)||!num(d.yaw,-100,100)||!num(d.pitch,-10,10))return;
   me.x=d.x;me.y=d.y;me.z=d.z;me.yaw=d.yaw;me.pitch=d.pitch;
   return bc({t:'p',id,x:d.x,y:d.y,z:d.z,yaw:d.yaw,pitch:d.pitch},ws)}
  if(d.t=='b'&&Array.isArray(d.l)){const out=[];
   for(const v of d.l.slice(0,500)){if(!Array.isArray(v))continue;const[x,y,z,b,md]=v;
    if(!num(x,-3e7,3e7)||!num(y,1,87)||!num(z,-3e7,3e7)||!num(b,0,65535))continue;
    const m2=md&&typeof md=='object'&&JSON.stringify(md).length<240?md:0,k=(x|0)+','+(y|0)+','+(z|0),o=room.blocks.get(k);
    if(o&&o[0]==b&&JSON.stringify(o[1])==JSON.stringify(m2))continue;                 /* дубликат — не рассылаем */
    if(!o&&room.blocks.size>=MAXB)continue;
    room.blocks.set(k,[b|0,m2]);out.push([x|0,y|0,z|0,b|0,m2])}
   if(out.length)bc({t:'b',l:out},ws);return}
  if(d.t=='chat'&&typeof d.m=='string'){const s=d.m.replace(/[<>]/g,'').trim().slice(0,120);if(s)bc({t:'chat',id,name:me.name,m:s});return}
  if(d.t=='clk'&&me.host&&num(d.c,0,1e5))return bc({t:'clk',c:d.c},ws)});
 ws.on('close',()=>{if(!room)return;room.players.delete(id);bc({t:'leave',id});
  if(me.host&&room.players.size){const h=room.players.values().next().value;h.host=true;tx(h.ws,{t:'host'})}   /* хост ушёл — им становится следующий */
  if(!room.players.size){room.empty=Date.now();saveRooms()}})});
/* Render усыпляет и рвёт «мёртвые» соединения — проверяем живость каждые 30 с */
setInterval(()=>{for(const w of wss.clients){if(!w.alive){w.terminate();continue}w.alive=false;w.ping()}},30000).unref();
srv.listen(PORT,()=>console.log('MC server on :'+PORT));
