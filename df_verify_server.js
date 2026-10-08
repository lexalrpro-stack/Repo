/* DEAD FROST · сервер верификации.  Запуск: npm i ws && CREATOR_CODE=lexalrcigi123 node df_verify_server.js */
const {WebSocketServer}=require('ws'),fs=require('fs'),F='db.json',CC=process.env.CREATOR_CODE||'lexalrcigi123';
/* ===== ХРАНЕНИЕ: Upstash Redis (бесплатно, переживает перезапуски Render). Без переменных окружения — старый режим db.json ===== */
const UU=process.env.UPSTASH_REDIS_REST_URL,UT=process.env.UPSTASH_REDIS_REST_TOKEN,sleep=ms=>new Promise(r=>setTimeout(r,ms));
const rcmd=async a=>{const r=await fetch(UU,{method:'POST',headers:{Authorization:'Bearer '+UT,'Content-Type':'application/json'},body:JSON.stringify(a)});if(!r.ok)throw new Error('upstash '+r.status);return (await r.json()).result};
let db={ver:{},creator:null,bind:{},dev:{},ban:{},inv:{}},loaded=!UU,dirty=0,busy=0;
async function load(){
 if(!UU){console.log('⚠ UPSTASH не настроен — база в db.json (на бесплатном Render она будет стираться!)');try{db=Object.assign(db,JSON.parse(fs.readFileSync(F)))}catch(e){}return}
 for(let i=0;i<6;i++){try{const v=await rcmd(['GET','df:db']);if(v)db=Object.assign(db,JSON.parse(v));loaded=true;console.log('База загружена из Upstash: верифицировано',Object.keys(db.ver).length);return}
  catch(e){console.log('Не удалось загрузить базу:',e.message);await sleep(1500*(i+1))}}
 throw new Error('Upstash недоступен — не запускаюсь, чтобы не затереть базу пустой')}
async function flush(){if(busy)return;busy=1;while(dirty){dirty=0;try{await rcmd(['SET','df:db',JSON.stringify(db)])}catch(e){console.log('Ошибка сохранения, повтор:',e.message);dirty=1;await sleep(3000)}}busy=0}
const save=()=>{if(!UU){try{fs.writeFileSync(F,JSON.stringify(db))}catch(e){}return}if(!loaded)return;dirty=1;flush()},live=new Map(),ID=/^[A-Z2-9]{8}$/;
process.on('SIGTERM',async()=>{try{if(UU&&loaded){dirty=1;await Promise.race([(async()=>{busy=0;await flush()})(),sleep(3000)])}}catch(e){}process.exit(0)});
const wss=new WebSocketServer({noServer:true});wss.on('connection',ws=>{
 let id=null;const tx=o=>ws.readyState==1&&ws.send(JSON.stringify(o));
 const st=(msg)=>tx({t:'st',ok:!!(db.ver[id]||db.creator===id),role:db.creator===id?'creator':'player',name:db.creator===id?'lexalr':db.ver[id]||'',msg});
 const ban=(a,b,why)=>{db.ban[a]=db.ban[b]=why;save();tx({t:'ban',msg:why});ws.close()};
 ws.on('message',m=>{let d;try{d=JSON.parse(m)}catch(e){return}
  if(d.t=='hello'){if(!ID.test(d.id)||typeof d.dev!='string'||d.dev.length<8||d.dev.length>40)return ws.close();
   if(db.ban[d.id]||db.ban[d.dev])return tx({t:'ban',msg:'бан'}),ws.close();
   if(db.bind[d.id]&&db.bind[d.id]!==d.dev)return tx({t:'dup'}),ws.close();          /* чужой ID на другом устройстве */
   if(db.dev[d.dev]&&db.dev[d.dev]!==d.id)return ban(d.id,d.dev,'подмена ID');       /* то же устройство, но ID изменён */
   {const o=live.get(d.id);if(o&&o!==ws)o.close()}                                    /* то же устройство переподключилось — старое соединение закрываем */
   id=d.id;live.set(id,ws);db.bind[id]=d.dev;db.dev[d.dev]=id;
   if(d.code===CC){if(db.creator&&db.creator!==id)return st('Создатель уже выбран навсегда');db.creator=id}
   else if(d.code&&db.inv[d.code]){db.ver[id]=db.inv[d.code];delete db.inv[d.code]}
   save();return st()}
  if(!id)return;
  if(db.creator!==id)return;                                                         /* дальше — только создатель */
  if(d.t=='set'&&ID.test(d.id)){if(d.on)db.ver[d.id]=String(d.name||'Игрок').slice(0,14);else delete db.ver[d.id];save();
   tx({t:'res',msg:(d.on?'✔ Верифицирован: ':'✖ Снята верификация: ')+d.id});const w=live.get(d.id);if(w&&w.st)w.st()}
  if(d.t=='gen'){const c=Array.from({length:6},()=>'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'[Math.random()*32|0]).join('');
   db.inv[c]=String(d.name||'Игрок').slice(0,14);save();tx({t:'res',msg:'Код для «'+db.inv[c]+'»: '+c})}});
 ws.st=()=>st();ws.on('close',()=>{if(id&&live.get(id)===ws)live.delete(id)})});

/* ===== MQTT-брокер: все комнаты и игровой трафик идут через этот сервер, входят только верифицированные ===== */
const aedes=require('aedes')(),{createWebSocketStream}=require('ws');
aedes.authenticate=(c,u,p,cb)=>{u=String(u||'');p=String(p||'');
 cb(null,ID.test(u)&&!db.ban[u]&&db.bind[u]===p&&!!(db.ver[u]||db.creator===u))};
aedes.authorizePublish=(c,pk,cb)=>pk.payload.length>20000?cb(new Error('big')):cb(null);
const srv=require('http').createServer((q,r)=>r.end('DF ok')),
 wm=new WebSocketServer({noServer:true,handleProtocols:s=>s.has('mqtt')?'mqtt':false});
srv.on('upgrade',(q,s,h)=>{if((q.url||'').startsWith('/mqtt'))wm.handleUpgrade(q,s,h,w=>aedes.handle(createWebSocketStream(w)));
 else wss.handleUpgrade(q,s,h,w=>wss.emit('connection',w,q))});
load().then(()=>srv.listen(process.env.PORT||8080,()=>console.log('DF server ok'))).catch(e=>{console.error(e.message);process.exit(1)});
