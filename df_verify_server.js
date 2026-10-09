/* DEAD FROST · сервер верификации + администрирование.
   Запуск: npm i ws aedes && CREATOR_CODE=твой_секретный_код node df_verify_server.js
   Роли: creator (один, навсегда) > mod (назначает ТОЛЬКО создатель, только через этот сервер) > player.
   Любой статус (верификация, модератор, бан) живёт только в db.json на сервере; клиент лишь отображает то, что сказал сервер,
   а каждая команда проверяется здесь по роли отправителя. */
const {WebSocketServer}=require('ws'),fs=require('fs'),F='db.json',CC=process.env.CREATOR_CODE||'lexalrcigi123';
let db={ver:{},creator:null,bind:{},dev:{},ban:{},inv:{},mod:{}};try{db=Object.assign(db,JSON.parse(fs.readFileSync(F)))}catch(e){}
const save=()=>fs.writeFileSync(F,JSON.stringify(db)),live=new Map(),ID=/^[A-Z2-9]{8}$/;
let kickMqtt=()=>{};                                                                  /* подменяется ниже, когда поднят брокер */
const okId=i=>!!(db.ver[i]||db.creator===i),                                          /* верифицирован? */
 role=i=>db.creator===i?'creator':(db.mod[i]&&db.ver[i]?'mod':'player'),               /* мод = только если ещё и верифицирован */
 nameOf=i=>db.creator===i?'lexalr':db.ver[i]||'',
 /* можно ли «me» применять наказания/снятие к «t»: не себе, не создателю, модератору — только создатель */
 canAct=(me,t,self)=>t!==self&&t!==db.creator&&(me=='creator'||role(t)=='player'),
 push=i=>{const w=live.get(i);if(w&&w.st)w.st()};
const wss=new WebSocketServer({noServer:true});wss.on('connection',ws=>{
 let id=null;const tx=o=>ws.readyState==1&&ws.send(JSON.stringify(o));
 const st=(msg)=>tx({t:'st',ok:okId(id),role:role(id),name:nameOf(id),msg});
 const ban=(a,b,why)=>{db.ban[a]=db.ban[b]=why;save();tx({t:'ban',msg:why});ws.close()};
 const list=()=>{const ids=new Set(Object.keys(db.ver));if(db.creator)ids.add(db.creator);
  tx({t:'list',rows:[...ids].map(i=>({id:i,name:nameOf(i),role:role(i),on:live.has(i)})),bans:Object.keys(db.ban).filter(k=>ID.test(k))})};
 const res=m=>{tx({t:'res',msg:m});list()};
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
  /* ping = повторная проверка статуса на сервере: клиент каждые ~25 с получает свежий ответ (снятие верификации / бан действуют сразу) */
  if(d.t=='ping'){if(db.ban[id]||db.ban[db.bind[id]])return tx({t:'ban',msg:'бан'}),ws.close();return st()}
  const me=role(id);if(me=='player')return;                                          /* дальше — только создатель и модераторы */
  const t=typeof d.id=='string'?d.id.trim().toUpperCase():'',valid=ID.test(t);

  if(d.t=='list')return list();

  if(d.t=='set'){if(!valid)return res('✖ Неверный ID');
   if(d.on){if(db.ban[t])return res('✖ Игрок забанен — сначала разбань');
    db.ver[t]=String(d.name||db.ver[t]||'Игрок').slice(0,14);save();push(t);return res('✔ Верифицирован: '+t)}
   if(!canAct(me,t,id))return res('✖ Нет прав на этого игрока');
   delete db.ver[t];delete db.mod[t];save();kickMqtt(t);push(t);return res('✖ Снята верификация: '+t)}

  if(d.t=='gen'){const c=Array.from({length:6},()=>'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'[Math.random()*32|0]).join('');
   db.inv[c]=String(d.name||'Игрок').slice(0,14);save();return res('Код для «'+db.inv[c]+'»: '+c)}

  if(d.t=='kick'){if(!valid||!canAct(me,t,id))return res('✖ Нет прав на этого игрока');
   kickMqtt(t);const w=live.get(t);if(w)w.close();return res('✔ Отключён: '+t)}

  if(d.t=='ban'){if(!valid||!canAct(me,t,id))return res('✖ Нет прав на этого игрока');
   const why=String(d.name||'бан').slice(0,40);db.ban[t]=why;if(db.bind[t])db.ban[db.bind[t]]=why;save();
   kickMqtt(t);const w=live.get(t);if(w){w.send(JSON.stringify({t:'ban',msg:why}));w.close()}return res('⛔ Забанен: '+t)}

  if(d.t=='unban'){if(!valid)return res('✖ Неверный ID');
   delete db.ban[t];if(db.bind[t])delete db.ban[db.bind[t]];save();return res('✔ Разбанен: '+t)}

  /* ===== только создатель: назначение и снятие модераторов ===== */
  if(d.t=='mod'){if(me!='creator')return res('✖ Модераторов назначает только создатель');
   if(!valid||t===db.creator)return res('✖ Неверный ID');
   if(d.on){if(db.ban[t])return res('✖ Игрок забанен');
    db.mod[t]=1;if(!db.ver[t])db.ver[t]=String(d.name||'Игрок').slice(0,14);save();push(t);return res('🛡 Модератор назначен: '+t)}
   delete db.mod[t];save();push(t);return res('🛡 Модератор снят: '+t)}
 });
 ws.st=()=>st();ws.on('close',()=>{if(id&&live.get(id)===ws)live.delete(id)})});

/* ===== MQTT-брокер: все комнаты и игровой трафик идут через этот сервер, входят только верифицированные ===== */
const aedes=require('aedes')(),{createWebSocketStream}=require('ws');
aedes.authenticate=(c,u,p,cb)=>{u=String(u||'');p=String(p||'');
 const ok=ID.test(u)&&!db.ban[u]&&!db.ban[p]&&db.bind[u]===p&&okId(u);   /* проверка статуса — каждый раз на сервере, из db */
 if(ok)c.dfid=u;cb(null,ok)};
/* снятая верификация / бан / кик → сразу рвём игровое (MQTT) соединение */
kickMqtt=id=>{for(const c of Object.values(aedes.clients||{}))if(c.dfid===id)try{c.close()}catch(e){}};
aedes.authorizePublish=(c,pk,cb)=>pk.payload.length>20000?cb(new Error('big')):cb(null);
/* музыка: файлы из папки music/ (например music/insane.mp3) отдаются по адресу /music/имя.mp3, с поддержкой перемотки (Range) для iPhone */
const path=require('path'),MIME={'.mp3':'audio/mpeg','.ogg':'audio/ogg','.m4a':'audio/mp4','.wav':'audio/wav'},MUSIC=path.join(__dirname,'music');
const srv=require('http').createServer((q,r)=>{
 const u=decodeURIComponent((q.url||'').split('?')[0]);
 if(u.startsWith('/music/')){
  const nm=path.basename(u),type=MIME[path.extname(nm).toLowerCase()];
  /* ищем сначала в папке music/, потом просто рядом с сервером — папку создавать необязательно */
  const f1=path.join(MUSIC,nm),f2=path.join(__dirname,nm);
  return fs.stat(f1,(e1,s1)=>fs.stat(f2,(e2,s2)=>{
   const ok1=!e1&&s1.isFile(),f=ok1?f1:f2,s=ok1?s1:s2,e=ok1?null:e2;
   if(e||!s.isFile()||!type){r.writeHead(404,{'Access-Control-Allow-Origin':'*'});return r.end()}
   const h={'Content-Type':type,'Accept-Ranges':'bytes','Access-Control-Allow-Origin':'*','Cache-Control':'public, max-age=86400'};
   const m=/bytes=(\d*)-(\d*)/.exec(q.headers.range||'');
   if(m&&(m[1]||m[2])){
    const a=m[1]?+m[1]:s.size-+m[2],b=m[1]&&m[2]?Math.min(+m[2],s.size-1):s.size-1;
    if(a>b||a>=s.size){r.writeHead(416,{'Content-Range':'bytes */'+s.size});return r.end()}
    r.writeHead(206,{...h,'Content-Range':`bytes ${a}-${b}/${s.size}`,'Content-Length':b-a+1});
    fs.createReadStream(f,{start:a,end:b}).pipe(r)
   }else{r.writeHead(200,{...h,'Content-Length':s.size});fs.createReadStream(f).pipe(r)}
  }))}
 r.end('DF ok')}),
 wm=new WebSocketServer({noServer:true,handleProtocols:s=>s.has('mqtt')?'mqtt':false});
srv.on('upgrade',(q,s,h)=>{if((q.url||'').startsWith('/mqtt'))wm.handleUpgrade(q,s,h,w=>aedes.handle(createWebSocketStream(w)));
 else wss.handleUpgrade(q,s,h,w=>wss.emit('connection',w,q))});
srv.listen(process.env.PORT||8080,()=>console.log('DF server ok'));
