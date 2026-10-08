/* DEAD FROST · сервер верификации.  Запуск: npm i ws && CREATOR_CODE=lexalrcigi123 node df_verify_server.js */
const {WebSocketServer}=require('ws'),fs=require('fs'),F='db.json',CC=process.env.CREATOR_CODE||'lexalrcigi123';
let db={ver:{},creator:null,bind:{},dev:{},ban:{},inv:{}};try{db=Object.assign(db,JSON.parse(fs.readFileSync(F)))}catch(e){}
const save=()=>fs.writeFileSync(F,JSON.stringify(db)),live=new Map(),ID=/^[A-Z2-9]{8}$/;
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
