// Keeps the game available offline (solo play). Online rooms still need the internet.
const CACHE='chaupar-v1';
const SHELL=['/','/index.html','/manifest.webmanifest','/icons/icon-192.png','/icons/icon-512.png','/icons/maskable-512.png','/icons/apple-touch-icon.png','/icons/favicon.png'];
self.addEventListener('install',e=>{e.waitUntil(caches.open(CACHE).then(c=>c.addAll(SHELL)).then(()=>self.skipWaiting()));});
self.addEventListener('activate',e=>{e.waitUntil(caches.keys().then(ks=>Promise.all(ks.filter(k=>k!==CACHE).map(k=>caches.delete(k)))).then(()=>self.clients.claim()));});
self.addEventListener('fetch',e=>{
  const req=e.request;if(req.method!=='GET')return;
  const url=new URL(req.url);
  if(url.pathname==='/ws')return;
  if(req.mode==='navigate'){ // fresh page when online, cached page when offline
    e.respondWith(fetch(req).then(r=>{const cp=r.clone();caches.open(CACHE).then(c=>c.put('/index.html',cp));return r;}).catch(()=>caches.match('/index.html')));return;
  }
  e.respondWith(caches.match(req).then(hit=>hit||fetch(req).then(r=>{
    if(r.ok&&(url.origin===location.origin||url.host.endsWith('gstatic.com')||url.host.endsWith('googleapis.com'))){const cp=r.clone();caches.open(CACHE).then(c=>c.put(req,cp));}
    return r;})));
});
