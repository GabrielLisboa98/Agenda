/* Service worker da Agenda: guarda o app no aparelho para abrir sem internet.
   - A página (index.html) vem da rede quando dá (pega a versão nova), senão do cache.
   - Scripts, ícones, dicionário e fontes: do cache, atualizando por trás.
   - Imagens das notas (Supabase Storage): do cache depois da primeira vez.
   - Chamadas de dados ao Supabase (auth/rest) nunca passam pelo cache. */
var VERSION = '20261013-192553';
var CACHE = 'agenda-' + VERSION;
var SHELL = ['./', './index.html', './cloud.js?v=' + VERSION, './config.js', './manifest.webmanifest',
  './icons/icon-192.png', './icons/icon-512.png', './icons/apple-touch-icon.png', './icons/favicon-32.png'];

self.addEventListener('install', function(e){
  e.waitUntil(caches.open(CACHE).then(function(c){ return c.addAll(SHELL.map(function(u){ return new Request(u, {cache:'reload'}); })); }).then(function(){ return self.skipWaiting(); }));
});
self.addEventListener('activate', function(e){
  e.waitUntil(caches.keys().then(function(keys){
    return Promise.all(keys.filter(function(k){ return k.indexOf('agenda-') === 0 && k !== CACHE && k !== 'agenda-media'; })
      .map(function(k){ return caches.delete(k); }));
  }).then(function(){ return self.clients.claim(); }));
});

function fromNetworkFirst(req){
  return caches.open(CACHE).then(function(c){
    var timeout = new Promise(function(res){ setTimeout(function(){ res(null); }, 3500); });
    // sem o cache HTTP do navegador: o GitHub Pages guarda a página por 10 min, e aí a versão nova demoraria
    var net = fetch(new Request(req.url, {cache:'no-cache', credentials:'same-origin'})).then(function(r){ if(r && r.ok) c.put(req, r.clone()); return r; }).catch(function(){ return null; });
    return Promise.race([net, timeout]).then(function(r){
      return r || c.match(req, {ignoreSearch:true}).then(function(m){ return m || net.then(function(r2){ return r2 || c.match('./index.html'); }); });
    });
  });
}
function staleWhileRevalidate(req, cacheName){
  return caches.open(cacheName).then(function(c){
    return c.match(req).then(function(hit){
      var net = fetch(req).then(function(r){ if(r && (r.ok || r.type === 'opaque')) c.put(req, r.clone()); return r; }).catch(function(){ return hit; });
      return hit || net;
    });
  });
}

self.addEventListener('fetch', function(e){
  var req = e.request;
  if(req.method !== 'GET') return;
  var url = new URL(req.url);
  if(/\/(auth|rest|realtime)\/v1\//.test(url.pathname)) return;
  if(url.origin === location.origin && url.pathname.indexOf('/_asset/') >= 0){ e.respondWith(assetResponse(req, url)); return; }
  if(req.mode === 'navigate'){ e.respondWith(fromNetworkFirst(req)); return; }
  if(url.origin === location.origin || /fonts\.(googleapis|gstatic)\.com$/.test(url.hostname)){
    e.respondWith(staleWhileRevalidate(req, CACHE));
  }
});

// A página pergunta "qual versão você é?" para saber se há versão nova (aviso de atualizar).
self.addEventListener('message', function(e){
  if(e.data && e.data.type === 'version' && e.ports && e.ports[0]) e.ports[0].postMessage({version:VERSION});
});

/* ---------- arquivos privados das Notas ---------- */
var AUTH = null, authWaiters = [];
function saveAuth(a){
  AUTH = a;
  caches.open('agenda-auth').then(function(c){ return c.put('./__auth', new Response(JSON.stringify(a))); }).catch(function(){});
  var w = authWaiters; authWaiters = []; w.forEach(function(f){ f(a); });
}
function getAuth(){
  if(AUTH) return Promise.resolve(AUTH);
  return caches.open('agenda-auth').then(function(c){ return c.match('./__auth'); })
    .then(function(r){ return r ? r.json() : null; }).then(function(a){ AUTH = AUTH || a; return AUTH; }).catch(function(){ return null; });
}
function freshAuth(){
  return new Promise(function(res){
    authWaiters.push(res);
    self.clients.matchAll({type:'window'}).then(function(cs){ cs.forEach(function(c){ c.postMessage({type:'need-auth'}); }); });
    setTimeout(function(){ res(AUTH); }, 5000);
  });
}
self.addEventListener('message', function(e){
  if(e.data && e.data.type === 'auth'){
    var a = e.data.auth || {};
    if(!a.token){ AUTH = null; caches.delete('agenda-auth'); caches.delete('agenda-media'); return; }
    saveAuth(a);
  }
});
function fetchAsset(a, id){
  if(!a || !a.token || !a.uid) return Promise.resolve(new Response('', {status:401}));
  return fetch(a.url + '/storage/v1/object/authenticated/' + a.bucket + '/' + a.uid + '/' + id,
    {headers:{Authorization:'Bearer ' + a.token, apikey:a.key}, cache:'no-store'});
}
function ranged(req, res){
  // vídeo no iPhone só toca se a resposta respeitar o pedaço (Range) que ele pede
  var range = req.headers.get('range');
  if(!range || !res.ok) return Promise.resolve(res);
  return res.blob().then(function(b){
    var m = /bytes=(\d*)-(\d*)/.exec(range) || [], start = m[1] ? +m[1] : 0, end = m[2] ? +m[2] : b.size - 1;
    end = Math.min(end, b.size - 1);
    return new Response(b.slice(start, end + 1), {status:206, headers:{
      'Content-Type':res.headers.get('Content-Type') || b.type, 'Content-Range':'bytes ' + start + '-' + end + '/' + b.size,
      'Content-Length':String(end - start + 1), 'Accept-Ranges':'bytes'}});
  });
}
function assetResponse(req, url){
  var key = url.origin + url.pathname, id = url.pathname.split('/_asset/')[1];
  return caches.open('agenda-media').then(function(c){
    return c.match(key).then(function(hit){
      if(hit) return ranged(req, hit);
      return getAuth().then(function(a){
        return fetchAsset(a, id).then(function(r){
          if(r.status >= 400 && r.status < 500){
            return freshAuth().then(function(a2){ return a2 && a2.token !== (a && a.token) ? fetchAsset(a2, id) : r; });
          }
          return r;
        });
      }).then(function(r){
        if(!r.ok) return r;
        return c.put(key, r.clone()).then(function(){ return ranged(req, r); });
      }).catch(function(){ return new Response('', {status:503}); });
    });
  });
}
