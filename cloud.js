/* Agenda — camada de dados fora do Claude.
 *
 * Substitui o `window.claude.use(...)` que o app usava dentro do artefato, com a MESMA forma de API
 * (collection/doc/where/orderBy/onSnapshot/get/set/update/delete), então o resto do app não muda.
 *
 * Como funciona:
 *  - Tudo fica numa cópia local (IndexedDB) — o app abre e funciona sem internet.
 *  - Cada gravação entra numa fila ("outbox") guardada no aparelho; quando há internet, a fila sobe
 *    pro Supabase e as mudanças dos outros aparelhos descem (puxada incremental por número de revisão).
 *  - Sem Supabase configurado (config.js vazio), roda só local.
 *
 * Sem bibliotecas externas: conversa com o Supabase por fetch puro (Auth, REST e Storage), pra
 * funcionar offline e não depender de CDN.
 */
(function(){
  'use strict';
  var CFG = window.AGENDA_CONFIG || {};
  var URL_ = (CFG.supabaseUrl || '').replace(/\/+$/, '');
  var KEY = CFG.supabaseAnonKey || '';
  var CLOUD = !!(URL_ && KEY);
  var PULL_EVERY_MS = CFG.pullEveryMs || 20000;
  var LS_SESSION = 'agenda-cloud-session';
  var LS_CURSOR = 'agenda-cloud-cursor';

  function clone(v){ return v === undefined ? undefined : JSON.parse(JSON.stringify(v)); }
  function rid(){ var a = new Uint8Array(10); crypto.getRandomValues(a); return Array.prototype.map.call(a, function(b){ return ('0'+b.toString(36)).slice(-2); }).join('').slice(0, 20); }
  function hex32(){ var a = new Uint8Array(16); crypto.getRandomValues(a); return Array.prototype.map.call(a, function(b){ return ('0'+b.toString(16)).slice(-2); }).join(''); }
  function lsGet(k){ try{ return localStorage.getItem(k); }catch(_){ return null; } }
  function lsSet(k, v){ try{ if(v == null) localStorage.removeItem(k); else localStorage.setItem(k, v); }catch(_){} }

  /* ================= IndexedDB ================= */
  var idb = null;
  function idbOpen(){
    return new Promise(function(res, rej){
      var r = indexedDB.open(CFG.dbName || 'agenda-cloud', 1);
      r.onupgradeneeded = function(){
        var d = r.result;
        if(!d.objectStoreNames.contains('docs')) d.createObjectStore('docs');
        if(!d.objectStoreNames.contains('outbox')) d.createObjectStore('outbox', {autoIncrement:true});
      };
      r.onsuccess = function(){ res(r.result); };
      r.onerror = function(){ rej(r.error); };
    });
  }
  function tx(stores, mode, fn){
    return new Promise(function(res, rej){
      var t = idb.transaction(stores, mode), out;
      t.oncomplete = function(){ res(out); };
      t.onerror = t.onabort = function(){ rej(t.error); };
      out = fn(t);
    });
  }
  function reqP(r){ return new Promise(function(res, rej){ r.onsuccess = function(){ res(r.result); }; r.onerror = function(){ rej(r.error); }; }); }

  /* ================= memória ================= */
  var mem = {};            // col -> { id -> data }
  var ver = {};            // col -> { id -> nº de versão local (muda a cada escrita) }
  var verN = 0;
  var pendingIds = {};     // 'col/id' -> nº de operações ainda não enviadas
  function colMap(c){ return mem[c] || (mem[c] = {}); }
  function verMap(c){ return ver[c] || (ver[c] = {}); }
  function k(c, id){ return c + '\u0000' + id; }

  async function loadLocal(){
    var rows = await tx(['docs', 'outbox'], 'readonly', function(t){
      var out = {docs:null, ob:null};
      reqP(t.objectStore('docs').getAll()).then(function(v){ out.docs = v; });
      reqP(t.objectStore('outbox').getAll()).then(function(v){ out.ob = v; });
      return out;
    });
    (rows.docs || []).forEach(function(r){ colMap(r.col)[r.id] = r.data; verMap(r.col)[r.id] = ++verN; });
    (rows.ob || []).forEach(function(o){ var p = o.col + '/' + o.id; pendingIds[p] = (pendingIds[p] || 0) + 1; });
  }

  /* Grava local + fila na mesma transação: ou as duas acontecem, ou nenhuma. */
  function persist(c, id, data, op){
    return tx(['docs', 'outbox'], 'readwrite', function(t){
      if(data === null) t.objectStore('docs').delete(k(c, id));
      else t.objectStore('docs').put({col:c, id:id, data:data}, k(c, id));
      if(op && CLOUD) t.objectStore('outbox').add(op);
    });
  }

  /* ================= ouvintes ================= */
  var listeners = [];      // {col, docId?, filters, order, prev:{id:ver}, prevOrder:[], started, next, err}
  var dirtyCols = {}, flushTimer = null;
  function markDirty(c){ dirtyCols[c] = 1; if(!flushTimer) flushTimer = setTimeout(flushListeners, 0); }

  function cmp(a, b){
    if(a === b) return 0;
    if(a === undefined || a === null) return -1;
    if(b === undefined || b === null) return 1;
    if(typeof a === typeof b) return a < b ? -1 : 1;
    return String(a) < String(b) ? -1 : 1;
  }
  function matches(data, filters){
    for(var i = 0; i < filters.length; i++){
      var f = filters[i], v = data ? data[f[0]] : undefined, w = f[2];
      switch(f[1]){
        case '==': if(w === null ? (v !== null && v !== undefined) : JSON.stringify(v) !== JSON.stringify(w)) return false; break;
        case '!=': if(JSON.stringify(v) === JSON.stringify(w)) return false; break;
        case '<':  if(v === undefined || v === null || !(cmp(v, w) < 0)) return false; break;
        case '<=': if(v === undefined || v === null || !(cmp(v, w) <= 0)) return false; break;
        case '>':  if(v === undefined || v === null || !(cmp(v, w) > 0)) return false; break;
        case '>=': if(v === undefined || v === null || !(cmp(v, w) >= 0)) return false; break;
        case 'in': if(!Array.isArray(w) || !w.some(function(x){ return JSON.stringify(x) === JSON.stringify(v); })) return false; break;
        case 'array-contains': if(!Array.isArray(v) || !v.some(function(x){ return JSON.stringify(x) === JSON.stringify(w); })) return false; break;
        default: break;
      }
    }
    return true;
  }
  function runQuery(c, filters, order, lim){
    var m = colMap(c), ids = Object.keys(m).filter(function(id){ return matches(m[id], filters); });
    ids.sort(function(a, b){
      for(var i = 0; i < order.length; i++){
        var r = cmp(m[a][order[i][0]], m[b][order[i][0]]);
        if(r) return order[i][1] === 'desc' ? -r : r;
      }
      return a < b ? -1 : a > b ? 1 : 0;
    });
    if(lim) ids = ids.slice(0, lim);
    return ids;
  }
  function mkDoc(c, id, data){
    var d = data === undefined ? undefined : clone(data);
    return {id:id, exists:d !== undefined, ref:docRef(c, id),
      data:function(){ return d === undefined ? undefined : clone(d); },
      get:function(f){ return d ? clone(d[f]) : undefined; },
      metadata:{fromCache:!online(), hasPendingWrites:!!pendingIds[c + '/' + id]}};
  }
  function emit(l){
    try{
      if(l.docId){
        var v = colMap(l.col)[l.docId], vv = v === undefined ? -1 : verMap(l.col)[l.docId];
        if(l.started && l.prevV === vv) return;
        l.started = true; l.prevV = vv;
        l.next(mkDoc(l.col, l.docId, v));
        return;
      }
      var m = colMap(l.col), vm = verMap(l.col), ids = runQuery(l.col, l.filters, l.order, l.lim);
      var changes = [], prev = l.prev, next = {};
      ids.forEach(function(id, i){
        next[id] = vm[id];
        if(!(id in prev)) changes.push({type:'added', id:id, newIndex:i, oldIndex:-1});
        else if(prev[id] !== vm[id]) changes.push({type:'modified', id:id, newIndex:i, oldIndex:l.prevOrder.indexOf(id)});
      });
      Object.keys(prev).forEach(function(id){ if(!(id in next)) changes.push({type:'removed', id:id, newIndex:-1, oldIndex:l.prevOrder.indexOf(id)}); });
      var orderChanged = ids.join('\u0001') !== l.prevOrder.join('\u0001');
      if(l.started && !changes.length && !orderChanged) return;
      var lastSeen = l.lastData || {};
      l.started = true; l.prev = next; l.prevOrder = ids;
      var docs = ids.map(function(id){ return mkDoc(l.col, id, m[id]); });
      var dc = changes.map(function(ch){
        var data = ch.type === 'removed' ? lastSeen[ch.id] : m[ch.id];
        return {type:ch.type, doc:mkDoc(l.col, ch.id, data), oldIndex:ch.oldIndex, newIndex:ch.newIndex};
      });
      var keep = {}; ids.forEach(function(id){ keep[id] = m[id]; }); l.lastData = keep;
      l.next({docs:docs, size:docs.length, empty:!docs.length,
        forEach:function(f){ docs.forEach(f); },
        docChanges:function(){ return dc; },
        metadata:{fromCache:!online(), hasPendingWrites:false}});
    }catch(e){ console.error('[agenda] erro num ouvinte', e); }
  }
  function flushListeners(){
    flushTimer = null;
    var cols = dirtyCols; dirtyCols = {};
    listeners.slice().forEach(function(l){ if(cols[l.col]) emit(l); });
  }
  function listen(l){
    l.prev = {}; l.prevOrder = []; l.started = false;
    listeners.push(l);
    setTimeout(function(){ if(listeners.indexOf(l) >= 0) emit(l); }, 0);
    return function(){ var i = listeners.indexOf(l); if(i >= 0) listeners.splice(i, 1); };
  }

  /* ================= escrita local ================= */
  function writeLocal(c, id, data, op){
    var m = colMap(c);
    if(data === null){ delete m[id]; delete verMap(c)[id]; }
    else { m[id] = data; verMap(c)[id] = ++verN; }
    if(op && CLOUD){ var p = c + '/' + id; pendingIds[p] = (pendingIds[p] || 0) + 1; }
    markDirty(c);
    var pr = persist(c, id, data, op);
    if(op && CLOUD) pr.then(function(){ Sync.soon(); });
    return pr;
  }
  function checkPath(c, id){
    if(typeof c !== 'string' || !c || c.indexOf('/') >= 0) throw {code:'invalid_argument', message:'Coleção inválida'};
    if(typeof id !== 'string' || !id || id.indexOf('/') >= 0) throw {code:'invalid_argument', message:'Id inválido'};
  }
  function docRef(c, id){
    return {
      id:id, path:c + '/' + id,
      get:function(){ var v = colMap(c)[id]; return Promise.resolve(mkDoc(c, id, v)); },
      set:function(d, opts){
        try{ checkPath(c, id); }catch(e){ return Promise.reject(e); }
        var data = clone(d || {});
        if(opts && opts.merge){ var cur = colMap(c)[id]; if(cur){ data = Object.assign(clone(cur), data); } }
        return writeLocal(c, id, data, {col:c, id:id, kind:'set', data:data});
      },
      update:function(d){
        try{ checkPath(c, id); }catch(e){ return Promise.reject(e); }
        var cur = colMap(c)[id];
        if(!cur) return Promise.reject({code:'not_found', message:'Documento não existe: ' + c + '/' + id});
        var patch = clone(d || {});
        return writeLocal(c, id, Object.assign(clone(cur), patch), {col:c, id:id, kind:'update', data:patch});
      },
      delete:function(){
        try{ checkPath(c, id); }catch(e){ return Promise.reject(e); }
        if(!(id in colMap(c)) && !CLOUD) return Promise.resolve();
        return writeLocal(c, id, null, {col:c, id:id, kind:'delete', data:null});
      },
      onSnapshot:function(next, err){ return listen({col:c, docId:id, next:next, err:err}); }
    };
  }
  function query(c, filters, order, lim){
    var q = {
      doc:function(id){ return docRef(c, id || rid()); },
      add:function(d){ var r = docRef(c, rid()); return r.set(d).then(function(){ return r; }); },
      where:function(f, op, v){ return query(c, filters.concat([[f, op, v]]), order, lim); },
      orderBy:function(f, dir){ return query(c, filters, order.concat([[f, dir === 'desc' ? 'desc' : 'asc']]), lim); },
      limit:function(n){ return query(c, filters, order, n); },
      get:function(){
        var m = colMap(c), docs = runQuery(c, filters, order, lim).map(function(id){ return mkDoc(c, id, m[id]); });
        return Promise.resolve({docs:docs, size:docs.length, empty:!docs.length, forEach:function(f){ docs.forEach(f); }, docChanges:function(){ return []; }});
      },
      onSnapshot:function(next, err){ return listen({col:c, filters:filters, order:order, lim:lim, next:next, err:err}); }
    };
    return q;
  }
  var db = {
    collection:function(c){ return query(c, [], [], 0); },
    doc:function(p){ var s = String(p).split('/'); return docRef(s[0], s[1]); }
  };

  /* ================= Supabase: sessão ================= */
  var session = null, needReload = false, appOpen = false;
  try{ session = JSON.parse(lsGet(LS_SESSION) || 'null'); }catch(_){ session = null; }
  function saveSession(s){ session = s; lsSet(LS_SESSION, s ? JSON.stringify(s) : null); if(s && s.user && s.user.id) lsSet('agenda-cloud-user', s.user.id); sendAuth(); Ui.update(); }
  function online(){ return typeof navigator === 'undefined' || navigator.onLine !== false; }

  async function http(path, opts){
    opts = opts || {};
    var headers = Object.assign({apikey:KEY}, opts.headers || {});
    if(opts.auth !== false && session && session.access_token) headers.Authorization = 'Bearer ' + session.access_token;
    if(opts.json !== undefined){ headers['Content-Type'] = 'application/json'; }
    var res = await fetch(URL_ + path, {method:opts.method || 'GET', headers:headers,
      body:opts.json !== undefined ? JSON.stringify(opts.json) : opts.body});
    var txt = await res.text(), body = null;
    try{ body = txt ? JSON.parse(txt) : null; }catch(_){ body = txt; }
    if(!res.ok){
      var msg = (body && (body.msg || body.message || body.error_description || body.error)) || ('HTTP ' + res.status);
      var e = new Error(msg); e.status = res.status; e.body = body; throw e;
    }
    return body;
  }
  function fromAuth(r){
    return {access_token:r.access_token, refresh_token:r.refresh_token,
      expires_at:Date.now() + (r.expires_in || 3600) * 1000, user:{id:r.user && r.user.id, email:r.user && r.user.email}};
  }
  async function refreshIfNeeded(force){
    if(!session) throw new Error('sem sessão');
    if(!force && session.expires_at - Date.now() > 60000) return;
    try{
      var r = await http('/auth/v1/token?grant_type=refresh_token', {method:'POST', auth:false, json:{refresh_token:session.refresh_token}});
      saveSession(fromAuth(r));
    }catch(e){
      if(e.status === 400 || e.status === 401){ saveSession(null); Ui.showLogin('Sua sessão expirou. Entre de novo.'); }
      throw e;
    }
  }
  async function authed(path, opts){
    await refreshIfNeeded(false);
    try{ return await http(path, opts); }
    catch(e){ if(e.status === 401){ await refreshIfNeeded(true); return http(path, opts); } throw e; }
  }
  // Entrou numa conta diferente da última usada neste aparelho: limpa a cópia local da outra conta.
  async function adopt(r){
    var prevUser = lsGet('agenda-cloud-user');
    if(prevUser && r.user && prevUser !== r.user.id){ await resetLocal(); needReload = appOpen; }
    saveSession(fromAuth(r));
  }
  function b64url(bytes){ var s = ''; for(var i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]); return btoa(s).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, ''); }
  var LS_PKCE = 'agenda-pkce';
  var Auth = {
    signIn:async function(email, password){
      var r = await http('/auth/v1/token?grant_type=password', {method:'POST', auth:false, json:{email:email, password:password}});
      await adopt(r);
    },
    /* Continuar com o Google: vai para a tela do Google e volta para o app com um código de uso único.
       O código só vale junto com um segredo que ficou guardado neste aparelho (PKCE). */
    google:async function(){
      var verifier = b64url(crypto.getRandomValues(new Uint8Array(32)));
      var challenge = b64url(new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier))));
      lsSet(LS_PKCE, verifier);
      var back = location.origin + location.pathname;
      location.assign(URL_ + '/auth/v1/authorize?provider=google&redirect_to=' + encodeURIComponent(back) +
        '&code_challenge=' + challenge + '&code_challenge_method=s256');
    },
    completeRedirect:async function(){
      var q = new URLSearchParams(location.search), h = new URLSearchParams(location.hash.replace(/^#/, ''));
      var code = q.get('code'), err = q.get('error_description') || q.get('error') || h.get('error_description');
      if(!code && !err) return null;
      try{ history.replaceState(null, '', location.pathname); }catch(_){}
      if(err) return {error:err};
      var verifier = lsGet(LS_PKCE); lsSet(LS_PKCE, null);
      if(!verifier) return {error:'O login do Google voltou numa janela diferente. Tente de novo por aqui.'};
      var r = await http('/auth/v1/token?grant_type=pkce', {method:'POST', auth:false, json:{auth_code:code, code_verifier:verifier}});
      await adopt(r);
      return {ok:true};
    },
    signUp:async function(email, password){
      var r = await http('/auth/v1/signup', {method:'POST', auth:false, json:{email:email, password:password}});
      if(r && r.access_token){ await adopt(r); return 'in'; }
      return 'confirm';
    },
    signOut:async function(){
      try{ if(session) await http('/auth/v1/logout', {method:'POST'}); }catch(_){}
      saveSession(null); lsSet('agenda-cloud-user', null); await resetLocal();
      try{ await caches.delete('agenda-media'); }catch(_){}
    }
  };
  async function resetLocal(){
    lsSet(LS_CURSOR, null);
    if(idb) await tx(['docs', 'outbox'], 'readwrite', function(t){ t.objectStore('docs').clear(); t.objectStore('outbox').clear(); });
    var cols = Object.keys(mem); mem = {}; ver = {}; pendingIds = {};
    cols.forEach(markDirty);
  }

  /* ================= Sincronização ================= */
  var Sync = {
    state:'idle', lastOk:0, lastErr:null, running:null, timer:null, again:false,
    soon:function(){ var s = this; clearTimeout(s.timer); s.timer = setTimeout(function(){ s.run(); }, 400); },
    run:function(){
      var s = this;
      if(!CLOUD || !session) return Promise.resolve();
      if(s.running){ s.again = true; return s.running; }
      s.state = 'syncing'; Ui.update();
      s.running = (async function(){
        try{ await push(); await pull(); s.state = 'ok'; s.lastOk = Date.now(); s.lastErr = null; }
        catch(e){ s.state = online() ? 'error' : 'offline'; s.lastErr = e && e.message; console.warn('[agenda] sync', e); }
        finally{ s.running = null; Ui.update(); if(s.again){ s.again = false; s.soon(); } }
      })();
      return s.running;
    }
  };
  async function push(){
    for(;;){
      var batch = await tx(['outbox'], 'readonly', function(t){
        var out = {keys:[], ops:[]}, cur = t.objectStore('outbox').openCursor();
        cur.onsuccess = function(){ var c = cur.result; if(!c || out.keys.length >= 200) return; out.keys.push(c.key); out.ops.push(c.value); c.continue(); };
        return out;
      });
      if(!batch.ops.length) return;
      await authed('/rest/v1/rpc/apply_ops', {method:'POST', json:{ops:batch.ops.map(function(o){ return {col:o.col, id:o.id, kind:o.kind, data:o.data}; })}});
      await tx(['outbox'], 'readwrite', function(t){ batch.keys.forEach(function(key){ t.objectStore('outbox').delete(key); }); });
      batch.ops.forEach(function(o){ var p = o.col + '/' + o.id; if(pendingIds[p] && --pendingIds[p] <= 0) delete pendingIds[p]; });
    }
  }
  async function pull(){
    var cursor = Number(lsGet(LS_CURSOR) || 0);
    for(;;){
      // pequena sobreposição: uma transação mais lenta pode gravar uma revisão menor depois de uma maior
      var from = Math.max(0, cursor - 25);
      var rows = await authed('/rest/v1/docs?select=col,id,data,deleted,rev&rev=gt.' + from + '&order=rev.asc&limit=1000', {});
      if(!rows || !rows.length) return;
      var changed = [];
      rows.forEach(function(r){
        if(r.rev > cursor) cursor = r.rev;
        if(pendingIds[r.col + '/' + r.id]) return;  // tem edição local ainda subindo: ela ganha
        var cur = colMap(r.col)[r.id];
        if(r.deleted){ if(cur !== undefined) changed.push([r.col, r.id, null]); }
        else if(JSON.stringify(cur) !== JSON.stringify(r.data)) changed.push([r.col, r.id, r.data]);
      });
      if(changed.length){
        changed.forEach(function(ch){
          if(ch[2] === null){ delete colMap(ch[0])[ch[1]]; delete verMap(ch[0])[ch[1]]; }
          else { colMap(ch[0])[ch[1]] = ch[2]; verMap(ch[0])[ch[1]] = ++verN; }
          markDirty(ch[0]);
        });
        await tx(['docs'], 'readwrite', function(t){
          var st = t.objectStore('docs');
          changed.forEach(function(ch){ if(ch[2] === null) st.delete(k(ch[0], ch[1])); else st.put({col:ch[0], id:ch[1], data:ch[2]}, k(ch[0], ch[1])); });
        });
      }
      lsSet(LS_CURSOR, String(cursor));
      if(rows.length < 1000) return;
    }
  }
  function startLoops(){
    setInterval(function(){ if(document.visibilityState !== 'hidden') Sync.run(); }, PULL_EVERY_MS);
    window.addEventListener('online', function(){ Sync.run(); });
    window.addEventListener('offline', function(){ Sync.state = 'offline'; Ui.update(); });
    window.addEventListener('focus', function(){ Sync.run(); });
    document.addEventListener('visibilitychange', function(){ if(document.visibilityState === 'visible') Sync.run(); });
  }
  async function pendingCount(){
    if(!idb) return 0;
    return tx(['outbox'], 'readonly', function(t){ var o = {n:0}; reqP(t.objectStore('outbox').count()).then(function(n){ o.n = n; }); return o; }).then(function(o){ return o.n; });
  }

  /* ================= arquivos (imagens/vídeos/PDF das notas) ================= */
  var ASSET_TYPES = /^(image\/(png|jpeg|gif|webp|svg\+xml)|video\/(mp4|webm)|application\/pdf|text\/(csv|plain|markdown)|application\/json)$/;
  /* Os arquivos ficam num bucket PRIVADO: só abrem com a sua senha. A página usa um endereço do próprio
     app (…/_asset/<id>) e o service worker busca o arquivo no Supabase com o seu login, guardando uma
     cópia no aparelho (abre sem internet depois da primeira vez). */
  var BUCKET = CFG.bucket || 'assets';
  function assetPublicUrl(id){ return id ? new URL('_asset/' + id, location.href).href : ''; }
  function swPost(msg){
    try{
      var sw = navigator.serviceWorker; if(!sw) return;
      if(sw.controller) sw.controller.postMessage(msg);
      sw.getRegistration().then(function(reg){ if(reg && reg.active && reg.active !== sw.controller) reg.active.postMessage(msg); });
    }catch(_){}
  }
  function sendAuth(){
    if(!CLOUD) return;
    swPost({type:'auth', auth:{url:URL_, key:KEY, bucket:BUCKET,
      token:(session && session.access_token) || null, uid:(session && session.user && session.user.id) || null}});
  }
  var assets = {
    upload:async function(blob, opts){
      var t = (opts && opts.type) || blob.type;
      if(!ASSET_TYPES.test(t)) throw {code:'unsupported_type', message:'Tipo de arquivo não suportado'};
      if(!CLOUD || !session) throw {code:'unavailable', message:'Entre na sua conta para enviar arquivos'};
      if(!online()) throw {code:'store_unavailable', message:'Sem internet agora'};
      var id = hex32();
      await authed('/storage/v1/object/' + BUCKET + '/' + session.user.id + '/' + id,
        {method:'POST', body:blob, headers:{'Content-Type':t, 'x-upsert':'false', 'cache-control':'31536000'}});
      var url = assetPublicUrl(id);
      try{ var c = await caches.open('agenda-media'); await c.put(url, new Response(blob, {headers:{'Content-Type':t}})); }catch(_){}
      return {id:id, url:url, sizeBytes:blob.size, contentType:t};
    },
    url:assetPublicUrl,
    list:function(){ return Promise.resolve({assets:[], usage:{}}); },
    delete:function(){ return Promise.resolve({deleted:true}); }
  };
  var downloads = {
    save:function(o){
      try{
        var a = document.createElement('a'), u = URL.createObjectURL(o.data);
        a.href = u; a.download = o.filename || 'arquivo'; document.body.appendChild(a); a.click();
        setTimeout(function(){ URL.revokeObjectURL(u); a.remove(); }, 1500);
        return Promise.resolve({status:'saved'});
      }catch(e){ return Promise.reject({code:'unavailable'}); }
    }
  };

  /* ================= backup ================= */
  function exportAll(){
    var out = {format:'agenda-backup', version:1, exportedAt:new Date().toISOString(), collections:{}};
    Object.keys(mem).forEach(function(c){ if(Object.keys(mem[c]).length) out.collections[c] = clone(mem[c]); });
    return out;
  }
  async function importAll(json, mode){
    var cols = json && (json.collections || json);
    if(!cols || typeof cols !== 'object') throw new Error('Arquivo não parece um backup da Agenda');
    var n = 0;
    var names = Object.keys(cols);
    for(var i = 0; i < names.length; i++){
      var c = names[i], docs = cols[c];
      if(!docs || typeof docs !== 'object') continue;
      var ids = Object.keys(docs);
      for(var j = 0; j < ids.length; j++){
        var id = ids[j];
        if(mode === 'missing' && colMap(c)[id] !== undefined) continue;
        await docRef(c, id).set(docs[id]); n++;
      }
    }
    return n;
  }

  /* ================= interface mínima (login + status) ================= */
  var Ui = (function(){
    var root = null, dot = null, menu = null, loginEl = null;
    var css = '' +
      '.ac-dot{position:fixed;left:calc(10px + env(safe-area-inset-left));bottom:calc(10px + env(safe-area-inset-bottom));z-index:99990;width:26px;height:26px;border-radius:50%;border:1px solid rgba(0,0,0,.15);background:rgba(255,252,245,.85);backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px);display:flex;align-items:center;justify-content:center;cursor:pointer;box-shadow:0 4px 14px -6px rgba(0,0,0,.4);padding:0}' +
      '.ac-dot i{width:10px;height:10px;border-radius:50%;background:#9a9a9a;display:block}' +
      '.ac-dot[data-s=ok] i{background:#5E8A6E}.ac-dot[data-s=syncing] i{background:#C08A2E;animation:acp 1s ease-in-out infinite}' +
      '.ac-dot[data-s=error] i{background:#B3261E}.ac-dot[data-s=offline] i{background:#9a9a9a;box-shadow:0 0 0 2px #fff,0 0 0 3px #9a9a9a}' +
      '@keyframes acp{50%{opacity:.35}}' +
      '.ac-menu{position:fixed;left:calc(10px + env(safe-area-inset-left));bottom:calc(48px + env(safe-area-inset-bottom));z-index:99991;width:260px;background:#FFFCF5;color:#1F1B18;border:1px solid rgba(31,27,24,.18);border-radius:10px;box-shadow:0 22px 60px rgba(31,27,24,.35);padding:12px;font:14px Nunito,system-ui,sans-serif}' +
      '.ac-menu b{display:block;font-size:13px;margin-bottom:2px}.ac-menu small{display:block;color:rgba(31,27,24,.6);font-size:12px;margin-bottom:10px;line-height:1.35}' +
      '.ac-menu button,.ac-login button{display:block;width:100%;text-align:left;margin-top:6px;padding:9px 10px;border-radius:7px;border:1px solid rgba(31,27,24,.15);background:#F6F0E4;color:#1F1B18;font:600 13px Nunito,system-ui,sans-serif;cursor:pointer}' +
      '.ac-login{position:fixed;inset:0;z-index:99995;background:rgba(239,231,216,.96);display:flex;align-items:center;justify-content:center;padding:20px;font:15px Nunito,system-ui,sans-serif;color:#1F1B18}' +
      '.ac-card{width:100%;max-width:360px;background:#FFFCF5;border:1px solid rgba(31,27,24,.15);border-radius:14px;padding:22px;box-shadow:0 22px 60px -20px rgba(31,27,24,.4)}' +
      '.ac-card h2{font:700 22px Quicksand,Nunito,system-ui,sans-serif;margin:0 0 4px}.ac-card p{margin:0 0 14px;color:rgba(31,27,24,.62);font-size:14px;line-height:1.4}' +
      '.ac-card input{display:block;width:100%;box-sizing:border-box;margin-top:8px;padding:11px 12px;border-radius:8px;border:1px solid rgba(31,27,24,.2);background:#fff;font:16px Nunito,system-ui,sans-serif;color:#1F1B18}' +
      '.ac-login button.pri{background:#C1462F;color:#F6F0E4;border-color:transparent;text-align:center;margin-top:12px;padding:11px}' +
      '.ac-login button.link{background:none;border:none;color:#8A2B1C;text-align:center;font-weight:600;padding:6px}' +
      '.ac-upd{position:fixed;left:50%;top:calc(10px + env(safe-area-inset-top));transform:translateX(-50%);z-index:99992;display:flex;align-items:center;gap:8px;padding:6px 6px 6px 14px;border-radius:999px;background:#1F1B18;color:#EFE7D6;font:600 14px Nunito,system-ui,sans-serif;box-shadow:0 10px 30px -10px rgba(0,0,0,.55);white-space:nowrap;max-width:calc(100vw - 24px);animation:acin .35s ease-out}' +
      '.ac-upd button{border:none;cursor:pointer;font:700 13px Nunito,system-ui,sans-serif;border-radius:999px}' +
      '.ac-upd-go{background:#C1462F;color:#F6F0E4;padding:7px 14px}.ac-upd-go:disabled{opacity:.7}' +
      '.ac-upd-x{background:transparent;color:rgba(239,231,214,.6);width:30px;height:30px;padding:0}' +
      '@keyframes acin{from{opacity:0;transform:translate(-50%,-12px)}}' +
      '.ac-login button.ac-google{display:flex;align-items:center;justify-content:center;gap:10px;margin-top:4px;padding:11px;background:#fff;color:#1F1B18;border:1px solid rgba(31,27,24,.25);font-weight:700;font-size:14px}' +
      '.ac-or{display:flex;align-items:center;gap:10px;margin:14px 0 2px;color:rgba(31,27,24,.5);font-size:12px}.ac-or:before,.ac-or:after{content:"";flex:1;height:1px;background:rgba(31,27,24,.15)}' +
      '.ac-busy input,.ac-busy button,.ac-busy .ac-or{display:none!important}' +
      '.ac-msg{min-height:18px;margin-top:10px;font-size:13px;color:#B3261E}.ac-msg.ok{color:#5E8A6E}' +
      '@media (prefers-color-scheme:dark){.ac-menu,.ac-card{background:#231D18;color:#F1E7D6;border-color:rgba(241,231,214,.16)}.ac-menu small,.ac-card p{color:rgba(241,231,214,.62)}.ac-menu button,.ac-login button{background:#2C241D;color:#F1E7D6;border-color:rgba(241,231,214,.16)}.ac-login{background:rgba(23,19,16,.96);color:#F1E7D6}.ac-card input{background:#1C1713;color:#F1E7D6;border-color:rgba(241,231,214,.2)}.ac-dot{background:rgba(35,29,24,.85);border-color:rgba(255,255,255,.12)}.ac-or{color:rgba(241,231,214,.5)}.ac-or:before,.ac-or:after{background:rgba(241,231,214,.16)}.ac-login button.ac-google{background:#fff;color:#1F1B18}}';
    function el(h){ var d = document.createElement('div'); d.innerHTML = h; return d.firstElementChild; }
    function ensure(){
      if(root || !document.body) return !!root;
      var st = document.createElement('style'); st.textContent = css; document.head.appendChild(st);
      root = document.createElement('div'); root.id = 'agenda-cloud-ui'; document.body.appendChild(root);
      dot = el('<button class="ac-dot" type="button" aria-label="Sincronização"><i></i></button>');
      dot.addEventListener('click', function(e){ e.stopPropagation(); toggleMenu(); });
      root.appendChild(dot);
      document.addEventListener('click', function(e){ if(menu && !menu.contains(e.target)) closeMenu(); });
      return true;
    }
    function fmtAgo(t){ if(!t) return 'nunca'; var s = Math.round((Date.now() - t) / 1000); if(s < 60) return 'agora há pouco'; if(s < 3600) return 'há ' + Math.round(s / 60) + ' min'; return 'há ' + Math.round(s / 3600) + ' h'; }
    function statusText(){
      if(!CLOUD) return ['Só neste aparelho', 'A nuvem ainda não foi configurada (config.js). Tudo fica salvo aqui.'];
      if(!session) return ['Desconectado', 'Entre para sincronizar com os outros aparelhos.'];
      var st = Sync.state, who = (session.user && session.user.email) || '';
      if(st === 'offline') return ['Sem internet', 'Tudo continua salvo aqui e sobe quando a internet voltar. ' + who];
      if(st === 'error') return ['Erro ao sincronizar', (Sync.lastErr || '') + ' · ' + who];
      if(st === 'syncing') return ['Sincronizando…', who];
      return ['Sincronizado', 'Última vez ' + fmtAgo(Sync.lastOk) + ' · ' + who];
    }
    async function toggleMenu(){
      if(menu){ closeMenu(); return; }
      var t = statusText(), n = await pendingCount();
      menu = el('<div class="ac-menu"><b></b><small></small></div>');
      menu.querySelector('b').textContent = t[0];
      menu.querySelector('small').textContent = t[1] + (n ? ' · ' + n + ' alteração(ões) esperando para subir' : '');
      function add(label, fn){ var b = document.createElement('button'); b.type = 'button'; b.textContent = label; b.addEventListener('click', function(){ closeMenu(); fn(); }); menu.appendChild(b); }
      if(CLOUD && session) add('Sincronizar agora', function(){ Sync.run(); });
      if(CLOUD && !session) add('Entrar', function(){ showLogin(); });
      add('Baixar backup (.json)', function(){
        var b = new Blob([JSON.stringify(exportAll())], {type:'application/json'});
        downloads.save({filename:'agenda-backup-' + new Date().toISOString().slice(0, 10) + '.json', data:b});
      });
      add('Importar backup…', function(){ pickImport('missing'); });
      if(CLOUD && session) add('Sair desta conta', async function(){ await Auth.signOut(); location.reload(); });
      root.appendChild(menu);
    }
    function closeMenu(){ if(menu){ menu.remove(); menu = null; } }
    function pickImport(mode){
      var inp = document.createElement('input'); inp.type = 'file'; inp.accept = 'application/json,.json';
      inp.addEventListener('change', async function(){
        var f = inp.files && inp.files[0]; if(!f) return;
        try{ var n = await importAll(JSON.parse(await f.text()), mode); toast(n + ' item(ns) importado(s).'); }
        catch(e){ toast('Não consegui importar: ' + (e.message || e)); }
      });
      inp.click();
    }
    function toast(msg){
      if(!ensure()) return;
      var t = el('<div style="position:fixed;left:50%;bottom:calc(20px + env(safe-area-inset-bottom));transform:translateX(-50%);z-index:99999;background:#1F1B18;color:#EFE7D6;padding:10px 14px;border-radius:9px;font:14px Nunito,system-ui,sans-serif;max-width:90vw;box-shadow:0 10px 30px -10px rgba(0,0,0,.5)"></div>');
      t.textContent = msg; root.appendChild(t); setTimeout(function(){ t.remove(); }, 3800);
    }
    /* Conta nova (nada na nuvem): antes de o app abrir — senão ele cria as categorias padrão e
       elas se misturariam com as do backup — pergunta se é para trazer o backup. */
    function emptyAccountStep(form){
      return new Promise(function(resolve){
        form.classList.remove('ac-busy');
        form.innerHTML = '<h2>Conta pronta</h2><p>Ela ainda está vazia. Quer trazer o que você já tinha na Agenda do Claude?</p>' +
          '<button class="pri" type="button" data-imp>Importar backup (.json)</button>' +
          '<button class="link" type="button" data-zero>Começar do zero</button><div class="ac-msg"></div>';
        var msgEl = form.querySelector('.ac-msg');
        form.querySelector('[data-zero]').addEventListener('click', function(){ resolve(); });
        form.querySelector('[data-imp]').addEventListener('click', function(){
          var inp = document.createElement('input'); inp.type = 'file'; inp.accept = 'application/json,.json';
          inp.addEventListener('change', async function(){
            var f = inp.files && inp.files[0]; if(!f) return;
            try{
              msgEl.className = 'ac-msg ok'; msgEl.textContent = 'Importando…';
              var n = await importAll(JSON.parse(await f.text()), 'missing');
              msgEl.textContent = n + ' itens importados. Enviando para a nuvem…';
              await Sync.run();
              resolve();
            }catch(e){ msgEl.className = 'ac-msg'; msgEl.textContent = 'Não consegui importar: ' + (e.message || e); }
          });
          inp.click();
        });
      });
    }
    var onDone = null;
    function gate(opts){ return new Promise(function(resolve){ onDone = resolve; showLogin(opts && opts.msg, opts); }); }
    var GOOGLE_G = '<svg width="18" height="18" viewBox="0 0 48 48" aria-hidden="true"><path fill="#FFC107" d="M43.6 20.5H42V20H24v8h11.3C33.7 32.7 29.2 36 24 36c-6.6 0-12-5.4-12-12s5.4-12 12-12c3.1 0 5.8 1.2 7.9 3.1l5.7-5.7C34 6.1 29.3 4 24 4 12.9 4 4 12.9 4 24s8.9 20 20 20 20-8.9 20-20c0-1.3-.1-2.4-.4-3.5z"/><path fill="#FF3D00" d="M6.3 14.7l6.6 4.8C14.7 15.1 19 12 24 12c3.1 0 5.8 1.2 7.9 3.1l5.7-5.7C34 6.1 29.3 4 24 4 16.3 4 9.7 8.3 6.3 14.7z"/><path fill="#4CAF50" d="M24 44c5.2 0 9.9-2 13.4-5.2l-6.2-5.2C29.2 35.1 26.7 36 24 36c-5.2 0-9.6-3.3-11.3-8l-6.5 5C9.5 39.6 16.2 44 24 44z"/><path fill="#1976D2" d="M43.6 20.5H42V20H24v8h11.3c-.8 2.2-2.2 4.2-4.1 5.6l6.2 5.2C37 39.2 44 34 44 24c0-1.3-.1-2.4-.4-3.5z"/></svg>';
    function showLogin(msg, opts){
      if(!ensure() || loginEl) return;
      var mode = 'in';
      loginEl = el('<div class="ac-login"><form class="ac-card" autocomplete="on">' +
        '<h2>Agenda</h2><p class="ac-sub">Entre para ver suas coisas em todos os aparelhos.</p>' +
        '<button class="ac-google" type="button" data-google>' + GOOGLE_G + '<span>Continuar com o Google</span></button>' +
        '<div class="ac-or"><span>ou com e-mail e senha</span></div>' +
        '<input name="email" type="email" placeholder="seu e-mail" autocomplete="username" required>' +
        '<input name="password" type="password" placeholder="senha" autocomplete="current-password" minlength="6" required>' +
        '<button class="pri" type="submit">Entrar</button>' +
        '<button class="link" type="button" data-sw>Primeira vez? Criar conta</button>' +
        '<div class="ac-msg"></div></form></div>');
      var form = loginEl.querySelector('form'), m = loginEl.querySelector('.ac-msg'), pri = loginEl.querySelector('.pri'), sw = loginEl.querySelector('[data-sw]');
      if(msg) m.textContent = msg;
      sw.addEventListener('click', function(){
        mode = mode === 'in' ? 'up' : 'in';
        pri.textContent = mode === 'in' ? 'Entrar' : 'Criar conta';
        sw.textContent = mode === 'in' ? 'Primeira vez? Criar conta' : 'Já tenho conta';
        form.password.autocomplete = mode === 'in' ? 'current-password' : 'new-password';
        m.textContent = ''; m.className = 'ac-msg';
      });
      function errText(err){
        var t = String(err && err.message || err);
        if(/invalid login|invalid_credentials/i.test(t)) return 'E-mail ou senha incorretos.';
        if(/email not confirmed/i.test(t)) return 'Falta confirmar o e-mail (veja sua caixa de entrada).';
        if(/failed to fetch|network/i.test(t)) return 'Sem conexão com o servidor. Confira a internet.';
        if(/provider is not enabled|unsupported provider/i.test(t)) return 'O login com Google ainda não foi ligado no Supabase.';
        if(/already registered/i.test(t)) return 'Esse e-mail já tem conta. Clique em "Já tenho conta".';
        return t;
      }
      // depois de entrar (por senha ou voltando do Google): baixa os dados e, se a conta for nova, oferece o backup
      async function finish(){
        m.className = 'ac-msg ok'; m.textContent = 'Baixando suas coisas…';
        await Sync.run();
        if(Sync.state === 'error') throw new Error(Sync.lastErr || 'Não consegui baixar seus dados');
        if(needReload){ location.reload(); return; }
        if(countDocs() === 0) await emptyAccountStep(form);
        hideLogin();
        if(onDone){ var f = onDone; onDone = null; f(); }
      }
      loginEl.querySelector('[data-google]').addEventListener('click', async function(){
        var b = this; b.disabled = true; m.className = 'ac-msg'; m.textContent = '';
        try{ await Auth.google(); }catch(err){ b.disabled = false; m.textContent = errText(err); }
      });
      form.addEventListener('submit', async function(e){
        e.preventDefault(); m.className = 'ac-msg'; m.textContent = ''; pri.disabled = true;
        var email = form.email.value.trim(), pw = form.password.value;
        try{
          if(mode === 'up'){
            var r = await Auth.signUp(email, pw);
            if(r === 'confirm'){ m.className = 'ac-msg ok'; m.textContent = 'Conta criada. Confirme pelo link que chegou no seu e-mail e depois entre aqui.'; mode = 'up'; sw.click(); pri.disabled = false; return; }
          } else {
            await Auth.signIn(email, pw);
          }
          await finish();
        }catch(err){ pri.disabled = false; m.textContent = errText(err); }
      });
      if(opts && opts.resume){
        form.classList.add('ac-busy');
        finish().catch(function(err){ form.classList.remove('ac-busy'); m.className = 'ac-msg'; m.textContent = errText(err); });
      }
      root.appendChild(loginEl);
      if(!(opts && opts.resume) && matchMedia('(min-width: 700px)').matches) setTimeout(function(){ try{ form.email.focus(); }catch(_){} }, 50);
    }
    function hideLogin(){ if(loginEl){ loginEl.remove(); loginEl = null; } }
    function update(){ if(!ensure()) return; var s = !CLOUD ? 'local' : !session ? 'out' : Sync.state; dot.setAttribute('data-s', s); dot.title = statusText()[0]; }
    var updEl = null;
    function showUpdate(){
      if(!ensure() || updEl) return;
      updEl = el('<div class="ac-upd" role="status"><span>Nova versão da Agenda</span><button type="button" class="ac-upd-go">Atualizar</button><button type="button" class="ac-upd-x" aria-label="Agora não">✕</button></div>');
      updEl.querySelector('.ac-upd-go').addEventListener('click', function(){
        var b = this; b.disabled = true; b.textContent = 'Salvando…';
        // tira o foco do campo que estiver sendo editado, para a nota/tarefa ser gravada antes de recarregar
        try{ if(document.activeElement && document.activeElement.blur) document.activeElement.blur(); }catch(_){}
        setTimeout(function(){ location.reload(); }, 700);
      });
      updEl.querySelector('.ac-upd-x').addEventListener('click', function(){ updEl.remove(); updEl = null; });
      root.appendChild(updEl);
    }
    return {update:update, showLogin:showLogin, hideLogin:hideLogin, toast:toast, pickImport:pickImport, ensure:ensure, gate:gate, showUpdate:showUpdate};
  })();

  function countDocs(){ var t = 0; Object.keys(mem).forEach(function(c){ t += Object.keys(mem[c]).length; }); return t; }

  /* ================= versão nova do app ================= */
  // Cada publicação tem um número (data-hora). A página sabe o dela (AGENDA_BUILD); o service worker,
  // o da versão mais nova que ele baixou. Se o dele for maior, aparece o aviso de atualizar.
  var BUILD = window.AGENDA_BUILD || '';
  function setupUpdates(){
    if(!('serviceWorker' in navigator) || location.protocol === 'file:') return;
    var sw = navigator.serviceWorker;
    function ask(){
      var c = sw.controller; if(!c || !BUILD) return;
      try{
        var ch = new MessageChannel();
        ch.port1.onmessage = function(e){ var v = e.data && e.data.version; if(v && v > BUILD) Ui.showUpdate(); };
        c.postMessage({type:'version'}, [ch.port2]);
      }catch(_){}
    }
    sw.register('sw.js').then(function(reg){
      if(!reg) return;
      function check(){ try{ reg.update().catch(function(){}); }catch(_){} }
      // procura versão nova ao voltar para o app (o iPhone deixa o app "congelado" aberto), ao voltar a
      // internet e a cada 30 min com ele aberto
      document.addEventListener('visibilitychange', function(){ if(document.visibilityState === 'visible') check(); });
      window.addEventListener('online', check);
      setInterval(check, 30 * 60 * 1000);
    }).catch(function(e){ console.warn('[agenda] service worker', e); });
    sw.addEventListener('controllerchange', function(){ ask(); sendAuth(); });
    sw.addEventListener('message', function(e){
      if(e.data && e.data.type === 'need-auth' && session){ refreshIfNeeded(true).then(sendAuth, sendAuth); }
    });
    ask(); sendAuth();
  }
  window.__agendaCheckUpdate = function(){ return navigator.serviceWorker && navigator.serviceWorker.getRegistration().then(function(r){ return r && r.update(); }); };

  /* ================= partida ================= */
  var ready = (async function(){
    try{ idb = await idbOpen(); await loadLocal(); }
    catch(e){ console.error('[agenda] IndexedDB indisponível — rodando só em memória', e); idb = null; persist = function(){ return Promise.resolve(); }; }
    if(document.readyState === 'loading') await new Promise(function(r){ document.addEventListener('DOMContentLoaded', r, {once:true}); });
    Ui.update();
    setupUpdates();
    if(CLOUD){
      startLoops();
      // Sem sessão: o app só abre depois do login e da primeira descida dos dados. Abrir antes faria
      // a Agenda achar o banco vazio e criar as categorias padrão por cima da sua conta.
      var back = null;
      try{ back = await Auth.completeRedirect(); }catch(e){ back = {error:e && e.message || String(e)}; }
      if(back && back.ok) await Ui.gate({resume:true});
      else if(!session){
        var gm = '';
        if(back && back.error) gm = /not enabled|unsupported provider/i.test(back.error) ? 'O login com Google ainda não foi ligado no Supabase.'
          : /access_denied|cancel/i.test(back.error) ? 'Login com Google cancelado.' : 'Não deu para entrar com o Google: ' + back.error;
        await Ui.gate({msg:gm});
      }
      else if(!lsGet(LS_CURSOR)) await Promise.race([Sync.run(), new Promise(function(r){ setTimeout(r, 8000); })]);
      else Sync.run();
    }
    appOpen = true;
    return true;
  })();

  window.AgendaCloud = {db:db, assets:assets, downloads:downloads, sync:Sync, auth:Auth, ui:Ui,
    exportAll:exportAll, importAll:importAll, assetUrl:assetPublicUrl, ready:ready,
    get session(){ return session; }, cloudEnabled:CLOUD};

  // A mesma "porta" que o app usava dentro do Claude.
  window.claude = window.claude || {};
  window.claude.use = function(name){
    return ready.then(function(){
      if(name === 'db') return db;
      if(name === 'assets') return CLOUD ? assets : null;
      if(name === 'downloads') return downloads;
      return null;
    });
  };
})();
