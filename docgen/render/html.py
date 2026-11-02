"""Single-file HTML viewer that renders the generated Markdown + Mermaid in a browser."""
from __future__ import annotations

import json
from typing import Dict

TEMPLATE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>__TITLE__</title>
<script src="https://cdn.jsdelivr.net/npm/marked@12.0.2/marked.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.min.js"></script>
<style>
:root{--bg:#fff;--fg:#1f2328;--mut:#59636e;--line:#d1d9e0;--side:#f6f8fa;--acc:#0969da}
@media (prefers-color-scheme:dark){:root{--bg:#0d1117;--fg:#e6edf3;--mut:#9198a1;--line:#30363d;--side:#151b23;--acc:#4493f8}}
*{box-sizing:border-box}body{margin:0;font:15px/1.55 system-ui,-apple-system,"Segoe UI",sans-serif;background:var(--bg);color:var(--fg);display:flex;min-height:100vh}
nav{width:260px;flex:none;background:var(--side);border-right:1px solid var(--line);padding:16px;position:sticky;top:0;height:100vh;overflow:auto}
nav h2{font-size:13px;text-transform:uppercase;letter-spacing:.06em;color:var(--mut);margin:16px 0 6px}
nav a{display:block;padding:4px 8px;border-radius:6px;color:var(--fg);text-decoration:none;font-size:14px}
nav a.on,nav a:hover{background:var(--line)}
main{flex:1;min-width:0;padding:24px 40px;max-width:1300px}
table{border-collapse:collapse;display:block;overflow:auto;font-size:13px;margin:8px 0}
th,td{border:1px solid var(--line);padding:4px 8px;text-align:left;vertical-align:top}th{background:var(--side)}
code{background:var(--side);padding:1px 4px;border-radius:4px;font-size:12.5px}
.mermaid{background:#fff;border:1px solid var(--line);border-radius:8px;padding:12px;overflow:auto;margin:12px 0}
blockquote{color:var(--mut);border-left:3px solid var(--line);margin:0;padding-left:12px}
a{color:var(--acc)}
@media (max-width:800px){body{display:block}nav{width:auto;height:auto;position:static}main{padding:16px}}
</style></head><body>
<nav><strong>__TITLE__</strong><h2>Overview</h2><a href="#index.md">Index</a><h2>Services</h2><div id="svcs"></div><h2>Features &amp; functions</h2><div id="feats"></div></nav>
<main id="doc"></main>
<script>
const DOCS = __DOCS__;
mermaid.initialize({startOnLoad:false, securityLevel:'loose', theme:'default', flowchart:{htmlLabels:true}});
const svcs = document.getElementById('svcs');
const feats = document.getElementById('feats');
Object.keys(DOCS).filter(k=>k.startsWith('services/')).sort().forEach(k=>{
  const a=document.createElement('a'); a.href='#'+k; a.textContent=k.slice(9,-3); svcs.appendChild(a);});
Object.keys(DOCS).filter(k=>k.startsWith('features/')).sort().forEach(k=>{
  const a=document.createElement('a'); a.href='#'+k; a.textContent=k.slice(9,-3); feats.appendChild(a);});
async function renderVisible(root){
  const nodes=[...root.querySelectorAll('.mermaid:not([data-processed])')].filter(d=>!d.closest('details:not([open])'));
  if(nodes.length) await mermaid.run({nodes});
}
async function show(){
  let key = decodeURIComponent(location.hash.slice(1)) || 'index.md';
  if(!DOCS[key]) key='index.md';
  const base = key.includes('/') ? key.split('/')[0]+'/' : '';
  const el=document.getElementById('doc');
  el.innerHTML = marked.parse(DOCS[key]);
  el.querySelectorAll('a[href$=".md"]').forEach(a=>{
    let h=a.getAttribute('href'); if(/^https?:/.test(h)) return;
    let p=(base+h).split('/'), r=[]; p.forEach(s=>{ if(s==='..') r.pop(); else if(s!=='.') r.push(s);});
    a.setAttribute('href','#'+r.join('/'));});
  el.querySelectorAll('code.language-mermaid').forEach((c,i)=>{
    const d=document.createElement('div'); d.className='mermaid'; d.textContent=c.textContent; c.parentElement.replaceWith(d);});
  document.querySelectorAll('nav a').forEach(a=>a.classList.toggle('on', a.getAttribute('href')==='#'+key));
  el.querySelectorAll('details').forEach(d=>d.addEventListener('toggle',()=>{ if(d.open) renderVisible(d); }));
  el.querySelectorAll('h1,h2,h3').forEach(h=>{ h.id=h.textContent.toLowerCase().replace(/[^a-z0-9 -]/g,'').replace(/ /g,'-'); });
  el.querySelectorAll('a[href^="#feature"]').forEach(a=>{ const id=a.getAttribute('href').slice(1);
    a.addEventListener('click',ev=>{ev.preventDefault(); const t=document.getElementById(id); if(t) t.scrollIntoView();}); });
  await renderVisible(el);

}
addEventListener('hashchange', ()=>{ show(); window.scrollTo(0,0); }); show();
</script></body></html>"""


def render_html(title: str, docs: Dict[str, str]) -> str:
    payload = json.dumps(docs, ensure_ascii=False).replace("</", "<\\/")
    return TEMPLATE.replace("__TITLE__", title).replace("__DOCS__", payload)
