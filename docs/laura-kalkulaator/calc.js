/* Laura kalkulaator — jagatud mootor (miljoni- ja võrdlusleht).
   Andmed: kogu Eesti II+III samba registriväljavõte (~925k, pseudonüümitud, vanus kümnendini ümardatud). */
const DATA = {
  meta:{ total:924849, living:882564, pctlGrid:[0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,100] },
  byDecade:{
    "20":{totPctls:[0,0,0,0,0,0,100,100,200,300,400,500,700,900,1200,1500,2000,2500,3300,4600,112300],meanTot:1128},
    "30":{totPctls:[0,0,0,0,0,0,0,200,500,1100,1800,2800,3900,5200,6600,8300,10400,13100,17000,24300,146300],meanTot:6002},
    "40":{totPctls:[0,0,0,0,0,0,0,0,0,200,700,1400,2700,4700,8100,12500,17400,23100,30700,44629,654000],meanTot:9565},
    "50":{totPctls:[0,0,0,0,0,0,0,0,0,600,1700,3400,6100,9900,14100,19200,25000,32000,42200,62100,767400],meanTot:14219},
    "60":{totPctls:[0,0,0,0,0,0,0,0,100,900,2600,5300,8700,12200,15800,19700,24400,30200,38600,56200,2342400],meanTot:14154},
    "70":{totPctls:[0,0,0,0,0,0,0,0,0,0,0,0,0,0,100,900,4100,10200,18200,31300,1952200],meanTot:5426}
  },
  // ausad numbrid (elavad). "teel" = kasutab Laura hoobasid, EI tähenda garanteeritud miljonit.
  stats:{ millionaires:5, recipe_loose:49328, recipe_both:30659, recipe_laura:10627, recipe_max:5205, pct_2:88.7, no_iii:83.2 }
};

const $ = id => document.getElementById(id);
const fmtE = new Intl.NumberFormat('et-EE',{maximumFractionDigits:0}).format;
const eur = n => fmtE(Math.round(n)) + ' €';
const setText = (id,t)=>{ const e=$(id); if(e) e.textContent=t; };
const setHTML = (id,h)=>{ const e=$(id); if(e) e.innerHTML=h; };
const setW = (id,w)=>{ const e=$(id); if(e) e.style.width=w; };
let rate = 2;
const ticked = new Set();

/* Projektsioon: II = (määr+4)% brutost (sinu osa + riigi 4%); III = sisestatud €/aastas /12.
   Palk ja III sissemakse kasvavad 3%/a. Tootluse sisestab kasutaja ise (me ei prognoosi). */
function project(o){
  const rm = Math.pow(1+o.toot/100,1/12)-1, g=o.palgakasv/100;
  const iiR=(o.rate+4)/100;
  let salary=o.palk, iiiAnnual=o.iiiAnnual, ii=o.saldoII, iii=o.saldoIII;
  let millAge=(ii+iii>=1e6)?o.vanus:null;
  for(let age=o.vanus; age<o.pension; age++){
    const iiiM=iiiAnnual/12;
    for(let m=0;m<12;m++){ ii=ii*(1+rm)+salary*iiR; iii=iii*(1+rm)+iiiM; }
    salary*=(1+g); iiiAnnual*=(1+g);
    if(millAge===null && ii+iii>=1e6) millAge=age+1;
  }
  const retireTotal=ii+iii;
  // miljoni ületamise vanus pärast pensioniiga: ainult kasv, väljamakseteta (kui tootlus>0)
  if(millAge===null && o.toot>0){ let t=ii+iii,a=o.pension; while(t<1e6&&a<101){t*=(1+o.toot/100);a++;} if(t>=1e6)millAge=a; }
  return {retireTotal, ii, iii, millAge};
}
const nearestDecade = age => ''+[20,30,40,50,60,70].reduce((p,c)=>Math.abs(c-age)<Math.abs(p-age)?c:p);
function percentile(total,pctls,grid){
  if(total<=pctls[0]) return 0;
  for(let i=1;i<pctls.length;i++){ if(total<=pctls[i]){ const span=pctls[i]-pctls[i-1]||1; return grid[i-1]+((total-pctls[i-1])/span)*(grid[i]-grid[i-1]); } }
  return 100;
}
const read = () => ({
  vanus:+$('vanus').value, palk:+$('palk').value||0, saldoII:+$('saldoII').value||0, saldoIII:+$('saldoIII').value||0,
  rate, iiiAnnual:+$('iii').value||0, pension:67, toot:parseFloat($('toot').value), palgakasv:3
});

function renderChecklist(o, canProj){
  if(!$('checklist')) return;
  const items=[];
  const doneII = o.rate>=6;
  let impII=''; if(canProj && !doneII){ impII='+'+eur(project({...o,rate:6}).retireTotal - project(o).retireTotal); }
  items.push({done:doneII, h:'Tõsta II samba makse 6%-le',
    p: doneII?'Tehtud. Kasutad ära suurima II samba makse.':'Riik lisab oma osa juurde. Suuremaks läheb ainult sinu enda makse.',
    a:['Kuidas maksudelt võita →','https://tuleva.ee/laura-rikkaks/kuidas-maksudelt-voita/'], imp:impII});

  const target = Math.round(0.15*o.palk*12);
  const doneIII = o.iiiAnnual>0 && o.iiiAnnual>=target*0.9;
  let impIII=''; if(canProj && !doneIII){ impIII='+'+eur(project({...o,iiiAnnual:target}).retireTotal - project(o).retireTotal); }
  const refund = Math.min(o.iiiAnnual||target, 0.15*o.palk*12, 6000)*0.22;
  const overCap = o.iiiAnnual>6000;
  items.push({done: overCap ? true : doneIII,
    h: overCap ? 'Üle maksuvõidu piiri? Jätka Tuleva Täiendavas Kogumisfondis' : (o.iiiAnnual>0 ? 'Pane III sambasse kuni 15% sissetulekust' : 'Ava III sammas ja sea püsimakse'),
    p: overCap ? 'III samba maksuvõit kehtib kuni 6000 € sissemakselt aastas. Üle selle võiks raha minna Tuleva Täiendavasse Kogumisfondi. Seal kehtib sama maailma aktsiate strateegia, ilma pensionisammaste maksueeliseta.'
              : (doneIII?`Tehtud. Riik tagastab sulle ~${eur(refund)}/aastas tulumaksu.`:`Laura paneb 15% palgast (~${eur(target)}/aastas). Riik tagastab ~${eur(refund)}/aastas tulumaksu.`),
    a: overCap ? ['Täiendav Kogumisfond →','https://tuleva.ee/taiendav-kogumisfond/'] : ['III sammas →','https://tuleva.ee/iii-sammas/'], imp: overCap ? '' : impIII});

  items.push({done:false, h:'Vali madala tasuga indeksfond',
    p:'Me ei näe, kui palju sinu fond tasu võtab. Laura-seeria näites tähendab 0,5% ja 1,5% tasu vahe miljoni juures umbes 200 000 € erinevust (samadel eeldustel).',
    a:['Miks iga fond pole hea →','https://tuleva.ee/laura-rikkaks/miks-iga-pensionifond-pole-hea-valik/'], imp:''});
  items.push({done:false, h:'Alusta juba täna',
    p:'Aeg on suurim abimees. Sama sissemakse 25-aastaselt alustades kasvab kordades suuremaks kui 50-aastaselt.',
    a:['Tee 15 minutiga korda →','https://tuleva.ee/laura-rikkaks/15-minutiga-miljonariks/'], imp:''});

  $('checklist').innerHTML = items.map(function(x,i){
    var done = x.done || ticked.has(i);
    return '<div class="check '+(done?'done':'')+'" data-i="'+i+'" role="button" tabindex="0">'
      +'<div class="box">'+(done?'✓':'')+'</div>'
      +'<div class="body"><h3>'+x.h+'</h3><p>'+x.p+'</p><a href="'+x.a[1]+'" target="_blank" rel="noopener">'+x.a[0]+'</a></div>'
      +'<div class="impact">'+(done?'✓ tehtud':x.imp)+'</div></div>';
  }).join('');
}

function updateTootUI(){
  const el=$('toot'); if(!el) return;
  const min=+el.min, max=+el.max, val=+el.value||0;
  const pct=(val-min)/(max-min);
  const b=$('tootBubble');
  if(b){ b.textContent=val+'%'; b.style.left=`calc(${pct*100}% + ${(0.5-pct)*1.25}rem)`; }
  const m=$('tootMark');
  if(m){ const mp=(7-min)/(max-min); m.style.left=`calc(${mp*100}% + ${(0.5-mp)*1.25}rem)`; }
}

function render(){
  const o=read();
  const mode = (document.body.dataset.mode||'million');
  updateTootUI();
  const validAges = o.pension > o.vanus;

  // --- VÕRDLUS (ei vaja tootlust, näidatakse alati) ---
  const today=o.saldoII+o.saldoIII;
  const bm=DATA.byDecade[nearestDecade(o.vanus)];
  const p = today>0 ? percentile(today,bm.totPctls,DATA.meta.pctlGrid) : 0;
  const median = bm.totPctls[10];
  const medStr = median>0 ? ` <span class="muted">Pooltel sinuvanustest on alla ${eur(median)}.</span>` : '';
  if(today>0){
    setHTML('cmpText',`Oled täna kogunud <b>${eur(today)}</b>, rohkem kui <b>${Math.round(p)}%</b> sinuvanustest.`+medStr);
    setText('percentBig', Math.round(p)+'%');
    setHTML('percentSub', `eakaaslastest on kogunud vähem kui sina.`+medStr);
  } else {
    setHTML('cmpText',`Sa pole veel alustanud. Juba esimene euro viib sind ettepoole.`+medStr);
    setText('percentBig','Alusta!');
    setHTML('percentSub', `Sa pole veel alustanud. Juba esimene sissemakse viib sind ettepoole.`);
  }
  setW('barFill',p+'%');

  renderChecklist(o, validAges);

  // --- PROJEKTSIOON (vajab kehtivat pensioniiga; tootluse määrab liugur) ---
  const projOK = validAges;
  if($('projPrompt')) $('projPrompt').style.display = projOK?'none':'block';
  if($('projResults')) $('projResults').style.display = projOK?'block':'none';
  setText('projPromptMsg','Oled juba pensionieas. Vaata ülalt, kuidas seisad teistega võrreldes.');

  if(projOK){
    const b=project(o);
    setText('rAge',o.pension);
    setText('rTotal',eur(b.retireTotal));
    setText('rII',eur(b.ii));
    setText('rIII',eur(b.iii));
    const scale=Math.max(1e6,b.retireTotal)*1.05;
    setW('jRetire',(b.retireTotal/scale*100)+'%');
    setW('jToday',(today/scale*100)+'%');
    const jm=$('jMill'); if(jm) jm.style.left=(1e6/scale*100)+'%';
    setText('jL1',eur(scale)); setText('kToday',eur(today)); setText('kRetire',eur(b.retireTotal));
    const v=$('verdict');
    if(v){
      if(b.millAge!==null && b.millAge<=o.pension){
        v.className='verdict win';
        v.innerHTML=`Selle eeldusega oleksid <span class="em">miljoni kursil juba ${b.millAge}-aastaselt</span>, veel enne pensioniiga.`;
      } else if(b.millAge!==null){
        v.className='verdict win';
        v.innerHTML=`Selle eeldusega ületaks sinu pensionivara <span class="em">miljoni piiri ${b.millAge}-aastaselt</span>, kui jätad raha edasi kasvama.`;
      } else {
        v.className='verdict';
        v.innerHTML=`Sinu raha kasvab usinasti. Vaata kontroll-lehelt, kui palju lisavad mõned sammud sinu pensionivarale.`;
      }
    }
    setText('rMill', b.millAge!==null ? (b.millAge+'-aastaselt') : '…');
    window._proj=b;
  } else { window._proj=null; }

  // --- jaga-tekst ---
  const url = location.href.split('#')[0];
  if(mode==='compare'){
    window._share = (today>0 ? `Olen oma vanuses ${Math.round(p)}% eakaaslastest ees oma pensionivaraga.` : 'Vaatasin, kus ma oma pensionivaraga seisan.') + ' Vaata sina ka: '+url;
  } else {
    const b=window._proj;
    window._share = (b && b.millAge && b.millAge<=o.pension
      ? `Minu eeldusega ületaks mu pensionivara miljoni piiri ${b.millAge}-aastaselt!`
      : 'Vaatasin, kas olen teel pensionimiljonäriks.') + ' Arvuta sina ka: '+url;
  }
}

function initShare(){
  const btn=$('share'); if(!btn) return;
  btn.addEventListener('click',()=>{
    (navigator.clipboard?navigator.clipboard.writeText(window._share):Promise.reject()).then(()=>{
      const t=$('toast'); if(t){ t.classList.add('show'); setTimeout(()=>t.classList.remove('show'),2200); }
    }).catch(()=>{});
  });
}

function init(){
  if($('rate')) $('rate').addEventListener('click',e=>{
    if(e.target.tagName!=='BUTTON') return;
    rate=+e.target.dataset.v;
    [...$('rate').children].forEach(btn=>btn.classList.toggle('on',btn===e.target));
    render();
  });
  ['vanus','palk','saldoII','saldoIII','iii','toot'].forEach(id=>{ if($(id)) $(id).addEventListener('input',render); });
  if($('checklist')) $('checklist').addEventListener('click',function(e){
    if(e.target.closest('a')) return;
    const c=e.target.closest('.check'); if(!c) return;
    const i=+c.dataset.i;
    if(ticked.has(i)) ticked.delete(i); else ticked.add(i);
    render();
  });
  // statistikud lehele
  setText('s_mill', DATA.stats.millionaires);
  setText('s_loose', fmtE(DATA.stats.recipe_loose));
  setText('s_both', fmtE(DATA.stats.recipe_both));
  setText('s_laura', fmtE(DATA.stats.recipe_laura));
  setText('s_pct2', Math.round(DATA.stats.pct_2)+'%');
  setText('s_noiii', Math.round(DATA.stats.no_iii)+'%');
  initShare();
  render();
}
document.addEventListener('DOMContentLoaded', init);
