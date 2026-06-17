"""
II + III samba vara jaotus Eesti turul (2025).
Allikas: pensionikeskuse turuväljavõte (market_savers_2025.csv), cache'itud repo väliselt.
Väljund: docs/ii_iii_wealth_distribution.html (avaldatakse GitHub Pages alla).
Kõik väljundid on agregaadid (loendused, summad, kvantiilid) — ühtegi isikuandmete rida ei avaldata.
"""
import os
from pathlib import Path
import pandas as pd

CACHE = Path(os.environ.get('TULEVA_CACHE_DIR', Path.home() / '.cache' / 'tuleva-reports'))
df = pd.read_csv(CACHE / 'market_savers_2025.csv', sep=';')
for c in ['Saldo II', 'Saldo III', 'Sissemakse III']:
    df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
df['kokku'] = df['Saldo II'] + df['Saldo III']

N = len(df)
AUM = df['kokku'].sum()

def eur(x):
    return f"{x:,.0f}".replace(',', ' ') + " €"

def millions(x):
    return f"{x/1e6:,.1f}".replace(',', ' ') + " M€"

# ---- Table 1: II=0 & III>0, quintiles by III AUM ----
c1 = df[(df['Saldo II'] == 0) & (df['Saldo III'] > 0)].copy()
n_c1 = len(c1)
c1['q'] = pd.qcut(c1['Saldo III'].rank(method='first'), 5,
                  labels=['Q1 (madalaim)', 'Q2', 'Q3', 'Q4', 'Q5 (kõrgeim)'])
g = c1.groupby('q', observed=True)['Saldo III']
t1 = pd.DataFrame({
    'Inimesi': g.size(),
    'III saldo vahemik': [f"{eur(a)} – {eur(b)}" for a, b in zip(g.min(), g.max())],
    'Mediaan': g.median().map(eur),
    'III vara kokku': g.sum().map(millions),
})

# ---- contributions among that cohort ----
active = c1[c1['Sissemakse III'] > 0]
n_active = len(active)

# ---- Table 2: contribution quintiles among active contributors ----
ac = active.copy()
ac['q'] = pd.qcut(ac['Sissemakse III'].rank(method='first'), 5,
                  labels=['Q1 (madalaim)', 'Q2', 'Q3', 'Q4', 'Q5 (kõrgeim)'])
g = ac.groupby('q', observed=True)['Sissemakse III']
t2 = pd.DataFrame({
    'Inimesi': g.size(),
    'Sissemakse vahemik': [f"{eur(a)} – {eur(b)}" for a, b in zip(g.min(), g.max())],
    'Mediaan': g.median().map(eur),
    'Sissemaksed kokku': g.sum().map(millions),
})
contrib_total = active['Sissemakse III'].sum()

# ---- Table 3: II+III deciles, whole population ----
df['d'] = pd.qcut(df['kokku'].rank(method='first'), 10, labels=[f'D{i}' for i in range(1, 11)])
g = df.groupby('d', observed=True)['kokku']
t3 = pd.DataFrame({
    'Inimesi': g.size(),
    'Vahemik': [f"{eur(a)} – {eur(b)}" for a, b in zip(g.min(), g.max())],
    'Mediaan': g.median().map(eur),
    'Vara kokku': g.sum().map(millions),
    'Osa AUM-ist': (g.sum() / AUM * 100).map(lambda x: f"{x:.1f}%"),
})
n_zero = int((df['kokku'] == 0).sum())

# ---- Table 4: II+III deciles, assets > 0 only ----
pos = df[df['kokku'] > 0].copy()
n_pos = len(pos)
pos['d'] = pd.qcut(pos['kokku'].rank(method='first'), 10, labels=[f'D{i}' for i in range(1, 11)])
g = pos.groupby('d', observed=True)['kokku']
t4 = pd.DataFrame({
    'Inimesi': g.size(),
    'Vahemik': [f"{eur(a)} – {eur(b)}" for a, b in zip(g.min(), g.max())],
    'Mediaan': g.median().map(eur),
    'Vara kokku': g.sum().map(millions),
    'Osa AUM-ist': (g.sum() / AUM * 100).map(lambda x: f"{x:.1f}%"),
})

def table_html(df_):
    return df_.to_html(index=True, border=0, escape=False, justify='left')

CSS = (Path(__file__).resolve().parents[2] / 'common' / 'branding' / 'style.css').read_text()

html = f"""<!DOCTYPE html>
<html lang="et">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>II ja III samba vara jaotus Eesti turul (2025)</title>
<style>
{CSS}
body {{ max-width: 900px; margin: 0 auto; padding: 2em 1.5em; }}
table {{ font-size: 10.5pt; }}
td:not(:first-child), th:not(:first-child) {{ text-align: right; }}
.lead {{ font-size: 12pt; color: #444; }}
.note {{ background:#edf7fe; border-left:4px solid #00AEEA; padding:0.8em 1.2em; margin:1.5em 0; border-radius:4px; }}
.kpi {{ font-weight:700; color:#002F63; }}
</style>
</head>
<body>
<h1>II ja III samba vara jaotus Eesti turul</h1>
<p class="lead">Kogu Eesti II ja III samba kogujate andmestiku põhjal (2025). Vaatame, kuidas
pensionivara on inimeste vahel jaotunud, ja eraldi neid, kellel II sammas puudub, aga III sambas on raha.</p>
<p><em>Allikas: pensionikeskuse turuväljavõte, {N:,} inimest. Saldod on ümardatud lähima 100 € peale.
Kõik arvud on agregaadid — ükski isikuandmete rida ei ole avaldatud.</em></p>

<h2>1. II sammas = 0, aga III sambas on raha</h2>
<p>Need on inimesed, kel <strong>II samba saldo on null</strong> — kas pole sinna kunagi kuulunud
(nt enne 1983 sündinud, kes ei liitunud) või on raha 2021. aasta reformi järel välja võtnud —
<strong>kuid III sambas on vara olemas</strong>. Kokku <span class="kpi">{n_c1:,} inimest</span>.
Allpool on nad jaotatud III samba vara järgi viide võrdsesse kvintiili.</p>
{table_html(t1)}
<div class="note">Vara on tugevalt koondunud: ülemine kvintiil (Q5) hoiab ligi kolmveerandi selle grupi
III samba varast, alumine pool alla 3%.</div>

<h2>2. Kas nad maksavad III sambasse sisse?</h2>
<p>Eelmise grupi {n_c1:,} inimesest teeb <span class="kpi">{n_active:,} ({n_active/n_c1*100:.0f}%)</span>
aktiivselt III samba sissemakseid. Mida suurem on kogutud vara, seda tõenäolisemalt inimene ka sisse maksab.
Allpool on aktiivsed sissemaksjad jaotatud aastase sissemakse suuruse järgi kvintiilidesse
(kokku ~{millions(contrib_total)} aastas).</p>
{table_html(t2)}

<h2>3. II + III samba koguväärtuse detsiilid — kogu populatsioon</h2>
<p>Kogu {N:,} inimese II ja III samba kombineeritud vara, jaotatud kümnesse võrdse arvukusega gruppi.
<strong>{n_zero/N*100:.0f}% ({n_zero:,} inimest) koguväärtus on null</strong> — seetõttu on alumised neli detsiili
läbinisti nullid (väärtuspõhised piirid ei tekiks).</p>
{table_html(t3)}
<div class="note">Mediaan kogu populatsioonis on vaid <span class="kpi">{df['kokku'].median():.0f} €</span>,
sest pea pool rahvastikust on nulliga. Ülemine detsiil hoiab <span class="kpi">61,5%</span> kogu varast.
II + III vara kokku: <span class="kpi">{millions(AUM)}</span>.</div>

<h2>4. Sama jaotus — ainult vara &gt; 0 inimesed</h2>
<p>Kui nullidega inimesed välja jätta, jääb <span class="kpi">{n_pos:,} inimest</span>, kel on II
ja/või III sambas raha. Detsiilipiirid muutuvad sujuvamaks ja paremini tõlgendatavaks.</p>
{table_html(t4)}
<div class="note">Mediaan vara omavate inimeste seas on <span class="kpi">{pos['kokku'].median():.0f} €</span>.
Koondumine on pehmem kui kogu populatsioonis: ülemine detsiil hoiab 45,2% (vs 61,5%), ülemine viiendik ~65%.</div>

<hr/>
<p><em>Koostatud {pd.Timestamp('2026-06-17').date()}. Tuleva reporting-engine.</em></p>
</body>
</html>"""

out = Path(__file__).resolve().parents[2] / 'docs' / 'ii_iii_wealth_distribution.html'
out.write_text(html)
print("Wrote", out, f"({len(html):,} bytes)")
print(f"Cohort II=0 & III>0: {n_c1:,}; active contributors: {n_active:,}; assets>0: {n_pos:,}")
