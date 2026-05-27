# CLAUDE.md — Tuleva blogipostide protsess

See juhend kehtib ainult `blogposts/` kataloogi sees. Üldised projekti juhised on repos `CLAUDE.md`-s (juur).

## Iga blogiposti kataloog

```
<YYYY-MM-slug>/
  meta.yaml          # autoritaatne metaandmete allikas (skriptid loevad sellest)
  post.md            # WordPressi minev tekst, üldlugejale
  analysis.ipynb     # iseseisev pikem analüüs koos koodi ja allikatega, andmenohikule
  charts/            # PNG/SVG — notebook toodab, mõlemad versioonid viitavad
  data/              # vahedataid (CSV jm)
```

**Slug-konventsioon:** `YYYY-MM-<estonian-slug>` (kuu on planeeritud avaldamise kuu, mitte loomise kuu).

## Kaks paralleelset versiooni — sama lugu

- **`post.md`** on lühem (~600-1000 sõna), igapäevasele lugejale. Viitab notebook'ile lõpus stiilis „*Andmenohikule: kogu analüüs, allikad ja arvutused on avalikud — vt notebookki*".
- **`analysis.ipynb`** on pikem, sisaldab andmepäringud, töötlust, graafikute genereerimise koodi, allikate viited, vahetabelid. Lugejaks pikaajaline koguja, ajakirjanik, akadeemik.

**Graafikud genereerib notebook** ja salvestab `charts/`-i:

```python
fig.savefig(Path('charts') / 'chart-a-ii-samba-churn-2025.png',
            bbox_inches='tight', dpi=150)
```

Sama PNG-failile viitab nii `post.md` (`![](charts/chart-a-...)`) kui notebook (samasugune markdown-lahter selguse mõttes).

## meta.yaml väljad

```yaml
title: ...                              # täielik pealkiri
slug: 2026-05-...                       # sama mis kataloogi nimi
date: 2026-05-27                        # ISO formaadis, planeeritud avaldamine
author: Tõnu
status: draft                           # draft | in_review | published

post_md: post.md                        # tee meta.yaml suhtes
notebook: analysis.ipynb
charts_dir: charts
data_dir: data

google_doc_id: 1xxx...                  # täita kui Doc loodud
google_doc_url: https://...
notebook_html_target: docs/blogposts/<slug>.html
wordpress_url:                          # täita avaldamise järel
```

`status` muutub: `draft` → `in_review` (kolleegidega Doci toimetamine) → `published` (WordPress live).

## Töövoog

### 0. Ülesande püstitamine (`task-<nimi>.md`)

Iga uus blogipost algab ülesande kirjeldusega.

1. Loon posti kataloogi: `blogposts/<YYYY-MM-slug>/`.
2. Sinna loon faili `task-<nimi>.md` (nt `task-fondivalitsejate-aruanded.md`), kuhu kirjutan:
   - **Algne ülesanne** (kasutaja sõnastusega niipalju kui võimalik)
   - **Eesmärk** (mida blogipost peab näitama / mida lugeja peab aru saama)
   - **Sihtgrupp** (üldlugeja / andmenohik / kindel inimene, nt ajakirjanik)
   - **Põhilised andmeallikad ja küsimused** (mida vaja päringutest, kust)
   - **Avatud küsimused** (mida kasutajaga enne edasi minekut täpsustada)
   - **Vahekokkuvõtted** (kui ülesanne on pikk, lisan progress-märkmeid)

`task-*.md` failid on `.gitignore`-is (`blogposts/*/task*.md`) — need on **sisemised planeerimismärkmed**, mitte publikatsiooni osa. Nii ei satu kasutaja töötamise-ajalised mõttekäigud kunagi avalikku repositooriumisse.

Mitu task-faili posti kohta on lubatud, kui ülesanne areneb (`task-esimene-versioon.md`, `task-toimetamisringi-jaoks.md` jne).

### 1. Analüüsi tegemine

Pärast task-faili loomist:
- Loon `meta.yaml`-i (template põhi-väljadega, status: `draft`)
- Loon `analysis.ipynb`-i (notebook, kus teen kõik andmepäringud, töötluse, graafikud)
- Graafikud salvestatakse alati `charts/` kausta (mitte notebook'i juurikas), näiteks:

```python
from pathlib import Path
CHARTS = Path('charts')
CHARTS.mkdir(exist_ok=True)
fig.savefig(CHARTS / 'chart-a-churn.png', bbox_inches='tight', dpi=150)
```

- Andmeid (CSV jm) hoian `data/` kaustas (kui ei ole tundlikku CRM-i, vt repo juur `CLAUDE.md`).

### 2. Blogiposti tekst (`post.md`)

Kui analüüs valmis, kirjutan `post.md`-i:
- YAML frontmatter (sama mis meta.yaml — kaks faili sünkroonis hoida vajalikku miinimumi)
- Lühem üldlugeja versioon, viitab samadele PNG-dele (`![](charts/X.png)`)
- Lõpus „Andmenohikule" sektsioon viidab notebook'i HTML-ile ja repos olevale `.ipynb`-le

### 3. Analüüsi muudatused notebookis

Kui kasutaja palub notebookis muuta arvutust või graafikut:
1. Muudan notebook'i.
2. Käivitan asjasse puutuvad lahtrid (vt jupyter convention juurikas `CLAUDE.md`-s).
3. Kui graafik muutus, salvestub uus PNG samale teele → automaatselt mõjub ka `post.md`-le.
4. Kui mõni number on muutunud, uuendan ka `post.md` prose-osa.
5. Kui Doc on juba olemas (`google_doc_id` täidetud), pakun käsku `push_to_doc.py <slug>` Doc-i sünkroonimiseks. **Ei tee automaatselt** — kolleegid võivad seal juba toimetada.

### 4. Mustand Google Doci

Eeldused:
- Kasutaja on loonud Doci Tuleva Drive'is (shared drive sees, mitte My Drive).
- Doc on jagatud `read-write@tuleva-claude.iam.gserviceaccount.com`-iga Editor-õigustega.
- `meta.yaml`-i on lisatud `google_doc_id` ja `google_doc_url`.

Käivitan:

```bash
.venv/bin/python3 blogposts/_scripts/push_to_doc.py <slug>
```

Skript:
1. Loeb `<slug>/meta.yaml` → tuvastab `post.md` ja `google_doc_id`.
2. Konverdib `post.md` → HTML (markdown + tables/extra extensions).
3. Asendab kohalikud kujundite viited GitHub raw URL-idega (`https://raw.githubusercontent.com/TulevaEE/reporting-engine/main/blogposts/<slug>/charts/...`).
4. Lisab stiilid (Roboto põhitekst, Merriweather pealkirjad, 1.5 reavahe, paragrahvi järel 12pt, tabel: tsentreeritud `th`, 1.15 reavahe, väike padding).
5. Lisab `<img>`-le `width="620"` (Doc lehe laius).
6. Üleslaeb HTML-i Doci kaudu Drive API `files.update(supportsAllDrives=True)`.

**Märkus:** Selleks et graafikud Doci ilmuksid, peavad PNG-d olema juba `main`-haru repos pushitud (et GitHub raw URL töötaks). Kui mitte, hoiata kasutajat.

### 5. Pull-back Doc → md

Kui kasutaja ütleb „tõmba doc tagasi", käivitan:

```bash
# vaikimisi kirjutab post.md.from-doc kõrvale, et saaks diffida:
.venv/bin/python3 blogposts/_scripts/pull_from_doc.py <slug>

# kui diff sobib, kirjutame otse post.md-le:
.venv/bin/python3 blogposts/_scripts/pull_from_doc.py <slug> --apply
```

Skript:
1. Loeb `meta.yaml` → `google_doc_id` ja `post_md`.
2. Tõmbab Doci sisu Docs API kaudu.
3. Konverdib markdownisse: pealkirjad (TITLE, HEADING_1..6), lõigud, **bold**, *italic*, [lingid](url), tsitaadid, tabelid, listid.
4. **Säilitab kujundite viited:** Doci sisselaaditud pildid asendab tagasi originaalsete `![](charts/...)` viidetega `post.md`-st, järjekorra alusel. Kui pildi-numbrid ei klapi, lisab TODO-markeri.
5. Säilitab YAML frontmatter'i olemasolevast `post.md`-st.

Pärast pull-i vaatan üle, kas notebook'is on midagi vaja uuendada (numbreid, viiteid, järeldusi). Kuna Doci kommentaarid (mitte trackitud muudatused) Docs API kaudu sisus pole, peab inimene need eraldi üle vaatama enne pulli.

### 6. Notebook → HTML eksport (docs/)

```bash
.venv/bin/python3 blogposts/_scripts/export_notebook.py <slug>

# kui tahad notebookit enne ka uuesti käivitada:
.venv/bin/python3 blogposts/_scripts/export_notebook.py <slug> --execute
```

Skript:
1. Loeb `meta.yaml` → `notebook` (vaikimisi `analysis.ipynb`) ja `notebook_html_target`.
2. (Valikuline) käivitab notebook'i uuesti `jupyter nbconvert --execute --inplace`.
3. Ekspordib HTML-iks kasutades `blogposts/_scripts/nbconvert_blog_config.py`, mis erinevalt repo juur `common/nbconvert_config.py`-st **näitab koodi** (sihtgrupp: andmenohik).
4. Salvestab `docs/blogposts/<slug>.html` (või `notebook_html_target` järgi).

`docs/index.html`-i uuendamine on käsitsi (lisada link Blog sektsiooni). Skript meenutab.

Notebook on avalik kahel kujul:
- `https://tulevaee.github.io/reporting-engine/blogposts/<slug>.html` — renderdatud HTML
- `https://github.com/TulevaEE/reporting-engine/blob/main/blogposts/<slug>/analysis.ipynb` — raw notebook (GitHub renderdab automaatselt)

Mõlemad lingid lisatakse `post.md` lõppu „Andmenohikule" osasse.

### 7. WordPressi avaldamine

Praegu käsitsi: kasutaja kopeerib `post.md` (või Doci toimetatud versiooni) WordPressi. Hilisemaks on plaan automaatika (WP REST API kaudu) — vt allpool.

Pärast avaldamist:
- `status: published` `meta.yaml`-is
- `wordpress_url` täidetud

## Skriptide asukoht

```
blogposts/_scripts/
  push_to_doc.py             # md -> Google Doc
  pull_from_doc.py           # Google Doc -> md (best-effort)
  export_notebook.py         # ipynb -> docs/blogposts/<slug>.html
  nbconvert_blog_config.py   # blogi nbconvert config (näitab koodi, erinevalt common/-ist)
```

Skriptid loevad `meta.yaml`-i, et leida vajalikud parameetrid (Doc ID, slug jne). Ühisosa (Drive/Docs API kliendid, autentimine) saab ekstraheerida `_scripts/_common.py`-sse, kui kasvab.

## Mida MITTE teha

- **Ärge muutke `analysis.ipynb`-i lahtreid otse, kui notebook on juba publitseeritud HTML-ina** — kontrolli enne, kas number on jõudnud juba ka `post.md`-i ja Doci. Andmed peavad olema sünkroonis.
- **Ärge kustutage `charts/` PNG-faile**, kui need on juba GitHub raw URL kaudu Doci viidatud — Doci pildid katki.
- **Ärge muutke `meta.yaml`-i `slug`-välja** kui post on juba publitseeritud — link katki.
- **Ärge committige** `post.md` muudatusi otsesse `main`-i kui Doci toimetamine on pooleli — kolleegid ei näe Sinu uut versiooni Doci-s ja võivad „leida konfliktid".

## Viited

- Üldised projekti juhised: repo juur [`CLAUDE.md`](../CLAUDE.md)
- Google Workspace autentimine: vt user memory `feedback_google_workspace_auth.md`
- Tuleva tonaalsus ja stiil: [`common/style-guide/`](../common/style-guide/)
- Eelmine sarnane töövoog (mille põhjal see protsess loodi): `/reports/adhoc/blog-fondivalitsejate-aruanded-2025/` (vana asukoht, liikus siia)
