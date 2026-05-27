# Tuleva blogipostid

See kataloog sisaldab Tuleva blogiposte mustandist publitseerimiseni. Iga post on oma alamkaustas ja koosneb kahest paralleelsest tekstist:

- **`post.md`** — lühem, üldlugejale mõeldud blogipost (avaldatakse WordPressis).
- **`analysis.ipynb`** — pikem, „andmenohikule" mõeldud analüüs koos koodi, allikate ja arvutustega. Avaldatakse staatilise HTML lehena `docs/blogposts/` kaudu (GitHub Pages) ning ka raw notebookina samas repos.

Mõlemad versioonid viitavad samadele graafikutele (`charts/`), mille toodab notebook. Andmed (CSV jm) on `data/` kaustas.

## Praegused postid

| Slug | Pealkiri | Staatus | Doc |
|---|---|---|---|
| [2026-05-fondivalitsejate-aruanded](2026-05-fondivalitsejate-aruanded/) | Mida räägivad meile fondivalitsejate 2025. aasta aruanded? | draft | [Google Doc](https://docs.google.com/document/d/1dMuprG_rN64WtTxGJLq3JbG_wwhgSaokShJdW0wzV2g/edit) |

## Faili struktuur

```
blogposts/
  README.md                              # see fail — inimlugejale
  CLAUDE.md                              # protsessi juhend Claude'ile (push/pull, export jne)
  _scripts/                              # taaskasutatavad tööriistad
    push_to_doc.py                       # md -> Google Doc
    pull_from_doc.py                     # Google Doc -> md (best-effort, --apply või vaikimisi .from-doc)
    export_notebook.py                   # ipynb -> docs/blogposts/<slug>.html
    nbconvert_blog_config.py             # blogi-spetsiifiline nbconvert config (näitab koodi)
  <YYYY-MM-slug>/
    task-<nimi>.md                       # algne ülesanne ja eesmärk (gitignored, sisemine)
    post.md                              # WordPressi minev tekst
    analysis.ipynb                       # iseseisev pikem analüüs
    meta.yaml                            # title, status, doc_id, ...
    charts/                              # PNG/SVG (notebook toodab, mõlemad viitavad)
    data/                                # vahedataid (CSV)
```

**Märkus:** `task-*.md` failid (ja `task-*.txt`) on `.gitignore`-is — need on sisemised planeerimismärkmed, mis ei satu publikatsiooni. Mitu task-faili posti kohta on lubatud, kui ülesanne on mitme etapilise.

## Töövoog (lühidalt)

0. **Ülesande püstitamine** — loon kataloogi `blogposts/<YYYY-MM-slug>/`, kirjutan `task-<nimi>.md`-i algse ülesande, eesmärgi, sihtgrupi, andmeallikate kirjeldusega. See fail on gitignored.
1. **Analüüsi tegemine** — loon `analysis.ipynb`-i. Graafikud salvestatakse `charts/` kausta PNG-failidena.
2. **Blogiposti tekst** — kirjutan `post.md`-i (lühem versioon), viidates samadele graafikutele.
3. **Mustand kolleegidele** — looed Google Doci Tuleva Drive'is, jagad SA-ga (`read-write@tuleva-claude.iam.gserviceaccount.com`) Editor-õigustega, paned `doc_id` `meta.yaml`-i. Käsk `push_to_doc.py <slug>` viib `post.md` sisu Doci koos vormistuse ja graafikutega.
4. **Toimetamine** — sina ja kolleegid toimetate Doci. Kui vahepeal teen analüüsis muudatusi, uuendan ka `post.md` ja vajadusel Doci.
5. **Pull-back** — kui toimetamine läbi, käsk `pull_from_doc.py <slug>` tõmbab Doci sisu tagasi `post.md`-i. Vajadusel uuendan ka notebook'i.
6. **Publitseerimine** — `export_notebook.py <slug>` ekspordib notebook'i HTML-iks `docs/blogposts/`-i. `post.md` läheb WordPressi (käsitsi praegu, automaatne hiljem).

Detailne juhend Claude'ile: vt [`CLAUDE.md`](CLAUDE.md).
