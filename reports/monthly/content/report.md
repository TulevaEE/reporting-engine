# Tuleva igakuine juhatuse aruanne

**{{ month_name }} {{ year }}**

*Aruande kuupäev: {{ report_date }}*

---

## 1. Varade maht (AUM)

[Kommentaar]

{% if charts and charts.aum -%}
![AUM]({{ charts.aum }})
{% endif %}

{% if report.aum -%}
| Näitaja | Väärtus |
|---------|---------|
| AUM kuu lõpus | {{ report.aum['kuu lõpu AUM (M EUR)'] }} M EUR |
| AUM 12 kuu kasv | {{ report.aum['AUM 12 kuu kasv %'] }}% |
| sh sissemaksetest ja vahetustest | {{ report.aum['AUM 12 kuu kasv sissemaksetest ja -vahetustest %'] }}% |
{% endif %}

{% if charts and charts.growth_month -%}
![Kasvuallikad kuus]({{ charts.growth_month }})
{% endif %}

{% if report.growth_actual -%}
### Kasvuallikad (kuu tegelik), M EUR

| Kasvuallikas | M EUR |
|--------------|-------|
{% for row in report.growth_actual -%}
| {{ row['kasvuallikas'] }} | {{ row['väärtus'] }} |
{% endfor %}
{% endif %}

{% if charts and charts.growth_ytd -%}
![Kasvuallikad aasta algusest]({{ charts.growth_ytd }})
{% endif %}

{% if report.growth_ytd -%}
### Kasvuallikad YTD (tegelik), M EUR

| Kasvuallikas | M EUR |
|--------------|-------|
{% for row in report.growth_ytd -%}
| {{ row['kasvuallikas'] }} | {{ row['väärtus'] }} |
{% endfor %}
{% endif %}

---

## 2. Kogujad

[Kommentaar]

{% if charts and charts.savers -%}
![Kogujate arv]({{ charts.savers }})
{% endif %}

{% if report.savers -%}
| Näitaja | Väärtus |
|---------|---------|
| Kogujate arv | {{ "{:,}".format(report.savers['kogujate arv']) }} |
| sh ainult II sammas | {{ "{:,}".format(report.savers['ainult II sammas']) }} |
| sh ainult III sammas | {{ "{:,}".format(report.savers['ainult III sammas']) }} |
| sh II ja III sammas | {{ "{:,}".format(report.savers['II ja III sammas']) }} |
| YoY kasv | {{ "{:.1%}".format(report.savers['YoY, %']) }} |
{% endif %}

### Uued kogujad

{% if charts and charts.new_savers_pillar -%}
![Uued kogujad samba järgi]({{ charts.new_savers_pillar }})
{% endif %}

{% if charts and charts.new_ii_savers_source -%}
![II sambaga liitujad allika järgi]({{ charts.new_ii_savers_source }})
{% endif %}

{% if report.new_savers -%}
| Näitaja | Väärtus |
|---------|---------|
| Uued kogujad (kuu) | {{ "{:,}".format(report.new_savers['uute koguate arv']) }} |
| YoY muutus | {{ "{:.1%}".format(report.new_savers['YoY, %']) }} |
{% if report.new_savers_ytd -%}
| Uued kogujad YTD | {{ "{:,}".format(report.new_savers_ytd['uute kogujate arv']) }} |
{% endif -%}
{% if report.new_savers_ii_ytd -%}
| sh uued II samba kogujad YTD | {{ "{:,}".format(report.new_savers_ii_ytd['uute II samba kogujate arv']) }} |
{% endif -%}
{% if report.new_savers_iii_ytd -%}
| sh uued III samba kogujad YTD | {{ "{:,}".format(report.new_savers_iii_ytd['uute III samba kogujate arv']) }} |
{% endif -%}
{% endif %}

---

## 3. Sissemaksed

{% if report.ii_contributions or report.iii_contributions -%}
| Näitaja | Kuu | YoY | YTD |
|---------|-----|-----|-----|
{% if report.ii_contributions -%}
| II samba sissemaksed | {{ "{:,.0f}".format(report.ii_contributions['II samba sissemaksed, M EUR']) }} EUR | {{ "{:.1%}".format(report.ii_contributions['YoY, %']) }} | {% if report.ii_contributions_ytd %}{{ report.ii_contributions_ytd['second_pillar_contributions_eur'] }} M EUR{% endif %} |
{% endif -%}
{% if report.iii_contributions -%}
| III samba sissemaksed | {{ "{:,.0f}".format(report.iii_contributions['III samba sissemaksed, M EUR']) }} EUR | {{ "{:.1%}".format(report.iii_contributions['YoY, %']) }} | {% if report.iii_contributions_ytd %}{{ report.iii_contributions_ytd['third_pillar_contributions_eur'] }} M EUR{% endif %} |
{% endif -%}
{% endif %}

{% if report.iii_contributors -%}
### III samba sissemakse tegijad

| Näitaja | Väärtus |
|---------|---------|
| Sissemakse tegijate arv | {{ "{:,}".format(report.iii_contributors['III samba sissemakse tegijate arv']) }} |
| YoY kasv | {{ "{:.1%}".format(report.iii_contributors['YoY, %']) }} |
| Püsimakse tegijate osakaal | {{ "{:.1%}".format(report.iii_contributors['püsimakse tegijate osakaal, %']) }} |
{% endif %}

{% if report.rate_changes -%}
### II samba maksemäära muutmine

| Näitaja | Väärtus |
|---------|---------|
| Maksemäära tõstnud | {{ "{:,}".format(report.rate_changes['maksemäära tõstnute arv']) }} |
| Maksemäära langetanud | {{ "{:,}".format(report.rate_changes['maksemäära langetanute arv']) }} |
{% endif %}

---

## 4. Fondivahetused

{% if report.switchers or report.switchers_aum -%}
| Näitaja | Kuu | YoY | YTD |
|---------|-----|-----|-----|
{% if report.switchers -%}
| Sissevahetajate arv | {{ "{:,}".format(report.switchers['vahetajate arv']) }} | {{ "{:.1%}".format(report.switchers['YoY, %']) }} | {% if report.switchers_ytd %}{{ "{:,}".format(report.switchers_ytd['IIs sissevahetajate arv']) }}{% endif %} |
{% endif -%}
{% if report.switchers_aum -%}
| Ületoodud vara | {{ "{:,.0f}".format(report.switchers_aum['vahetajate ületoodud varade maht, M EUR']) }} EUR | {{ "{:.1%}".format(report.switchers_aum['YoY, %']) }} | {% if report.switchers_aum_ytd %}{{ report.switchers_aum_ytd['IIs vahetustega ületoodav vara M EUR'] }} M EUR{% endif %} |
{% endif -%}
{% endif %}

{% if report.switching_from -%}
### Millistest fondidest vahetatakse Tulevasse (top 10)

| Lähtefond | Avalduste arv |
|-----------|---------------|
{% for row in report.switching_from -%}
| {{ row['Fund - Security From → Name Estonian'] }} | {{ row['Distinct values of Code'] }} |
{% endfor %}
{% endif %}

{% if report.switching_to -%}
### Kuhu vahetatakse Tulevast välja (top 10)

| Sihtfond | Avalduste arv |
|----------|---------------|
{% for row in report.switching_to -%}
| {{ row['Fund - Security To → Name Estonian'] }} | {{ row['Distinct values of Code'] }} |
{% endfor %}
{% endif %}

---

## 5. Väljavoolud

{% if report.ii_leavers or report.ii_exiters or report.iii_withdrawals -%}
| Näitaja | Kuu | YoY | YTD |
|---------|-----|-----|-----|
{% if report.ii_leavers -%}
| II samba lahkujate vara | {{ "{:,.0f}".format(report.ii_leavers['lahkujate varade maht, M EUR']) }} EUR | {{ "{:.1%}".format(report.ii_leavers['YoY, %']) }} | {% if report.ii_leavers_ytd %}{{ report.ii_leavers_ytd['new_monthly_leavers_eur'] }} M EUR{% endif %} |
{% endif -%}
{% if report.ii_exiters -%}
| II samba väljujate vara | {{ "{:,.0f}".format(report.ii_exiters['väljujate varade maht, M EUR']) }} EUR | {{ "{:.1%}".format(report.ii_exiters['YoY, %']) }} | {% if report.ii_exiters_ytd %}{{ report.ii_exiters_ytd['new_monthly_exiters_eur'] }} M EUR{% endif %} |
{% endif -%}
{% if report.iii_withdrawals -%}
| III sambast väljavõetud vara | {{ "{:,.0f}".format(report.iii_withdrawals['III sambast väljavõetud varade maht, M EUR']) }} EUR | {{ "{:.1%}".format(report.iii_withdrawals['YoY, %']) }} | {% if report.iii_withdrawals_ytd %}{{ report.iii_withdrawals_ytd['new_monthly_withdrawals_third_pillar_eur'] }} M EUR{% endif %} |
{% endif -%}
{% endif %}

---

{% if report.growth_forecast -%}
## 6. Aasta lõpu prognoos, M EUR

| Kasvuallikas | M EUR |
|--------------|-------|
{% for row in report.growth_forecast -%}
| {{ row['kasvuallikas'] }} | {{ row['väärtus'] }} |
{% endfor %}
{% endif %}

---

*Aruanne genereeritud Tuleva Reporting Engine'iga*
