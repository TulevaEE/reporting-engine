# Tuleva igakuine juhatuse aruanne

**{{ month_name_et | capitalize }} {{ year }}**

*Aruande kuupäev: {{ report_date }}*

---

## 1. Varade maht ja kasv

{{ comments.aum }}

{% if charts and charts.aum -%}
![AUM]({{ charts.aum }})
{% endif %}

{% if report.aum -%}
| KPI | {{ month_name_et | capitalize }} {{ year }} |
|---------|---------|
| AUM kuu lõpus | {{ report.aum['kuu lõpu AUM (M EUR)'] }} M EUR |
| AUM 12 kuu kasv | {{ report.aum['AUM 12 kuu kasv %'] }}% |
| sh sissemaksetest ja vahetustest | {{ report.aum['AUM 12 kuu kasv sissemaksetest ja -vahetustest %'] }}% |
{% endif %}

{{ comments.aum_waterfall }}

{% if charts and charts.growth_month -%}
![Kasvuallikad kuus]({{ charts.growth_month }})
{% endif %}

{% if charts and charts.growth_ytd -%}
![Kasvuallikad aasta algusest]({{ charts.growth_ytd }})
{% endif %}

---

## 2. Uued kogujad

{{ comments.savers }}

{% if charts and charts.savers -%}
![Kogujate arv]({{ charts.savers }})
{% endif %}

{% if report.savers -%}
| KPI | {{ month_name_et | capitalize }} {{ year }} |
|---------|---------|
| Kogujate arv | {{ "{:,}".format(report.savers['kogujate arv']) }} |
| sh ainult II sammas | {{ "{:,}".format(report.savers['ainult II sammas']) }} |
| sh ainult III sammas | {{ "{:,}".format(report.savers['ainult III sammas']) }} |
| sh II ja III sammas | {{ "{:,}".format(report.savers['II ja III sammas']) }} |
| YoY kasv | {{ "{:.1%}".format(report.savers['YoY, %']) }} |
{% endif %}

### Uued kogujad

{{ comments.new_savers }}

{% if charts and charts.new_savers_pillar -%}
![Uued kogujad samba järgi]({{ charts.new_savers_pillar }})
{% endif %}

{% if report.new_savers -%}
| KPI | {{ month_name_et | capitalize }} {{ year }} |
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

{{ comments.contributions }}

{% if charts and charts.contributions -%}
![Sissemaksed]({{ charts.contributions }})
{% endif %}

{% if report.ii_contributions or report.iii_contributions -%}
| KPI | {{ month_name_et | capitalize }} {{ year }} |
|---------|---------|
{% if report.ii_contributions -%}
| II samba sissemaksed | {{ "{:,.0f}".format(report.ii_contributions['II samba sissemaksed, M EUR']) }} EUR |
{% endif -%}
{% if report.iii_contributions -%}
| III samba sissemaksed | {{ "{:,.0f}".format(report.iii_contributions['III samba sissemaksed, M EUR']) }} EUR |
{% endif -%}
{% endif %}

{{ comments.iii_contributions }}

{% if charts and charts.iii_contributors -%}
![III samba sissemakse tegijad]({{ charts.iii_contributors }})
{% endif %}

{% if report.iii_contributors -%}
### III samba sissemakse tegijad

| KPI | {{ month_name_et | capitalize }} {{ year }} |
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

{{ comments.switching }}

{% if charts and charts.switching_volume -%}
![Vahetuste maht]({{ charts.switching_volume }})
{% endif %}

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

{{ comments.switching_conversion }}

{% if charts and charts.new_ii_savers_source -%}
![II sambaga liitujad allika järgi]({{ charts.new_ii_savers_source }})
{% endif %}

{{ comments.switching_sources }}

{% if charts and charts.switching_sources -%}
![Millistest fondidest vahetatakse Tulevasse]({{ charts.switching_sources }})
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

{{ comments.outflows }}

{% if charts and charts.leavers -%}
![II samba lahkujate vara]({{ charts.leavers }})
{% endif %}

{{ comments.drawdowns }}

{% if charts and charts.drawdowns -%}
![Pensionifondidest väljavõetud vara]({{ charts.drawdowns }})
{% endif %}

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

## 6. Osakuhinna muutus

{{ comments.unit_price }}

{% if charts and charts.unit_price -%}
![Osakuhinna võrdlus]({{ charts.unit_price }})
{% endif %}

{{ comments.cumulative_returns }}

{% if charts and charts.cumulative_returns -%}
![Kumulatiivne tootlus]({{ charts.cumulative_returns }})
{% endif %}

---

{% if report.financials -%}
## 7. Tuleva finantstulemused

{{ comments.financials }}

| Näitaja | Kuu tulemus | YoY |
|---------|------------|-----|
{% for row in report.financials -%}
{% if row['Eur'] == 'litsentsitasu' -%}
| **Litsentsitasu ühistule** | **{{ "{:,.0f}".format(row['Kuu Tulemus'] | abs) }} EUR** | **{{ "{:.0%}".format(row['YoY %']) }}** |
{% else -%}
| {{ row['Eur'] | capitalize }} | {{ "{:,.0f}".format(row['Kuu Tulemus']) }} EUR | {{ "{:.0%}".format(row['YoY %']) }} |
{% endif -%}
{% endfor %}
{% endif %}

---

*Aruanne genereeritud Tuleva Reporting Engine'iga*
