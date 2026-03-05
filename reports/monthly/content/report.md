# Tuleva igakuine juhatuse aruanne

**{{ month_name_et | capitalize }} {{ year }}**

*Aruande kuupäev: {{ report_date }}*

---

## 1. Varade maht ja kasv

<!-- comment:aum -->
{{ comments.aum }}
<!-- /comment:aum -->

{% if charts and charts.aum -%}
![AUM]({{ charts.aum }})
{% endif %}

{% if report.aum -%}
| KPI | {{ month_name_et | capitalize }} {{ year }} |
|---------|:---:|
| AUM kuu lõpus | {{ report.aum['kuu lõpu AUM (M EUR)'] }} M EUR |
| AUM 12 kuu kasv | {{ report.aum['AUM 12 kuu kasv %'] }}% |
| sh sissemaksetest ja vahetustest | {{ report.aum['AUM 12 kuu kasv sissemaksetest ja -vahetustest %'] }}% |
{% endif %}

<!-- comment:aum_waterfall -->
{{ comments.aum_waterfall }}
<!-- /comment:aum_waterfall -->

{% if charts and charts.growth_waterfall -%}
![Kasvuallikad]({{ charts.growth_waterfall }})
{% endif %}

---

## 2. Uued kogujad

<!-- comment:savers -->
{{ comments.savers }}
<!-- /comment:savers -->

{% if charts and charts.savers -%}
![Kogujate arv]({{ charts.savers }})
{% endif %}

{% if report.savers -%}
| KPI | {{ month_name_et | capitalize }} {{ year }} |
|---------|:---:|
| Kogujate arv | {{ "{:,}".format(report.savers['kogujate arv']) }} |
| sh ainult II sammas | {{ "{:,}".format(report.savers['ainult II sammas']) }} |
| sh ainult III sammas | {{ "{:,}".format(report.savers['ainult III sammas']) }} |
| sh II ja III sammas | {{ "{:,}".format(report.savers['II ja III sammas']) }} |
| YoY kasv | *{{ "{:.1%}".format(report.savers['YoY, %']) }}* |
{% endif %}

### Uued kogujad

<!-- comment:new_savers -->
{{ comments.new_savers }}
<!-- /comment:new_savers -->

{% if charts and charts.new_savers_pillar -%}
![Uued kogujad samba järgi]({{ charts.new_savers_pillar }})
{% endif %}

{% if report.new_savers -%}
| KPI | {{ month_name_et | capitalize }} {{ year }} | YTD |
|---------|:---:|:---:|
| Uued kogujad | {{ "{:,}".format(report.new_savers['uute koguate arv']) }} | {% if report.new_savers_ytd %}{{ "{:,}".format(report.new_savers_ytd['uute kogujate arv']) }}{% endif %} |
| YoY muutus | *{{ "{:.1%}".format(report.new_savers['YoY, %']) }}* | |
| sh uued II samba kogujad | {% if report.new_savers_ii_month %}{{ "{:,}".format(report.new_savers_ii_month) }}{% endif %} | {% if report.new_savers_ii_ytd %}{{ "{:,}".format(report.new_savers_ii_ytd['uute II samba kogujate arv']) }}{% endif %} |
| sh uued III samba kogujad | {% if report.new_savers_iii_month %}{{ "{:,}".format(report.new_savers_iii_month) }}{% endif %} | {% if report.new_savers_iii_ytd %}{{ "{:,}".format(report.new_savers_iii_ytd['uute III samba kogujate arv']) }}{% endif %} |
{% endif %}

---

## 3. Sissemaksed

<!-- comment:contributions -->
{{ comments.contributions }}
<!-- /comment:contributions -->

{% if charts and charts.contributions -%}
![Sissemaksed]({{ charts.contributions }})
{% endif %}

{% if report.ii_contributions or report.iii_contributions -%}
| KPI | {{ month_name_et | capitalize }} {{ year }} | YoY | YTD | YoY |
|---------|:---:|:---:|:---:|:---:|
{% if report.ii_contributions -%}
| II samba sissemaksed | {{ "{:.1f}".format(report.ii_contributions['II samba sissemaksed, M EUR'] / 1000000) }} M EUR | *{{ "{:.1%}".format(report.ii_contributions['YoY, %']) }}* | {% if report.ii_contributions_ytd %}{{ report.ii_contributions_ytd['second_pillar_contributions_eur'] }} M EUR{% endif %} | {% if report.ii_contributions_ytd_yoy %}*{{ "{:.1%}".format(report.ii_contributions_ytd_yoy) }}*{% endif %} |
{% endif -%}
{% if report.iii_contributions -%}
| III samba sissemaksed | {{ "{:.1f}".format(report.iii_contributions['III samba sissemaksed, M EUR'] / 1000000) }} M EUR | *{{ "{:.1%}".format(report.iii_contributions['YoY, %']) }}* | {% if report.iii_contributions_ytd %}{{ report.iii_contributions_ytd['third_pillar_contributions_eur'] }} M EUR{% endif %} | {% if report.iii_contributions_ytd_yoy %}*{{ "{:.1%}".format(report.iii_contributions_ytd_yoy) }}*{% endif %} |
{% endif -%}
{% if report.contributions_total -%}
| **Sissemaksed kokku** | **{{ "{:.1f}".format(report.contributions_total['month']) }} M EUR** | ***{{ "{:.1%}".format(report.contributions_total['month_yoy']) }}*** | **{{ "{:.1f}".format(report.contributions_total['ytd']) }} M EUR** | ***{{ "{:.1%}".format(report.contributions_total['ytd_yoy']) }}*** |
{% endif -%}
{% endif %}

<!-- comment:iii_contributions -->
{{ comments.iii_contributions }}
<!-- /comment:iii_contributions -->

{% if charts and charts.iii_contributors -%}
![III samba sissemakse tegijad]({{ charts.iii_contributors }})
{% endif %}

{% if report.iii_contributors -%}
### III samba sissemakse tegijad

| KPI | {{ month_name_et | capitalize }} {{ year }} | YoY | YTD |
|---------|:---:|:---:|:---:|
| Sissemakse tegijate arv | {{ "{:,}".format(report.iii_contributors['III samba sissemakse tegijate arv']) }} | *{{ "{:.1%}".format(report.iii_contributors['YoY, %']) }}* | {% if report.iii_contributors_ytd %}{{ "{:,}".format(report.iii_contributors_ytd) }}{% endif %} |
| Püsimakse tegijate osakaal | {{ "{:.1%}".format(report.iii_contributors['püsimakse tegijate osakaal, %']) }} | | |
{% endif %}

{% if report.rate_changes -%}
### II samba maksemäära muutmine

| KPI | {{ month_name_et | capitalize }} {{ year }} | YoY | YTD | YoY |
|---------|:---:|:---:|:---:|:---:|
| Maksemäära tõstnud | {{ "{:,}".format(report.rate_changes['maksemäära tõstnute arv']) }} | {% if report.rate_changes_prev %}*{{ "{:.1%}".format((report.rate_changes['maksemäära tõstnute arv'] - report.rate_changes_prev['maksemäära tõstnute arv']) / report.rate_changes_prev['maksemäära tõstnute arv']) }}*{% endif %} | {% if report.rate_changes_ytd %}{{ "{:,}".format(report.rate_changes_ytd['raised']) }}{% endif %} | {% if report.rate_changes_ytd_prev %}*{{ "{:.1%}".format((report.rate_changes_ytd['raised'] - report.rate_changes_ytd_prev['raised']) / report.rate_changes_ytd_prev['raised']) }}*{% endif %} |
| Maksemäära langetanud | {{ "{:,}".format(report.rate_changes['maksemäära langetanute arv']) }} | {% if report.rate_changes_prev %}*{{ "{:.1%}".format((report.rate_changes['maksemäära langetanute arv'] - report.rate_changes_prev['maksemäära langetanute arv']) / report.rate_changes_prev['maksemäära langetanute arv']) }}*{% endif %} | {% if report.rate_changes_ytd %}{{ "{:,}".format(report.rate_changes_ytd['lowered']) }}{% endif %} | {% if report.rate_changes_ytd_prev %}*{{ "{:.1%}".format((report.rate_changes_ytd['lowered'] - report.rate_changes_ytd_prev['lowered']) / report.rate_changes_ytd_prev['lowered']) }}*{% endif %} |
{% endif %}

{% if report.tkf_contributions -%}
### Täiendavasse Kogumisfondi tehtud maksed

| KPI | {{ month_name_et | capitalize }} {{ year }} | YTD |
|---------|:---:|:---:|
| Sissemaksete summa | {{ "{:.1f}".format(report.tkf_contributions['amount'] / 1000000) }} M EUR | {{ "{:.1f}".format(report.tkf_contributions['ytd_amount'] / 1000000) }} M EUR |
| Sissemakse tegijate arv | {{ "{:,}".format(report.tkf_contributions['contributors']) }} | |
{% endif %}

---

## 4. Fondivahetused

<!-- comment:switching -->
{{ comments.switching }}
<!-- /comment:switching -->

{% if charts and charts.switching_volume -%}
![Vahetuste maht]({{ charts.switching_volume }})
{% endif %}

{% if report.switchers or report.switchers_aum -%}
| KPI | {{ month_name_et | capitalize }} {{ year }} | YoY | YTD | YoY |
|---------|:---:|:---:|:---:|:---:|
{% if report.switchers -%}
| Sissevahetajate arv | {{ "{:,}".format(report.switchers['vahetajate arv']) }} | *{{ "{:.1%}".format(report.switchers['YoY, %']) }}* | {% if report.switchers_ytd %}{{ "{:,}".format(report.switchers_ytd['IIs sissevahetajate arv']) }}{% endif %} | {% if report.switchers_ytd_yoy %}*{{ "{:.1%}".format(report.switchers_ytd_yoy) }}*{% endif %} |
{% endif -%}
{% if report.switchers_aum -%}
| Ületoodud vara | {{ "{:.1f}".format(report.switchers_aum['vahetajate ületoodud varade maht, M EUR'] / 1000000) }} M EUR | *{{ "{:.1%}".format(report.switchers_aum['YoY, %']) }}* | {% if report.switchers_aum_ytd %}{{ report.switchers_aum_ytd['IIs vahetustega ületoodav vara M EUR'] }} M EUR{% endif %} | {% if report.switchers_aum_ytd_yoy %}*{{ "{:.1%}".format(report.switchers_aum_ytd_yoy) }}*{% endif %} |
{% endif -%}
{% endif %}

<!-- comment:switching_conversion -->
{{ comments.switching_conversion }}
<!-- /comment:switching_conversion -->

{% if charts and charts.new_ii_savers_source -%}
![II sambaga liitujad allika järgi]({{ charts.new_ii_savers_source }})
{% endif %}

<!-- comment:switching_sources -->
{{ comments.switching_sources }}
<!-- /comment:switching_sources -->

{% if charts and charts.switching_sources -%}
![Millistest fondidest vahetatakse Tulevasse]({{ charts.switching_sources }})
{% endif %}

---

## 5. Väljavoolud

<!-- comment:outflows -->
{{ comments.outflows }}
<!-- /comment:outflows -->

{% if charts and charts.leavers -%}
![II samba lahkujate vara]({{ charts.leavers }})
{% endif %}

<!-- comment:drawdowns -->
{{ comments.drawdowns }}
<!-- /comment:drawdowns -->

{% if charts and charts.drawdowns -%}
![Pensionifondidest väljavõetud vara]({{ charts.drawdowns }})
{% endif %}

{% if report.ii_leavers or report.ii_exiters or report.iii_withdrawals -%}
| KPI | {{ month_name_et | capitalize }} {{ year }} | YoY | YTD |
|---------|:---:|:---:|:---:|
{% if report.ii_leavers -%}
| II samba lahkujate vara | {{ "{:.1f}".format(report.ii_leavers['lahkujate varade maht, M EUR'] / 1000000) }} M EUR | *{{ "{:.1%}".format(report.ii_leavers['YoY, %']) }}* | {% if report.ii_leavers_ytd %}{{ report.ii_leavers_ytd['new_monthly_leavers_eur'] }} M EUR{% endif %} |
{% endif -%}
{% if report.ii_exiters -%}
| II samba väljujate vara | {{ "{:.1f}".format(report.ii_exiters['väljujate varade maht, M EUR'] / 1000000) }} M EUR | *{{ "{:.1%}".format(report.ii_exiters['YoY, %']) }}* | {% if report.ii_exiters_ytd %}{{ report.ii_exiters_ytd['new_monthly_exiters_eur'] }} M EUR{% endif %} |
{% endif -%}
{% if report.iii_withdrawals -%}
| III sambast väljavõetud vara | {{ "{:.1f}".format(report.iii_withdrawals['III sambast väljavõetud varade maht, M EUR'] / 1000000) }} M EUR | *{{ "{:.1%}".format(report.iii_withdrawals['YoY, %']) }}* | {% if report.iii_withdrawals_ytd %}{{ report.iii_withdrawals_ytd['new_monthly_withdrawals_third_pillar_eur'] }} M EUR{% endif %} |
{% endif -%}
{% endif %}

---

## 6. Osakuhinna muutus

<!-- comment:unit_price -->
{{ comments.unit_price }}
<!-- /comment:unit_price -->

{% if charts and charts.unit_price -%}
![Osakuhinna võrdlus]({{ charts.unit_price }})
{% endif %}

<!-- comment:cumulative_returns -->
{{ comments.cumulative_returns }}
<!-- /comment:cumulative_returns -->

{% if charts and charts.cumulative_returns -%}
![Kumulatiivne tootlus]({{ charts.cumulative_returns }})
{% endif %}

---

{% if report.financials -%}
## 7. Tuleva finantstulemused

<!-- comment:financials -->
{{ comments.financials }}
<!-- /comment:financials -->

| KPI | {{ month_name_et | capitalize }} {{ year }} | YoY |
|---------|:---:|:---:|
{% for row in report.financials -%}
{% if row['Eur'] == 'litsentsitasu' -%}
| **Litsentsitasu ühistule** | **{{ "{:,.0f}".format(row['Kuu Tulemus'] | abs) }} EUR** | ***{{ "{:.0%}".format(row['YoY %']) }}*** |
{% else -%}
| {{ row['Eur'] | capitalize }} | {{ "{:,.0f}".format(row['Kuu Tulemus']) }} EUR | *{{ "{:.0%}".format(row['YoY %']) }}* |
{% endif -%}
{% endfor %}
{% endif %}

---

*Aruanne genereeritud [Tuleva Reporting Engine](https://github.com/TulevaEE/reporting-engine)'iga*
