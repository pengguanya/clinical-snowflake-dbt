with ae as (select * from {{ ref('fct_ae') }}),
     dm as (select * from {{ ref('dim_subject') }})
select
  dm.STUDYID,
  count(distinct dm.USUBJID) as subjects,
  sum(case when ae.AESER='Y' then 1 else 0 end) as serious_events,
  round(100.0 * sum(case when ae.AESER='Y' then 1 else 0 end) / nullif(count(ae.USUBJID),0), 2) as ae_serious_rate_pct
from dm
left join ae on dm.USUBJID = ae.USUBJID
group by dm.STUDYID
