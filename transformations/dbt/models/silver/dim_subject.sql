select
  USUBJID,
  any_value(STUDYID) as STUDYID,
  any_value(SITEID) as SITEID,
  any_value(AGE) as AGE,
  any_value(SEX) as SEX,
  any_value(ARM) as ARM
from {{ ref('stg_dm') }}
group by USUBJID
