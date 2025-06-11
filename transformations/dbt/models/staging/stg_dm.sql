select
  STUDYID,
  USUBJID,
  SITEID,
  try_cast(AGE as int) as AGE,
  upper(SEX) as SEX,
  ARM,
  {{ parse_partial_date('RFSTDTC') }} as RFSTDTC_TS,
  BRTHDTC
from {{ source('raw','DM') }}
