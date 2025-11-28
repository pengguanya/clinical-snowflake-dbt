select
  STUDYID,
  USUBJID,
  CMTRT,
  {{ parse_partial_date('CMSTDTC') }} as CMSTDTC_TS,
  {{ parse_partial_date('CMENDTC') }} as CMENDTC_TS
from {{ source('raw','CM') }}
