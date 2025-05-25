select
  STUDYID,
  USUBJID,
  AESEQ,
  {{ parse_partial_date('AESTDTC') }} as AESTDTC_TS,
  {{ parse_partial_date('AEENDTC') }} as AEENDTC_TS,
  AEDECOD,
  upper(AESEV) as AESEV,
  AESER
from {{ source('raw','AE') }}
