```mermaid
erDiagram
    DM ||--o{ AE : has
    DM {
        string STUDYID
        string USUBJID PK
        string SITEID
        int AGE
        string SEX
        string ARM
        string RFSTDTC
        string BRTHDTC  // may be partial
    }
    AE {
        string STUDYID
        string USUBJID FK
        int AESEQ
        string AESTDTC
        string AEENDTC
        string AEDECOD
        string AESEV
        string AESER
    }
    CM {
        string STUDYID
        string USUBJID FK
        string CMTRT
        string CMSTDTC
        string CMENDTC
    }
```
