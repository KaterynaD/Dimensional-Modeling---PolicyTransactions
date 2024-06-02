SELECT transactiondate FROM {{ ref('orchestration_log') }} 
WHERE LoadDate=(select max(t.loaddate) from {{ ref('orchestration_log') }} t where t.endloaddate is not null)