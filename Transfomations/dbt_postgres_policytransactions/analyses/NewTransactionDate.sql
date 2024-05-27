/*Pretending it's an incremental load based on a full export*/
SELECT min(transactiondate) transactiondate 
FROM {{ ref('orchestration_history') }}
WHERE transactiondate>{{ var('latest_loaded_transactiondate' ) }}