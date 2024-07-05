Airflow + DBT + Snowflake: fact table and dimensional tables including SCD2 with load historical data and incremental updates out of order records using [dbt macro](https://github.com/KaterynaD/Dimensional-Modeling---PolicyTransactions/tree/master/Transfomations/dbt_postgres_policytransactions/macros/scd2)  because out of the box DBT snapshots aproach can not load out of order data properly. There are different approaches for creating Primary Key in dimensional tables: hash value of natural unique key, sequence, next max value of PK in this table (training and dbt testing).

DBT tests, orchestration log tables (dbt model), tables loads log (dbt pre-hook) and load complete emails with DW tables status. 
Airflow DAG is dynamically built based on dbt graph.

Not included: moving data from a source system to staging tables.

Orchestration is done using Airflow branch python operators and variables.

 # Pipeline Steps

 1. Load Start: Load date is set, orchestration logging starts, DB connection syncs between DBT and Airflow.
 2. Conditional step: Re-Creating DW tables (dbt full-refresh mode, seeds load). Default values can be loaded in tables at this step.
 3. Defining Incremental Load range based on a previous load (Airflow hook + dbt analytic query)
 4. Conditional step: Validating Staging data if they are ready/present to be loaded into DW (Airflow sensor + dbt analytic query)
 5. Conditional step: Some transformations in the staging area (run dbt models)
 6. Conditional step: Load dimensions (run dbt models and macro for SCD2)
 7. Conditional step: Load transactional fact tables (run dbt models)
 8. Conditional step: Load summaries (based on transactional fact tables) fact tables (run dbt models)
 9. Finalizing load, closing orchestration log (run dbt models), email notification (Airflow hook + dbt analytic query)
 10. Conditional step: Testing loaded data (dbt test)
 11. Conditional step: Refresh dashboards

![Lineage Graph](https://github.com/KaterynaD/Dimensional-Modeling---PolicyTransactions/assets/16999229/47d814ce-13fe-45ca-b2e6-6c1f06b02030)

![image](https://github.com/KaterynaD/Dimensional-Modeling---PolicyTransactions/assets/16999229/4f7d769e-afb0-42a1-8e78-46c3159a7788)

![image](https://github.com/KaterynaD/Dimensional-Modeling---PolicyTransactions/assets/16999229/e572578f-653c-4517-b7fa-66cd7bfdc619)


![image](https://github.com/KaterynaD/Dimensional-Modeling---PolicyTransactions/assets/16999229/5a74cb5d-09e8-4ccf-a1e7-a1c366eabe9f)

![image](https://github.com/KaterynaD/Dimensional-Modeling---PolicyTransactions/assets/16999229/f0bfb9da-cf44-479e-9bf0-06cab5d9c1ec)

![image](https://github.com/KaterynaD/Dimensional-Modeling---PolicyTransactions/assets/16999229/332d58cb-2abb-4a64-9643-f1eb529fc6c7)

![image](https://github.com/KaterynaD/Dimensional-Modeling---PolicyTransactions/assets/16999229/369529a4-ae37-435e-b703-86af26f9ba44)


![image](https://github.com/KaterynaD/Dimensional-Modeling---PolicyTransactions/assets/16999229/c2004af7-7d69-460b-8b8f-4894e8d722bc)
