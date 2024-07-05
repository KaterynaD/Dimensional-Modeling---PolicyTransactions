The recommended way to create a Slowly Changing Dimension Type 2 in DBT is a snapshot model. There are 2 types of snapshot models: timestamp and check strategies. 
Neither of them can create a proper SCD2 table from **historical data and incremental incoming staging records not in order**.

Sample incoming staging data:

![image](https://github.com/KaterynaD/Dimensional-Modeling---PolicyTransactions/assets/16999229/820a1ef8-1e1b-444f-a0aa-cfa945d9e307)

DBT JINJA functionality allows to build a generalized process (DRY) to be used for any SCD2 table. It’s more flexible than a database stored procedure. 
The alternate approach is only full refresh with Snowflake Dynamic Tables or Redshift Materialized Views which might be not fast enough with 20 years of historical data and 200+ columns to track history of changes.

DBT macro was developed to populate any SCD2 table based on parameters:

- dim_table_name: name of the dimensional table
- stg_data: view or table with staging data for the table configuraed in dim_table_name
- dim_primary_key: primary key column name 
- dim_unique_key: natural unique key column name
- dim_change_date: change date column name
- history_tracking_columns: list of columns where history should be tracked (check_cols)

It's possible to have business-info columns which should be just updated as in SCD1 but more arguments and updates is needed in the code. 
The other simplification in the code is assuming all history tracking columns are varchar.
**Improvment: add hash column to track changes of all columns easily.**

Macro Steps:
1.	Create a temporary table with new data to insert based on 
a.	Incremental batch  in stg_data
b.	Existing, historical data in dim_table_name with the same dim_unique_key as in the stg_data  incremental batch. 
c.	The data are ordered by change date and valid_fromdate and latest value is compared to the previous using lag function
d.	Only records with changes are selected at the last step
2.	New records are inserted in dim_table_name
3.	Valid_Fromdate – Valid_Todate are adjusted in the existing records



