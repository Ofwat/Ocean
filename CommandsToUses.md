##Commands
dbt init [project name]

dbt debug: to check the sql connection

dbt run: run project

dbt docs generate: to generate the data catalogue

dbt docs serve: to get the localhost web page ready

dbt seed: to upload the csv

dbt deps: once we add the package in packages.yml

##Specific commands
To run tests on sources:
dbt test --models source:*

To test a specific source:
dbt test --models source:ae_staging.source_pc_actuals_updates

dbt run --models stg_pc_updates
dbt snapshot --select pc_updates_snapshot
dbt run --models stg_F_pc_apr_updated
dbt run --models F_PC_apr


