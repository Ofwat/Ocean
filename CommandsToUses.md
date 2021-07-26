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
dbt test --models source:generated_sources.pc_actuals_updates


# dbt run --models stg_pc_updates_with_key
dbt snapshot --select pc_updates_snapshot

git statusF_PC_apr_table

dbt run --full-refresh --models D_Element: command to run incremental 


