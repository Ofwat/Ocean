1. dbt run: so we get basic tables
2. dbt snapshot: so we get snapshot
3. dbt run --full-refresh --models D_Element: to get changes
    make changes in main table then run incremental / we should be able to see the hanges in the element table
4. dbt snapshot

