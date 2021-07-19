with source_update as (
    select * from {{ source('generated_sources', 'PR14BaseSubmeasureCSVcreatedbyPython') }}
)
select unique_id
,sub_measure_id
,sub_measure_category
,sub_measure
,'2016-17' year
,'Actual' submission_status
,submeasure_performace_level_2016_17 submeasure_reference_performance_level
,submeasure_high_2016_17 submeasure_high
,submeasure_low_2016_17 submeasure_low
,actual_performance_level_pcs_submeasures_actual_2016_17 performance_level_actual_submeasures
,actual_performance_level_pcs_submeasures_pcl_met_2016_17 pcl_met_submeasures
from source_update
where Unique_ID is not NULL
