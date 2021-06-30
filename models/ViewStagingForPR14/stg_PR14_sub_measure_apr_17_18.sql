with source_update as (
    select * from {{ source('generated_sources', 'PR14SubmeasureFinalCSVcreatedbyPython') }}
)
select unique_id
,sub_measure_category
,sub_measure
,'2017-18' year
,'Actual' submission_status
,submeasure_performace_level_2017_18 submeasure_reference_performance_level
,submeasure_high_2017_18 submeasure_high
,submeasure_low_2017_18 submeasure_low
,actual_performance_level_pcs_submeasures_actual_2017_18 performance_level_actual_submeasures
,actual_performance_level_pcs_submeasures_pcl_met_2017_18 pcl_met_submeasures
from source_update
where Unique_ID is not NULL
