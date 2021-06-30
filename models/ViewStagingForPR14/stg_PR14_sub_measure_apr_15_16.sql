with source_update as (
    select * from {{ source('generated_sources', 'PR14SubmeasureFinalCSVcreatedbyPython') }}
)
select unique_id
,sub_measure_category
,sub_measure
,'2015-16' year
,'Actual' submission_status
,submeasure_performace_level_2015_16 submeasure_reference_performance_level
,submeasure_high_2015_16 submeasure_high
,submeasure_low_2015_16 submeasure_low
,actual_performance_level_pcs_submeasures_actual_2015_16 performance_level_actual_submeasures
,actual_performance_level_pcs_submeasures_pcl_met_2015_16 pcl_met_submeasures
from source_update
where Unique_ID is not NULL
