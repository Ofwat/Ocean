with source_update as (
    select * from {{ source('nw', 'PR14SubmeasureFinalCSVcreatedbyPython') }}
)
select unique_id
,sub_measure_category
,sub_measure
,'2018-19' year
,'Actual' submission_status
,submeasure_performace_level_2018_19 submeasure_reference_performance_level
,submeasure_high_2018_19 submeasure_high
,submeasure_low_2018_19 submeasure_low
,actual_performance_level_pcs_submeasures_actual_2018_19 performance_level_actual_submeasures
,actual_performance_level_pcs_submeasures_pcl_met_2018_19 pcl_met_submeasures
from source_update
where Unique_ID is not NULL
