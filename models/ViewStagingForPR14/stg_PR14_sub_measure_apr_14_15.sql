with source_update as (
    select * from {{ source('generated_sources', 'PR14SubmeasureFinalCSVcreatedbyPython') }}
)
select unique_id
,sub_measure_id
,sub_measure_category
,sub_measure
,'2014-15' year
,'Actual' submission_status
,submeasure_performace_level_reference_expected_performance_by_2014_15 as submeasure_reference_performance_level
,submeasure_high_reference_expected_performance_by_2014_15  as submeasure_high
,submeasure_low_reference_expected_performance_by_2014_15 as submeasure_low
,CAST(NULL as varchar(max)) as performance_level_actual_submeasures
,CAST(NULL as varchar(max)) as pcl_met_submeasures
from source_update
where unique_id is not NULL
