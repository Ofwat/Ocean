      select unique_id
   	  ,'2019-20' year
	  ,'Actual' submission_status
      ,company_type
      ,company
      ,element_acronym
      ,pc_ref
      ,performance_commitment
      ,odi_type
      ,primary_category
      ,pc_unit_description
      ,[starting_level_pr14_fd_2014_15]
      ,[pcl_2019_20] pcl
      ,sub_measure_id
      ,sub_measure
      ,sub_measure_category
      ,sub_measure_weighting
      ,pc_unit
      ,decimal_places
      ,submeasure_performace_level_reference_regulatory_output_during_2010_15
      ,submeasure_performace_level_reference_expected_performance_by_2014_15
      ,[submeasure_performace_level_2019_20] submeasure_performace_level
      ,submeasure_high_reference_regulatory_output_during_2010_15
      ,submeasure_high_reference_expected_performance_by_2014_15
      ,[submeasure_high_2019_20] submeasure_high
      ,submeasure_low_reference_regulatory_output_during_2010_15
      ,submeasure_low_reference_expected_performance_by_2014_15
      ,[submeasure_low_2019_20] submeasure_low
      ,failure_threshold_for_AMP6
      ,[actual_performance_level_pcs_submeasures_actual_2019_20] performance_level_actual
      ,[actual_performance_level_pcs_submeasures_pcl_met_2019_20] performance_level_met
      ,[actual_performance_level_pcs_submeasures_actual_estimate_2019_20]
      ,[actual_performance_level_pcs_submeasures_pcl_met_estimate_2019_20]
      ,direction_of_improving_performance
      ,comms_filter
      ,[actual_performance_compared_with_previous_actual_performance_2014_15_to_2015_16]
      ,[actual_performance_compared_with_previous_actual_performance_2015_16_to_2016_17]
      ,[actual_performance_compared_with_previous_actual_performance_2016_17_to_2017_18]
      ,[actual_performance_compared_with_previous_actual_performance_2017_18_to_2018_19]
      ,[actual_performance_compared_with_previous_actual_performance_2018_19_to_2019_20]
      ,[actual_performance_compared_with_previous_actual_performance_2014_15_to_2016_17_amp_so_far]
      from {{ ref('PR14SubmeasureFinalCSVcreatedbyPythonView') }}
      where Unique_ID is not NULL
