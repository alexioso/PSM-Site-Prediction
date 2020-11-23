# PSM-Site-Prediction
Feature engineering for modeling selection of sites for pharmaceutical trials. A site is a hospital or clinic that can conduct pharmaceutical studies. A study-site is one of these studies conducted at a specific site. A site can conduct multiple studies at a time. 

Pharmaceutical industry uses machine learning to predict site recruitment rates (PSM = patients per site per month). One important feature is site congestion. This feature is calculated for each site in site-congestion.py


site-congestion: A python algorithm that transforms a list of study-sites with recruitment start and end dates into a list of study-sites and their congestion metrics. One of the most important congestion metrics drv_max_site_study_overlap_trials_count_dqs calculates the maximum number of studies that overlapped with the given study-site at any single time point during the study-site's recruitment period (from site oped date to last subject first dose date).

drv_max_site_overlap_trials_count_dqs is a similar metric that calculates the maximum number of studies that overlapped at a site at any given moment in all of the dataset (not tied to any particular study). 

