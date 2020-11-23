# PSM-Site-Prediction
Feature engineering for modeling selection of sites for pharmaceutical trials. A site is a hospital or clinic that can conduct pharmaceutical studies. A study-site is one of these studies conducted at a specific site. A site can conduct multiple studies at a time. 

Pharmaceutical industry uses machine learning to predict site recruitment rates (PSM = patients per site per month). One important feature is site congestion. This feature is calculated for each site in site-congestion.py


## site-congestion

### event_competition_calculate()

The site_congestion.py file includes an algorithm in the function event_competition_calculate that computes the number of months at which the study site experienced c overlapping studies for c in {0, 1, ..., num_cap_overlap}, adding two columns in the output file for each value of c (one for absolute and one for relative duration). This function also returns a stacked sorted list that is necessary for the computation of calculate_max_overlap()

### calculate_max_overlap()
A python algorithm that transforms a stacked and sorted list of study-sites with recruitment start and end dates into a list of study-sites and their congestion metrics. One of the two congestion metrics drv_max_site_study_overlap_trials_count_dqs calculates the maximum number of studies that overlapped with the given study-site at any single time point during the study-site's recruitment period (from site oped date to last subject first dose date).

drv_max_site_overlap_trials_count_dqs is a similar metric that calculates the maximum number of studies that overlapped at a site at any given moment in all of the dataset (not tied to any particular study). 



