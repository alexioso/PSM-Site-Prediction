import pandas as pd
from time import perf_counter
from tqdm import tqdm


def main():
    
    df = pd.read_excel("site_congestion_history.xlsx")
    
    start = perf_counter()
    df_output1, stacked_sorted_input = event_competition_calculate(df,
                                num_cap_overlap=7,
                                key_column_identity_id = "facility_golden_id",
                                key_column_event_id = "nct_id_hist",	
                                key_column_date_start = "site_open_hist", 
                                key_column_date_end = "study_last_subject_first_dose")

    df_output2 = calculate_max_overlap(stacked_sorted_input,
                                       site_id = "facility_golden_id",
                                       study_id = "nct_id_hist")
    
    df_output = df_output1.merge(df_output2, 
                                 on=["facility_golden_id","nct_id_hist"],
                                 how="left")

    df_output.to_excel("results.xlsx")
    print("Total runtime:",str(perf_counter() - start))
    

#calculate_max_overlap
#Writen by Alex Braksator 
#2020-10-29

#INPUTS:
    #df: (Pandas DataFrame) 
        #must be a dataframe consisting of site and study columns, stacked by the
        #dates of site open and study last subject first dose, and ordered by the
        #resulting date column from earliest to latest. 
    #site_id: (str) column name of df pertaining to site_id (Facility Golden ID)
    #study_id: (str) column name of df pertaining to study_id (nct_id_hist)
    
#Note by passing a stacked sorted dataframe, we don't even need the values of
    #the dates to perform these calculations

#OUTPUTS:
#results2: dataframe of study-site pairs along with the maximum number of study
     #overlapped experienced by the pair at any moment. 
     #Also a column with the maximum number of study overlap for each site at any
     #moment
def calculate_max_overlap(df,site_id,study_id):
    
    site = site_id
    study = study_id

    
    #a nested dictionary structure used to keep 
    #track of max overlap  counts.
    overlap_dict = {}
    #store the results in this here dictionary
    output = {study:[],site:[],"max_count":[]}
    print("Loop 2 of 2")
    #for each row in the sorted dataframe
    for index, row in tqdm(df.reset_index().iterrows(),
                           total=df.reset_index().shape[0]):
        #if this is our first time seeing this site, add it to the overlap_dict 
        #with key=facility_golden_id 
        #and value is a dict containing a list of studies currently going on 
        #and a max_count value set to 0 representing the maximum study overlap
        #at the site. Notice the value of study_list is itself a dictionary of 
        #study:integer pairs representing the max overlap count at the 
        #study-site level. We initialize to 0 because a single study does not
        #overlap itself. Will increment to one when another study appears before
        #this one ends and so on...
        if row[site] not in overlap_dict.keys():
            overlap_dict[row[site]] = {"study_list":
                                    {row[study]: 0}, 
                                "max_count":0}

        #if we have seen this site before...
        else:
            #if we have seen this site-study before, this is the last 
            #time we will ever see it and we can save its max value to 
            #output and remove it from overlap_dict 
            if row[study] in overlap_dict[row[site]]["study_list"].keys():
                output[study].append(row[study])
                output[site].append(row[site])
                output["max_count"].append(overlap_dict[row[site]]["study_list"][row[study]])
                
                #save the length of study_list before removing the finished study
                current_study_overlap_list_length = len(overlap_dict[row[site]]["study_list"])
                
                #now we have to remove the study from study_list for this site
                #note this won't affect the max_count of overlap associated to any
                #other study in study_list since the list size has decreased
                del overlap_dict[row[site]]["study_list"][row[study]]
                
                #we check the max overlap count of the site here when we remove
                #a study because the study_list reaches its potential largest
                #size in these iterations right when studies are removed. We 
                #could also have done this in the following else statement but
                #I figured this placement would happen less yet still be 
                #sufficient at keeping the correct count. 
                if overlap_dict[row[site]]["max_count"] < current_study_overlap_list_length:
                    overlap_dict[row[site]]["max_count"] = current_study_overlap_list_length
                  
                
            #if we have seen this site before, but this time with a different study...
            #add the new study to the list of studies associated to the site
            else:
                #print("new study")
                #print(row[study])
                #first let's calculate the current count of ongoing studies for the site
                #including the new study (minus 1, kind of hard to explain why)
                current_site_study_overlap_count = len(overlap_dict[row[site]]["study_list"])
                #go through each study in study_list and update count 
                #if current overlap count is greater
                for study_id in overlap_dict[row[site]]["study_list"].keys():
                    if current_site_study_overlap_count > overlap_dict[row[site]]["study_list"][study_id]:
                        overlap_dict[row[site]]["study_list"][study_id] = current_site_study_overlap_count
                #now we add the new study to site's study list with the current overlap count
                # as its initial value
                overlap_dict[row[site]]["study_list"][row[study]] = current_site_study_overlap_count
                
        
    #create output dataframe using the site max overlap values
    site_max_results_dict = {site:[],"site_max_overlap":[]}   
    for site_id in overlap_dict:
        site_max_results_dict[site].append(site_id)
        site_max_results_dict["site_max_overlap"].append(overlap_dict[site_id]["max_count"])
        
    site_max_results = pd.DataFrame(site_max_results_dict) 
    #merge the study-site max overlap values with site overlap values
    results2 = pd.DataFrame(output).merge(site_max_results,on=site,how="left")
    return(results2)

def event_competition_calculate(_df_input,
                                # define cap for the max count column
                                num_cap_overlap=7,
                                # define input columns keys
                                key_column_identity_id='facility_golden_id_dqs',
                                key_column_event_id='nct_number_dqs',
                                key_column_date_start='site_open_dqs',
                                key_column_date_end='drv_lse_study_dqs',
                                # define output columns names
                                key_column_competing_suffix="competing_trials_months_dqs",
                                key_column_max_count_identity='drv_max_site_overlap_trials_count_dqs',
                                key_column_max_count_identity_event='drv_max_site_study_overlap_trials_count_dqs',
                                ):
    
    
    # check missing data
    missing_value_count = _df_input.isna().sum().sum()  # no missing values
    if missing_value_count > 0:
        print("ERROR: Check Missing Values in DataFrame!!!")
        

    # Generate output column names for overlapping events duration storage
    ls_column_name = []  # the absolute duration column names
    ls_column_name_relative = []  # the relative duration column names
    for number in range(num_cap_overlap):
        ls_column_name.append("drv_absolute_duration_" + str(number) + '_' + key_column_competing_suffix)
        ls_column_name_relative.append("drv_relative_duration_" + '_' + str(number) + key_column_competing_suffix)

    # # define an indexing object for multi-index slicing
    # idx = pd.IndexSlice

    # only select the relevant columns
    _df_input = _df_input[
        [key_column_identity_id, key_column_event_id, key_column_date_start, key_column_date_end]].copy()

    # Group and create multi row index
    _df_input_index = _df_input.groupby([key_column_identity_id, key_column_event_id]).first()


    # check if the [key_column_identity_id, key_column_event_id] is an unique record id in previous data frame
    if _df_input_index.shape[0] != _df_input.shape[0]:
        print("ERROR: the combination of " + key_column_identity_id + " and " + key_column_event_id +
              " are not unique for each data record")

    # stack the starting and ending date into one column and sort it by date
    _df_input_stack = _df_input_index.stack()
    _df_input_stack = pd.DataFrame({'time_sequence': _df_input_stack}).sort_values(by='time_sequence')

    
    # create a group object based on identity column (e.g. site)
    _df_input_stack_group = _df_input_stack.groupby([key_column_identity_id])
    
    
    #pd.DataFrame(_df_input_stack).to_excel("interim2.xlsx")
    #print("Done")
    
    
    
    # define two data frames to collect results.
    df_collect = None
    df_collect_2 = None

    # Iterate through each identity (e.g. site),
    # 1. count the overlapping events in each time section, put it in 'overlap_count' column;
    # 2. calculate the duration (months) for each period, and store in 'time interval' column;
    # 3. make those two columns and the key columns (identity, study) into a small data frame, only for one identity,
    #    then append it to df_collect;
    # 4. Create 2 dictionary to collect the generated data ongoing studies and completed studies. The key will be the
    #    study number, and value will be a list of number. The size of list will be the cap of max overlapping number.
    #    In each element of the list, it will store the time duration (months) of the overlapping.
    #       e.g. {"NCT1234567" : [9, 8, 2, 0, 0, 0]}
    #       it means for the study "NCT1234567" happening in the current identity (site), the cap of max overlapping is
    #           6, all the overlapping events count larger than 6 will be count as 6
    #           1 study running at the same time for 9 months;
    #           2 study running at the same time for 8 months;
    #           3 study running at the same time for 6 months;
    # 5. Iterate through the small data frame generated in step 3, in each study events (start or end record).
    # 6. For any study event (no matter start or end), update the study records in ongoing dictionary.
    # 7. When get to a study start event, add it to the ongoing dictionary.
    # 8. When get to a study end event, remove it from ongoing dictionary and add it to the complete dictionary.

    print("Loop 1 of 2")
    for golden_id, df_one_site in tqdm(_df_input_stack_group,
                                       total=pd.DataFrame(_df_input_stack_group).shape[0]):
        df_one_site = df_one_site.copy()

        ls_time_interval = []  # collect the time interval in each time period
        ls_overlap_count = []  # collect overlapping events count in each time period
        overlap_counter = 0  # record the overlapping events count in the last time period in the loop
        time_pre = df_one_site.iloc[0, 0]  # record the time of last time period in the loop

        for index, row in df_one_site.iterrows():
            ls_overlap_count.append(overlap_counter)  # it records the overlapping count at the time period end

            if index[2] == key_column_date_start:
                overlap_counter += 1
            else:
                overlap_counter -= 1

            time_interval = row['time_sequence'] - time_pre
            time_interval = time_interval.days / 30.0  # cast to month int
            ls_time_interval.append(time_interval)
            time_pre = row['time_sequence']

        df_one_site['time_interval'] = ls_time_interval
        df_one_site['overlap_count'] = ls_overlap_count

        if df_collect is None:
            df_collect = df_one_site
        else:
            df_collect = df_collect.append(df_one_site)

        # start to loop through the small data frame just created to gather the interval for each study and each
        # overlapping count
        dic_ongoing_study = {}
        dic_finished_study = {}

        def update_ongoing(dic_temp, df_row):
            overlap_number = int(df_row['overlap_count'])
            # cap the overlap_number by the max overlapping number
            if overlap_number > num_cap_overlap:
                overlap_number = num_cap_overlap
            for _nct_num in dic_temp:
                dic_temp[_nct_num][overlap_number - 1] += row['time_interval']
            return dic_temp

        for index, row in df_one_site.iterrows():
            dic_ongoing_study = update_ongoing(dic_ongoing_study, row)

            nct_num = index[1]
            if nct_num in dic_ongoing_study:
                dic_finished_study[nct_num] = dic_ongoing_study.pop(nct_num)
            else:
                dic_ongoing_study[nct_num] = [0.0] * num_cap_overlap

        # organize the data frame and rename columns
        df_overlap = pd.DataFrame(dic_finished_study).T
        df_overlap.columns = ls_column_name
        df_overlap = df_overlap.reset_index()
        df_overlap[key_column_identity_id] = golden_id
        df_overlap = df_overlap.rename(columns={'index': key_column_event_id})

        if df_collect_2 is None:
            df_collect_2 = df_overlap
        else:
            df_collect_2 = df_collect_2.append(df_overlap)

    # Summarize the max overlapping number has ever happened in an identity (site)
    df_max_overlap_count_site = df_collect[['overlap_count']].groupby(key_column_identity_id).max()
    df_max_overlap_count_site = df_max_overlap_count_site.rename(
        columns={'overlap_count': key_column_max_count_identity})

    df_max_overlap_count_site_study = pd.DataFrame(
        columns=[key_column_identity_id, key_column_event_id, "max_studies_at_a_time"])

    
    """
    start2 = perf_counter()
    

    
    # Summarize the max overlapping number has ever happened in an identity (study amd site)
    for index, row in tqdm(df_collect.reset_index().iterrows(),total=df_collect.reset_index().shape[0]):
        temp_final = pd.DataFrame(columns=[key_column_identity_id, key_column_event_id, "max_studies_at_a_time"])
        temp_final.loc[0, key_column_identity_id] = row[key_column_identity_id]
        temp_final.loc[0, key_column_event_id] = row[key_column_event_id]
        for index_1, row_1 in df_collect.reset_index().iterrows():
            if (index != index_1) and (row_1["level_2"] == "study_last_subject_first_dose"):
                if (row[key_column_identity_id] == row_1[key_column_identity_id]) and (
                        row[key_column_event_id] == row_1[key_column_event_id]):
                    temp = df_collect.reset_index().loc[index: index_1, ]
                    temp_final.loc[0, "max_studies_at_a_time"] = temp["overlap_count"].max() - 1
                    df_max_overlap_count_site_study = pd.concat([df_max_overlap_count_site_study, temp_final])
    print("Time Loop 2", str(perf_counter() - start2))

    


    df_max_overlap_count_site_study = df_max_overlap_count_site_study.rename(
        columns={'max_studies_at_a_time': key_column_max_count_identity_event})

    _df_input = _df_input.merge(df_max_overlap_count_site, how='left', left_on=key_column_identity_id, right_index=True)

    _df_input = _df_input.merge(df_max_overlap_count_site_study, how='left',
                                on=[key_column_identity_id, key_column_event_id])
"""
    _df_input = _df_input.merge(df_collect_2, how='left', on=[key_column_identity_id, key_column_event_id])


    # create relative duration columns
    _df_input['drv_duration_temp'] = (_df_input[key_column_date_end] - _df_input[key_column_date_start]).apply(
        lambda x: x.days / 30.0)
    _df_input[ls_column_name_relative] = _df_input[ls_column_name].apply(
        lambda column: column / _df_input['drv_duration_temp'])


    return _df_input, pd.DataFrame(_df_input_stack)


    
if __name__ == "__main__":
    main()	