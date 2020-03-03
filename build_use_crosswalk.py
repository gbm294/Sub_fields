# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 15:36:47 2020

@author: gbm294
"""



import sys, os
from datetime import date

import pandas as pd
pd.set_option("display.max_colwidth", 10000)  ##prevents strings from being truncated.
pd.set_option("display.max_columns", 100)  ##so all columns will show in print

import csv
#from datetime import datetime

import numpy as np


### File path to parts ###########################################
def split_filepath(file_path):
    file_dir = os.path.dirname(file_path)  #directory of file
    #file_name = os.path.basename(file_path)  ##get input file name
    file_name, file_ext =  os.path.splitext(os.path.basename(file_path))#[1]
    #os.chdir(user_dir)  #set as working directory
    return file_path, file_dir, file_name, file_ext
###################################################################


### Open File #########################################
def process_csv(in_path):
    df = pd.read_csv(in_path, sep=',', escapechar='\\', quotechar='"', encoding = "ISO-8859-1", keep_default_na =False)
    return df
###################################################################



### Read csv file into dictionary #########################################
def read_csv_to_dict(f):
    with open(f, mode='r') as infile:
        reader = csv.reader(infile)
        next(reader)  #remove if crosswalk file doesn't have a header.
        #mydict = {rows[0]:rows[1] for rows in reader}
        mydict = {}
        for row in reader:
            try:
                mydict[row[0]] = row[1] 
            except:
                pass  ## in case there are blank rows in the file
    return mydict



### Test Replace ID with number #########################################
def replace_id(df_in, field_to_sub, new_field_name, cw_path, cnt):
    df_field_unique = list(pd.unique(df_in[field_to_sub]))  ## get unique values
    
    ### Try reading existing crosswalk file.  If not exists, create dictionary
    try:
        crosswalk_dict = read_csv_to_dict(cw_path)
    except:
        crosswalk_dict = {}
    
    ### Get highest value in existing dictionary
#    try:
#        mx = max(crosswalk_dict.values()) #max(crosswalk_dict, key=crosswalk_dict.get)
#        cnt = int(mx)
#    except:
#        pass
    try:
        mx = int(max(crosswalk_dict.values()), 36) #max(crosswalk_dict, key=crosswalk_dict.get)
        cnt = int(mx)
    except:
        pass
    
    ## Loop through values in file.  If they exist in dictionary, do nothing. If they don't, then add them.
    ### may want to pass this to a function #########
#    for r in df_field_unique:  ## set value to a sequence number
#        if str(r) in crosswalk_dict:
#            pass
#        else:
#            cnt += 1
#            crosswalk_dict[str(r)] = str(cnt)
    cw_changes = 0
    ###################################################  
    for r in df_field_unique:  ## set value to a sequence number
        if str(r) in crosswalk_dict:
            pass
        else:
            cnt += 1
            #crosswalk_dict[str(r)] = str(cnt)
            crosswalk_dict[str(r)] = np.base_repr(cnt, base=36)
            cw_changes += 1
    ###################################################  
    
        
    ind = df_in.columns.get_loc(field_to_sub)  #get index of column
    df_in[field_to_sub] = df_in[field_to_sub].astype(str)    
    df_in.insert(loc=ind, column=new_field_name, value=df_in[field_to_sub].map(crosswalk_dict))  ##map to new ID    
    df_subbed = df_in.drop(columns=[field_to_sub])  ##Remove substituted column
    
    
    return df_subbed, crosswalk_dict, new_field_name, cw_changes
###################################################################




def main():
    in_f = 'U:/UWHealth/EA/SpecialShares/DM/CRDS/AdHocQueries/Special projects/gm_playground/File_to_code.csv'
    
    in_file_path, in_file_dir, in_file_name, in_file_ext = split_filepath(in_f) 
    os.chdir(in_file_dir) #set as working directory
    today = date.today().strftime("%Y_%m_%d")     ##Get todays date
    
    ### Read csv file into dataframe #################
    df_in = process_csv(in_file_path)
    #print(df_in.head(5))
    
    ### Set fields to replace #################
    field_list = [
                {'field_to_sub' : 'MRN'
                   , 'new_field_name' : 'SUBJECT_ID'
                   , 'crosswalk_path' : 'U:/UWHealth/EA/SpecialShares/DM/CRDS/AdHocQueries/Special projects/gm_playground/MRN_crosswalk.csv'
                   , 'number_to_start' : 1000000000   ##This should be replaced by a method of substitute
                   }
                    ,
                    {'field_to_sub' : 'ENCOUNTER'
                      , 'new_field_name' : 'ENCOUNTER_SUB'
                      , 'crosswalk_path' : 'U:/UWHealth/EA/SpecialShares/DM/CRDS/AdHocQueries/Special projects/gm_playground/ENCOUNTER_crosswalk.csv'
                      , 'number_to_start' : 100000  ##This should be replaced by a method of substitute
                      }
                    ]
    
    
    ### Copy dataframe 
    deident_df = df_in.copy()
    
    ### Create List.  Items will be crosswalks for each field substituted.
    crosswalks = []
    
    ###Send to function to substitute field
    for f in field_list:
        ##deident_df, crosswalk_df = replace_id(deident_df, f['field_to_sub'], f['new_field_name'], f['crosswalk_path'], f['number_to_start'])
        deident_df, crosswalk_dict, new_field_name, cw_changes = replace_id(deident_df, f['field_to_sub'], f['new_field_name'], f['crosswalk_path'], f['number_to_start'])
        
        ## Add crosswalk for field to crosswalks list
        #crosswalks.append({f['field_to_sub']:crosswalk_df})
        crosswalks.append({f['field_to_sub']:crosswalk_dict, 'changes': cw_changes, 'field_name': f['field_to_sub']} )
    
    
    ### Write outout file
    file_out = '%s_subbed_%s.csv' % (in_file_name, today)  ##build output file name
    deident_df.to_csv( file_out, index=False, encoding='utf-8-sig')
    
    ### Write crosswalk files if needed
    for c in crosswalks:
        if c['changes'] > 0:
            file_out = '%s_%s_crosswalk_%s.csv' % (in_file_name, c['field_name'], today)  ##build output file name    
            with open(file_out, 'w', newline="") as csv_file:  
                writer = csv.writer(csv_file)
                writer.writerow([c['field_name'],new_field_name])
                for key, value in c[c['field_name']].items():
                   writer.writerow([key, value])
                
    print('Done!')

if __name__ == "__main__":
    main()











if __name__ == "__main__":
    main()