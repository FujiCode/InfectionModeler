import pandas as pd
import numpy

class DataProcessor():
    
    def __init__(self):
        self.__object = None
               
    def _create_US_df(self, US_total_df, df_list, date_set, column_set, forecast,
                      states_abr):
        """Method will return a concatenation (union) of all DataFrames for each
        individual state, with an extra column for listing the state
        abbreviation. The return DataFrame will be used for generating
        the U.S. heat-map. Additionally, the US_total_df dictionary will be
        modified within method for creating total counts from all states.
         
        :param US_total_df: Empty Dictionary list
        :type US_total_df: dictionary
        
        :param  date_set: Set of all dates within data set index range
        :type date_set: set
        
        :param  column_set: Set of all columns within states DataFrame 
                            (should be consistent for all DataFrames)  
        :type column_set: set
        
        :param  forecast: Determines whether to use forecasted, or /
                          non-forecasted DataFrame
        :type forecast: bool
        
        :param  states_abr: US state state abbreviations of String type
        :type states_abr: list, or dictionary keys 
        
        :return: A union set of DataFrames for each individual state
        :rtype: Pandas DataFrame
        """
        __total = 0 
        __count = 0
        #Average values from all states 
        for date in date_set:
            US_total_df['date'].append(date)
            for col in column_set:
                #Iterate through each state
                for state in states_abr:
                    #Count variable for averaging temperature
                    #values only
                    __count += 1
                    if forecast == True:
                        df = df_list[state + '_forecast']
                    else:    
                        df = df_list['df_'+state]
                    #Obtain the value for a given date
                    #and for a given state
                    value = df[col][date]
                    if numpy.isnan(value) == True:
                        __total += 0
                    else:
                        __total += value
                    
                #If the column is the temperature column
                #assign the average
                if col == 'TAVG':
                    #if str(date) >= '2021-01-28' and str(date) <= '2021-04-01':
                        #print('Date:',date,' Col:',col,' Value:',__total/__count)
                        #print()
                    US_total_df[col].append(__total/__count)
                #Total value from all states
                #for a given date
                else: 
                    #if str(date) >= '2021-01-28' and str(date) <= '2021-02-25':
                        #print('Date:',date,' Col:',col,' Value:',__total/__count)
                        #print()   
                    US_total_df[col].append(__total)
                __total = 0
                __count = 0

    
        __df_union = pd.DataFrame()
        __state_list = []
        __date_list = []
        __i = 0      
        
        #The date, and 'state' column were
        #removed for processing, and 
        #will be recreated
        #State column is necessary for creating
        #heatmap
        for state in states_abr:
            #Obtain a copy of a merged DataFrame
            
            #If False, then retrieve non-forecasted DataFrame.
            #If True, then retrieve forecasted DataFrame.
            if forecast == True:
                df = df_list[state + '_forecast'].copy().sort_index()
            else:    
                df = df_list['df_'+state].copy().sort_index()
            #Obtain a copy of a merged DataFrame
            #df = df_list[df_state].copy().sort_index()
            #Create a list of indexes from this DataFrame
            index_list = df.index
            #Use the state abbreviation for
            #creating a new column consisting
            #of values of the state abbreviation
            while __i < len(df.index):
                __state_list.append(state)
                __i += 1   
            #Create the new 'state' column 
            df['state'] = __state_list
            for date in index_list:
                __date_list.append(str(date))
            #Create the new 'date' column
            df['date'] = __date_list
            __df_union = pd.concat([__df_union, df], ignore_index=False)
            __i = 0
            __state_list = []
            __date_list = []
        
        return __df_union 
        
    def _merge_all_dfs(self, merged_df, COVID19_df, daily_temp_df):
        """Method will merge the columns of COVID19 DataFrame with the
        columns of the Temperature DataFrame. Merged DataFrames will be
        stored in merged_df.
        
        :param merged_df: Empty dictionary list for storing merged DataFrames
        :type merged_df: dictionary
        
        :param COVID19_df: Dictionary containing COVID19 DataFrames from all states
        :type merged_df: dictionary
        
        :param daily_temp_df: Dictionary containing Temperature DataFrames from all states
        :type daily_temp_df: dictionary
        """
        for state_c in COVID19_df:
            for state_t in daily_temp_df:
                if state_c[-2:] == state_t[-2:]:
                    __df = pd.merge(COVID19_df[state_c],   
                                    daily_temp_df[state_t], how='right',
                                    left_index=True, right_index=True)
                    merged_df['df_' + state_c[-2:]] = __df   
        
    def _convert_file_to_dataframe(self, file):
        return pd.read_excel(file)    
    
    def _US_df_into_state_dfs(self, 
                             US_df_data_set,
                             US_states_list):
        __df_list = {}   
        for state in US_states_list:
            __df_list["df_COVID19_" + state] = US_df_data_set[US_df_data_set['state']==state]
        return __df_list
    
    def _set_index(self, df, column):
        indexed_df = df.copy().set_index([column])
        del df
        return indexed_df
    
    def _convert_to_datetime(self, df, column):
        datetime_df = df.copy()
        datetime_df[column] = pd.to_datetime(df[column], infer_datetime_format=True)
        del df
        return datetime_df 
            
    def _date_average(self, df, date_column, target_column):
        """Method will accept the name of the date column (date_column),
        and the column to average values for (target_column). This will
        also drop all other columns that are not being refer to. Duplicate
        dates will merge into a single date with the average value.
        
        :param df: DataFrame containing a 'Date' column with a column of 
               numerical data type
        :type df: Pandas DataFrame
        
        :param date_column: Name of the data column
        :type date_column: String
        
        :param target_column: Name of the target column
        :type target_column: String
        """
        date_list = {}
        for date in df[date_column]:
            if date not in date_list:
                date_list[date] = 0 
        df = df.set_index(date_column)
        date_column_list = []
        target_column_list = []
        for date in date_list.keys():
            data_type = str(type(df.loc[date][target_column])) 
            if (data_type == "<class 'numpy.float64'>" or
                data_type == "<class 'numpy.int64'>"):
                avg = df.loc[date][target_column]
                date_column_list.append(date)
                target_column_list.append(avg)
            else:   
                value_list = df.loc[date][target_column].to_numpy()
                sum_total = sum(value_list)
                length = len(value_list)
                avg = sum_total / length
                date_column_list.append(date)
                target_column_list.append(avg)
        
        df = pd.DataFrame(
            {str(date_column): date_column_list, str(target_column):target_column_list}
        )
        df[date_column] = pd.to_datetime(df[date_column], infer_datetime_format=True)
        df = df.sort_values(by=[date_column])                
        return df        
           
    
    def _sort_df(self, df, column, asc):
        sort_df = df.copy().sort_values(
                            by=[column], ascending=asc)
        del df
        return sort_df
            
    def _append_dfs(self, df1, df2):
        d1 = df1.copy()
        d2 = df2.copy()
        d3 = d1.append(d2)
        del d1
        del d2
        return d3
    
    def _merge_dfs(self, df1, df2):
        d1 = df1.copy()
        d2 = df2.copy()
        d3 = pd.merge(
                      df1, 
                      df2, 
                      left_index=True, 
                      right_index=True
                      )
        del d1
        del d2
        return d3
    
    def _get_states_census_df_list(self, location):
        df_census_list = {}
        df_census_list['census_US'] = pd.read_csv(
            location + r'\census_US.csv'
        )
        df_census_list['census_AL'] = pd.read_csv(
             location + '\census_AL.csv'
        )
        df_census_list['census_AK'] = pd.read_csv(
            location + r'\census_AK.csv'
        )
        df_census_list['census_AR'] = pd.read_csv(
            location + r'\census_AR.csv'
        )
        df_census_list['census_AZ'] = pd.read_csv(
            location + r'\census_AZ.csv'
        )
        df_census_list['census_CA'] = pd.read_csv(
           location + r'\census_CA.csv'
        )
        df_census_list['census_CO'] = pd.read_csv(
            location + r'\census_CO.csv'
        )
        df_census_list['census_CT'] = pd.read_csv(
            location + r'\census_CT.csv'
        )
        df_census_list['census_DE'] = pd.read_csv(
            location + r'\census_DE.csv'
        )
        df_census_list['census_FL'] = pd.read_csv(
            location + r'\census_FL.csv'
        )
        df_census_list['census_GA'] = pd.read_csv(
            location + r'\census_GA.csv'
        )
        df_census_list['census_HI'] = pd.read_csv(
            location + r'\census_HI.csv'
        )
        df_census_list['census_IL'] = pd.read_csv(
            location + r'\census_IL.csv'
        )
        df_census_list['census_IN'] = pd.read_csv(
            location + r'\census_IN.csv'
        )
        df_census_list['census_IA'] = pd.read_csv(
            location + r'\census_IA.csv'
        )
        df_census_list['census_KS'] = pd.read_csv(
            location + r'\census_KS.csv'
        )
        df_census_list['census_KY'] = pd.read_csv(
            location + r'\census_KY.csv'
        )
        df_census_list['census_LA'] = pd.read_csv(
            location + r'\census_LA.csv'
        )
        df_census_list['census_ME'] = pd.read_csv(
            location + '\census_ME.csv'
        )
        df_census_list['census_MD'] = pd.read_csv(
            location + r'\census_MD.csv'
        )
        df_census_list['census_MA'] = pd.read_csv(
            location + r'\census_MA.csv'
        )
        df_census_list['census_MI'] = pd.read_csv(
            location + r'\census_MI.csv'
        )
        df_census_list['census_MN'] = pd.read_csv(
            location + r'\census_MN.csv'
        )
        df_census_list['census_MS'] = pd.read_csv(
            location + r'\census_MS.csv'
        )
        df_census_list['census_MO'] = pd.read_csv(
            location + r'\census_MO.csv'
        )
        df_census_list['census_MT'] = pd.read_csv(
            location + r'\census_MT.csv'
        )
        df_census_list['census_NE'] = pd.read_csv(
            location + r'\census_NE.csv'
        )
        df_census_list['census_NV'] = pd.read_csv(
            location + r'\census_NV.csv'
        )
        df_census_list['census_NH'] = pd.read_csv(
            location + r'\census_NH.csv'
        )
        df_census_list['census_NJ'] = pd.read_csv(
            location + r'\census_NJ.csv'
        )
        df_census_list['census_NM'] = pd.read_csv(
            location + r'\census_NM.csv'
        )
        df_census_list['census_NY'] = pd.read_csv(
            location + r'\census_NY.csv'
        )
        df_census_list['census_NC'] = pd.read_csv(
            location + r'\census_NC.csv'
        )
        df_census_list['census_ND'] = pd.read_csv(
            location + r'\census_ND.csv'
        )
        df_census_list['census_OH'] = pd.read_csv(
            location + r'\census_OH.csv'
        )
        df_census_list['census_OK'] = pd.read_csv(
            location + r'\census_OK.csv'
        )
        df_census_list['census_OR'] = pd.read_csv(
            location + r'\census_OR.csv'
        )
        df_census_list['census_PA'] = pd.read_csv(
            location + r'\census_PA.csv'
        )
        df_census_list['census_RI'] = pd.read_csv(
            location + r'\census_RI.csv'
        )
        df_census_list['census_SC'] = pd.read_csv(
            location + r'\census_SC.csv'
        )
        df_census_list['census_SD'] = pd.read_csv(
            location + r'\census_SD.csv'
        )
        df_census_list['census_TN'] = pd.read_csv(
            location + r'\census_TN.csv'
        )
        df_census_list['census_TX'] = pd.read_csv(
            location + r'\census_TX.csv'
        )
        df_census_list['census_UT'] = pd.read_csv(
            location + r'\census_UT.csv'
        )
        df_census_list['census_VT'] = pd.read_csv(
            location + r'\census_VT.csv'
        )
        df_census_list['census_VA'] = pd.read_csv(
            location + r'\census_VA.csv'
        )
        df_census_list['census_WA'] = pd.read_csv(
            location + r'\census_WA.csv'
        )
        df_census_list['census_WV'] = pd.read_csv(
            location + r'\census_WV.csv'
        )
        df_census_list['census_WI'] = pd.read_csv(
            location + r'\census_WI.csv'
        )
        df_census_list['census_WY'] = pd.read_csv(
            location + r'\census_WY.csv'
        )
        return df_census_list 
                
    def _get_states_daily_temp_df_list(self, test, location):
        df_temp_list = {}
        if test == True:
            print()
        else:
            df_temp_list['daily_temp_AK'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\AK_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\AK_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_AL'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\AL_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\AL_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            
            df_temp_list['daily_temp_AR'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\AR_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\AR_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_AZ'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\AZ_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\AZ_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\AZ_Daily_Temp3.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_CA'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\CA_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\CA_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\CA_Daily_Temp3.csv'
                    ),
                    pd.read_csv(
                        location + r'\CA_Daily_Temp4.csv'
                    ),
                    pd.read_csv(
                        location + r'\CA_Daily_Temp5.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_CO'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\CO_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                       location + r'\CO_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\CO_Daily_Temp3.csv'
                    ),
                    pd.read_csv(
                        location + r'\CO_Daily_Temp4.csv'
                    ),
                    pd.read_csv(
                        location + r'\CO_Daily_Temp5.csv'
                    ),
                    pd.read_csv(
                        location + r'\CO_Daily_Temp6.csv'
                    ),
                    pd.read_csv(
                        location + r'\CO_Daily_Temp7.csv'
                    )
                ], 
                ignore_index=True
            )                                     
            df_temp_list['daily_temp_CT'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\CT_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\CT_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )  
            df_temp_list['daily_temp_DE'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\DE_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\DE_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_FL'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\FL_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\FL_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\FL_Daily_Temp3.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_GA'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\GA_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\GA_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_HI'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\HI_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\HI_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )  
            df_temp_list['daily_temp_ID'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\ID_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\ID_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_IL'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\IL_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\IL_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\IL_Daily_Temp3.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_IN'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\IN_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\IN_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\IN_Daily_Temp3.csv'
                    )
                ], 
                ignore_index=True
            )   
            df_temp_list['daily_temp_IA'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\IA_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\IA_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_KS'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\KS_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\KS_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\KS_Daily_Temp3.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_KY'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\KY_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\KY_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_LA'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\LA_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\LA_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_ME'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\ME_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\ME_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_MD'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\MD_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\MD_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_MA'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\MA_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\MA_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_MS'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\MS_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\MS_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_MI'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\MI_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\MI_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_MN'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\MN_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\MN_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\MN_Daily_Temp3.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_MS'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\MS_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\MS_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_MO'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\MO_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\MO_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\MO_Daily_Temp3.csv'
                    )
                ], 
                ignore_index=True
            )             
            df_temp_list['daily_temp_MT'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\MT_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\MT_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_NC'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\NC_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\NC_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\NC_Daily_Temp3.csv'
                    ),
                    pd.read_csv(
                        location + r'\NC_Daily_Temp4.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_NE'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\NE_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\NE_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\NE_Daily_Temp3.csv'
                    ),
                    pd.read_csv(
                        location + r'\NE_Daily_Temp4.csv'
                    ),
                    pd.read_csv(
                        location + r'\NE_Daily_Temp5.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_NV'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\NV_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\NV_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_NH'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\NH_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\NH_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_NJ'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\NJ_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\NJ_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_NM'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\NM_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\NM_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\NM_Daily_Temp3.csv'
                    ),
                    pd.read_csv(
                        location + r'\NM_Daily_Temp4.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_NY'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\NY_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\NY_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\NY_Daily_Temp3.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_ND'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\ND_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\ND_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\ND_Daily_Temp3.csv'
                    ),
                    pd.read_csv(
                        location + r'\ND_Daily_Temp4.csv'
                    )
                ], 
                ignore_index=True
            )         
            df_temp_list['daily_temp_OH'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\OH_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\OH_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_OK'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\OK_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\OK_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_OR'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\OR_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\OR_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\OR_Daily_Temp3.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_PA'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\PA_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\PA_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\PA_Daily_Temp3.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_RI'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\RI_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\RI_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_SC'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\SC_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\SC_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_SD'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\SD_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\SD_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_TN'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\TN_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\TN_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\TN_Daily_Temp3.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_TX'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\TX_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\TX_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\TX_Daily_Temp3.csv'
                    ),
                    pd.read_csv(
                        location + r'\TX_Daily_Temp4.csv'
                    ),
                    pd.read_csv(
                        location + r'\TX_Daily_Temp5.csv'
                    ),
                    pd.read_csv(
                        location + r'\TX_Daily_Temp6.csv'
                    ),
                    pd.read_csv(
                        location + r'\TX_Daily_Temp7.csv'
                    ),
                    pd.read_csv(
                        location + r'\TX_Daily_Temp8.csv'
                    ),
                    pd.read_csv(
                        location + r'\TX_Daily_Temp9.csv'
                    ),
                    pd.read_csv(
                        location + r'\TX_Daily_Temp10.csv'
                    ),
                    pd.read_csv(
                        location + r'\TX_Daily_Temp11.csv'
                    ),
                    pd.read_csv(
                        location + r'\TX_Daily_Temp12.csv'
                    ),
                    pd.read_csv(
                        location + r'\TX_Daily_Temp13.csv'
                    )
                ], 
                ignore_index=True
            )                
            df_temp_list['daily_temp_UT'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\UT_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\UT_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_VT'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\VT_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\VT_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_VA'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\VA_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\VA_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_WA'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\WA_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\WA_Daily_Temp2.csv'
                    ),
                    pd.read_csv(
                        location + r'\WA_Daily_Temp3.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_WV'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\WV_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\WV_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_WI'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\WI_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\WI_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
            df_temp_list['daily_temp_WY'] = pd.concat(
                [
                    pd.read_csv(
                        location + r'\WY_Daily_Temp1.csv'
                    ),
                    pd.read_csv(
                        location + r'\WY_Daily_Temp2.csv'
                    )
                ], 
                ignore_index=True
            )
                       
        #Reformatting the date values to the form 'YYYY-M-D'
        df_temp_list['daily_temp_AK']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_AK']["DATE"])
 
        df_temp_list['daily_temp_AL']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_AL']["DATE"])
            
        df_temp_list['daily_temp_AR']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_AR']["DATE"])
            
        df_temp_list['daily_temp_AZ']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_AZ']["DATE"])   
            
        df_temp_list['daily_temp_CA']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_CA']["DATE"])
            
        df_temp_list['daily_temp_CO']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_CO']["DATE"])  
            
        df_temp_list['daily_temp_CT']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_CT']["DATE"])  
        
        df_temp_list['daily_temp_DE']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_DE']["DATE"])
            
        df_temp_list['daily_temp_FL']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_FL']["DATE"]) 
            
        df_temp_list['daily_temp_GA']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_GA']["DATE"])  
            
        df_temp_list['daily_temp_HI']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_HI']["DATE"]) 
            
        df_temp_list['daily_temp_ID']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_ID']["DATE"])   
        
        df_temp_list['daily_temp_IN']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_IN']["DATE"])
            
        df_temp_list['daily_temp_IA']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_IA']["DATE"])
            
        df_temp_list['daily_temp_KS']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_KS']["DATE"]) 
            
        df_temp_list['daily_temp_KY']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_KY']["DATE"]) 
            
        df_temp_list['daily_temp_LA']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_LA']["DATE"])
        
        df_temp_list['daily_temp_ME']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_ME']["DATE"])
            
        df_temp_list['daily_temp_MD']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_MD']["DATE"])
            
        df_temp_list['daily_temp_MA']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_MA']["DATE"])
            
        df_temp_list['daily_temp_MS']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_MS']["DATE"]) 
            
        df_temp_list['daily_temp_MI']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_MI']["DATE"])
            
        df_temp_list['daily_temp_MN']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_MN']["DATE"])
            
        df_temp_list['daily_temp_MS']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_MS']["DATE"])
        
        df_temp_list['daily_temp_MO']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_MO']["DATE"])
            
        df_temp_list['daily_temp_MT']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_MT']["DATE"])
         
        df_temp_list['daily_temp_NC']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_NC']["DATE"]) 
            
        df_temp_list['daily_temp_NE']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_NE']["DATE"])
            
        df_temp_list['daily_temp_NV']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_NV']["DATE"])
            
        df_temp_list['daily_temp_NH']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_NH']["DATE"])
        
        df_temp_list['daily_temp_NJ']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_NJ']["DATE"])
            
        df_temp_list['daily_temp_NM']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_NM']["DATE"])
            
        df_temp_list['daily_temp_NY']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_NY']["DATE"])
            
        df_temp_list['daily_temp_ND']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_ND']["DATE"])
            
        df_temp_list['daily_temp_OH']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_OH']["DATE"])
        
        df_temp_list['daily_temp_OK']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_OK']["DATE"]) 
            
        df_temp_list['daily_temp_OR']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_OR']["DATE"])
            
        df_temp_list['daily_temp_PA']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_PA']["DATE"])
            
        df_temp_list['daily_temp_RI']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_RI']["DATE"])
            
        df_temp_list['daily_temp_SC']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_SC']["DATE"]) 
            
        df_temp_list['daily_temp_SD']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_SD']["DATE"])
            
        df_temp_list['daily_temp_TN']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_TN']["DATE"])
            
        df_temp_list['daily_temp_TX']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_TX']["DATE"])
            
        df_temp_list['daily_temp_UT']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_UT']["DATE"])
            
        df_temp_list['daily_temp_VT']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_VT']["DATE"])
            
        df_temp_list['daily_temp_VA']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_VA']["DATE"])
            
        df_temp_list['daily_temp_WA']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_WA']["DATE"])
            
        df_temp_list['daily_temp_WV']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_WV']["DATE"])
            
        df_temp_list['daily_temp_WI']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_WI']["DATE"])
            
        df_temp_list['daily_temp_WY']["DATE"] = \
            pd.to_datetime(df_temp_list['daily_temp_WY']["DATE"])            
               
        return df_temp_list
    