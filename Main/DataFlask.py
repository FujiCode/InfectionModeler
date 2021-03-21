from flask import redirect, url_for
from datetime import datetime
from datetime import timedelta  
from Main import Data, Figure
import pandas as pd
import numpy as np


class DataPrepFlask():
    
    
        
    def __init__(self, dictionary, dictionary_model, states_abr):
        self.__dictionary = dictionary
        self.__dictionary_model = dictionary_model
        #self.__states_list = states_list
        self.__figure_factory = Figure.FigureProcessor()
        self.__data_factory = Data.DataProcessor()  
        self.__states_abr = states_abr
    
    
    
    def _average_duplicates(self, request):
        """Method will retrieve the COVID19, and the Temperature DataFrames,
        and modify one of these DataFrames depending on the users view
        (table_view). This will also retrieve the user's request, and
        a check is done to see if the user's specified target column 
        ("average_duplicates2") are of the appropriate data type
        (must be float64, or int64).
        
        :return: Redirect for URL
        :rtype: Flask redirect
        
        """   
        if request.method == 'POST':
            err_msg_invalid_type = "Can only average column of float64, or int64 values\
                                (numerical values)"
            err_msg_nulls = "Column contains nulls"
            if (request.form.get("average_duplicates1") != None and 
                request.form.get("average_duplicates2") != None):
                df_list_covid = self.__dictionary_get('COVID19_df', 'list')
                df_list_temp = self.__dictionary_get('Temp_df', 'list')
                if(self.__dictionary_get('display_table', 'table_view') ==
                    'COVID19'  
                ):
                    for region in df_list_covid:
                        df = df_list_covid[region]
                        #If not the appropriate data type, then 
                        #message error
                        if(str(df[request.form.get("average_duplicates2")].dtypes) !=
                            'float64'
                        ):
                            if(str(df[request.form.get("average_duplicates2")].dtypes) !=
                                'int64'
                            ):
                                self._set_error_message(err_msg_invalid_type)
                                return redirect(url_for('_index'))
                        #Check to see if the column
                        #contain nulls
                        if(self._has_nulls(df, request.form.get(
                            "average_duplicates2"))==True
                        ):
                            self._set_error_message(err_msg_nulls)
                            return redirect(url_for('_index'))
                    #The set of DataFrames does not contain any
                    #NULL values, and the specified columns are of
                    #the appropriate data type
                    for region in df_list_covid:
                        df = df_list_covid[region]
                        df = self.__data_factory._date_average(
                            df, 
                            request.form.get("average_duplicates1"), 
                            request.form.get("average_duplicates2")
                        )
                        df_list_covid[region] = df
                        
                elif (self.__dictionary_get('display_table', 'table_view') ==
                    'Temperature'  
                ):       
                    for region in df_list_temp:
                        df = df_list_temp[region]          
                        #If not the appropriate data type, then 
                        #message error
                        if(str(df[request.form.get("average_duplicates2")].dtypes) !=
                            'float64'
                        ):
                            if(str(df[request.form.get("average_duplicates2")].dtypes) !=
                                'int64'
                            ):
                                self._set_error_message(err_msg_invalid_type)
                                return redirect(url_for('_index'))
                        #Check to see if the column
                        #contain nulls
                        if(self._has_nulls(df, request.form.get(
                            "average_duplicates2"))==True
                        ):
                            self._set_error_message(err_msg_nulls)
                            return redirect(url_for('_index'))
                    #The set of DataFrames does not contain any
                    #NULL values, and the specified columns are of
                    #the appropriate data type. Average duplicate date
                    #values
                    for region in df_list_temp:
                        df = df_list_temp[region]
                        
                        df = self.__data_factory._date_average(
                            df, 
                            request.form.get("average_duplicates1"), 
                            request.form.get("average_duplicates2")
                        )
                        df_list_temp[region] = df 
            self._reset_column_headers()
            self._set_success_message('Averaging Successful')
            return redirect(url_for('_index')) 
    
    
    
    def _check(self, request):
        """
        Check to ensure that the COVID19, and Temperature DataFrames have
        columns/rows of the appropriate data type, the dates have been 
        specified and set, the target variable has been selected, there are
        no missing rows within data set range, and that there are no NULL
        values present in any row within the data set range.   
        
        :return: Redirect for URL
        :rtype: Flask redirect  
        """
        if request.method == 'POST':
            #Checking to see if the columns are of the appropriate
            #data type         
            if self._valid_rows() == False:
                return redirect(url_for('_index')) 
            #Checking to see if the user has selected 
            #valid dates
            if self._dates_set() == False:
                self._set_error_message('Please select date ranges')                                       
                return redirect(url_for('_index')) 
            #Checking to see if the user has selected
            #a valid target variable
            if self.__dictionary_models_get('target_variable', 'valid') == False:
                self._set_error_message('Please select a target variable')                                       
                return redirect(url_for('_index'))
            #Checking to see if there are any missing rows within
            #the specified date ranges
            if self._missing_rows() == False:
                return redirect(url_for('_index'))    
            #Checking to see if any row contains a NULL value 
            if self._check_nulls() == False:
                return redirect(url_for('_index')) 
            if self._tables_partitioned_filled() == False:
                self._set_error_message('Please click on Partition/Fill Tables' +
                                         ' before proceeding')                                       
                return redirect(url_for('_index'))  
            #All checks have passed, proceed to process information  
            return redirect(url_for('_modeling'))
    
    
    
    def _check_nulls(self):
        date_start = self.__dictionary_get('date_ranges', 'start')
        date_end = self.__dictionary_get('date_ranges', 'end')
        date_pred = self.__dictionary_get('date_ranges', 'pred_end')
        df_list_covid = self.__dictionary_get('COVID19_df', 'list')
        df_list_temp = self.__dictionary_get('Temp_df', 'list')
        if(self.__dictionary_models_get('target_variable', 'target_df') ==
            'COVID19'
        ):
            #Using start date to prediction end for
            #checking of NULL values
            for region in df_list_covid:
                df = df_list_covid[region]
                df = df[str(date_start):str(date_end)]
                if df.isnull().values.any() == True:
                    self._set_error_message(
                        'NULL Value(s) detected within specified ' +
                        'range in COVID-19 table for ' +
                        region
                    )
                    return False
            #Using start date to prediction end for
            #checking of NULL values
            for region in df_list_temp :
                df = df_list_temp[region]
                df = df[str(date_start):str(date_pred)]
                if df.isnull().values.any() == True:
                    self._set_error_message(
                        'NULL Value(s) detected within specified ' +
                        'range in Temperature table for ' +
                        region
                    )
                    return False
        elif (self.__dictionary_models_get('target_variable', 'target_df') ==
            'Temperature'
        ):
            #Using start date to prediction end for
            #checking of NULL values
            for region in df_list_covid:
                df = df_list_covid[region]
                df = df[str(date_start):str(date_pred)]
                if df.isnull().values.any() == True:
                    self._set_error_message(
                        'NULL Value(s) detected within specified ' +
                        'range in COVID-19 table for ' +
                        region
                    )
                    return False
            #Using start date to prediction end for
            #checking of NULL values
            for region in df_list_temp :
                df = df_list_temp[region]
                df = df[str(date_start):str(date_end)]
                if df.isnull().values.any() == True:
                    self._set_error_message(
                        'NULL Value(s) detected within specified ' +
                        'range in Temperature table for ' +
                        region
                    )
                    return False    
        return True
     
     
    
    def _check_data_set_dates(self, request):
        """
        Check to ensure that the user specified dates are compatible in regards
        to dates appropriately coinciding one after another.
        
        :return: Redirect for URL
        :rtype: Flask redirect 
        """
        if request.method == 'POST':
            ds = request.form.get("data_set_start")
            de = request.form.get("data_set_end")
            dpred = request.form.get("prediction_end")
            if ds == '' or de == '' or dpred =='':
                self._set_error_message("Please select all dates")                                       
                return redirect(url_for('_index'))
            if ds != '' and de != '' and dpred !='':
                date_start = datetime.strptime(request.form.get("data_set_start"), '%Y-%m-%d').date()
                date_end = datetime.strptime(request.form.get("data_set_end"), '%Y-%m-%d').date()
                date_pred = datetime.strptime(request.form.get("prediction_end"), '%Y-%m-%d').date()
                if date_end <= date_start or date_pred <= date_end or date_pred <= date_start:
                    self._set_error_message("Incompatible date ranges")                                       
                    return redirect(url_for('_index'))
                else:
                    self.__dictionary_set('date_ranges', 'valid', True)
                    self.__dictionary_set('date_ranges', 'start', date_start)
                    self.__dictionary_set('date_ranges', 'end', date_end)
                    self.__dictionary_set('date_ranges', 'pred_end', date_pred)
        self._set_success_message('Dates Accepted')
        return redirect(url_for('_index'))
    
    
        
    def _display_table(self, request):
        """"
        If HTTP requests is POST, retrieve the current region information stored 
        in dictionary, retrieve the DataFrame with the region information,  
        and change the table view to the user-specified selection  
        (HTML button - table_button). IF HTTP requests POST, and table button  
        is empty, then this indicates that the user is instead selecting to 
        switch regions. If the user is switching regions, then update the  
        region in dictionary.  
        If HTTP requests is GET, then create, and return a table according to 
        the current table view, and current region stored in the dictionary. 
        If the DataFrame has been indexed, then the show_index variable will  
        be set to True for displaying dates to the user.
        
        :param request: Data tracker 
        :type request: Request 
        
        :return: Either Flask Redirect for URL, or a Plotly Figure object 
        :rtype: Flask redirect, Plotly Figure 
        """
        if request.method == 'GET':
            page = self.__dictionary_get('page_number', 'number')
            region = self.__dictionary_get('display_table', 'region')
            if (self.__dictionary_get('display_table', 'table_view') == 
                   'COVID19'
                ):             
                df_list = self.__dictionary_get('COVID19_df', 'list')
                df = df_list['df_COVID19_' + region].copy()
                #If DataFrame has been indexed, display index to user by
                #creating a new, and temporary column
                show_index = False
                if self.__dictionary_get('COVID19_df', 'index') == True:
                    show_index = True  
                #Adjust headers
                self.__load_col_headers(df)
                return self.__figure_factory.create_table(
                                                     df,
                                                     region,
                                                     page,
                                                     show_index,
                                                     False,
                                                     False,
                                                     True,
                                                     False
                                                    ).to_html()
            #Create table view of the temperature values for a given
            #state
            elif (self.__dictionary_get('display_table', 'table_view') == 
                     'Temperature'
                 ):  
                    df_list = self.__dictionary_get('Temp_df', 'list')
                    df = df_list['daily_temp_' + region].copy()
                    #If DataFrame has been indexed, display index to user by
                    #creating a new, and temporary column
                    show_index = False
                    if self.__dictionary_get('Temp_df', 'index') == True:
                        show_index = True
                    #Adjust headers 
                    self.__load_col_headers(df)
                    return  self.__figure_factory.create_table(
                                                      df,
                                                      region,
                                                      page,
                                                      show_index,
                                                      False,
                                                      True,
                                                      False,
                                                      False
                                                      ).to_html()
        #User is switching between table views
        if request.method == 'POST':
            #User has selected to view the COVID-19 table
            if request.form.get('table_button') == "COVID19_table":
                region = self.__dictionary_get('display_table', 'region')
                #Obtain the current DataFrame, and then adjust headers
                df_list = self.__dictionary_get('COVID19_df', 'list')
                df = df_list['df_COVID19_' + region]
                self.__load_col_headers(df)    
                self.__dictionary_set('display_table', 'table_view', 'COVID19')
            elif request.form.get('table_button') == "temp_table":
                region = self.__dictionary_get('display_table', 'region')
                #Obtain the current DataFrame, and then adjust headers
                df_list = self.__dictionary_get('Temp_df', 'list')
                df = df_list['daily_temp_' + region]
                self.__load_col_headers(df)    
                self.__dictionary_set('display_table', 'table_view', 'Temperature')
            #Maintain current view, but switch regions
            else:
                region = request.form.get('region')
                self.__dictionary_set('display_table', 'region', region)
                self._clear_messages()                 
            return redirect(url_for('_index'))
    
        
    def _sort_column(self, request):
        if request.method == 'POST':
            if (self.__dictionary_get('display_table', 'table_view') ==
                    'COVID19'
                ):
                df_list = self.__dictionary_get('COVID19_df', 'list')
                for region in df_list:
                    df = df_list[region]
                    df = df.sort_values(by=[request.form.get("column_sort")])
                    df_list[region] = df
            elif (self.__dictionary_get('display_table', 'table_view') == 
                    'Temperature'
                ):
                df_list = self.__dictionary_get('Temp_df', 'list')
                for region in df_list:
                    df = df_list[region]
                    df = df.sort_values(by=[request.form.get("column_sort")])
                    df_list[region] = df
        self._set_success_message('Column Sorted')
        return redirect(url_for('_index'))
    
    def _check_target_var(self, request):
        """Retrieve the input from the user, and check
        to see if input is of float64, or int64. 
        
        :param request: Data tracker 
        :type request: Request 
        
        :return: Redirect for URL
        :rtype: Flask redirect
          
        """
        target = request.form.get("target")
        if target != '':
            if (self.__dictionary_get('display_table', 'table_view')
                == 'COVID19'
            ):
                df_list = self.__dictionary_get('COVID19_df', 'list')
                for region in df_list:
                    df = df_list[region]
                    if(str(df[target].dtypes) !=
                       'float64'
                    ):
                        if(str(df[target].dtypes) !=
                            'int64'
                        ):
                            self._set_error_message(
                                'Incompatible data type for ' +
                                'target variable'
                            )
                            return redirect(url_for('_index')) 
            elif (self.__dictionary_get('display_table', 'table_view')
                  == 'Temperature'
                  ):
                df_list = self.__dictionary_get('Temp_df', 'list')
                for region in df_list:
                    df =  df_list[region]   
                    if(str(df[target].dtypes) !=
                        'float64'
                    ):
                        if(str(df[target].dtypes) !=
                            'int64'
                        ):
                            self._set_error_message(
                                'Incompatible data type for ' +
                                'target variable'
                            )
                            return redirect(url_for('_index')) 
            #Target variable is of the appropriate data type
            self.__dictionary_models_set('target_variable', 'valid', True) 
            self.__dictionary_models_set('target_variable', 'target', target)   
            if (self.__dictionary_get('display_table', 'table_view') ==
                'COVID19'       
            ):
                self.__dictionary_models_set('target_variable', 'target_df', 'COVID19')  
            elif (self.__dictionary_get('display_table', 'table_view') ==
                'Temperature'       
            ):   
                self.__dictionary_models_set('target_variable', 'target_df', 'Temperature')
        self._set_success_message('Target Variable Accepted')
        return redirect(url_for('_index'))
                    
    #Returns True if dates are specified and valid, or False otherwise
    def _dates_set(self):
        for key in self.__dictionary:
            if key['id'] == 'date_ranges':
                if key['valid'] == False:
                    return False
        return True 
    
    def _extend_temp_tables(self):
        if(self.__dictionary_get('date_ranges', 'valid') ==
            False   
        ):
            self._set_error_message("Please valid dates")                                       
            return redirect(url_for('_index'))
        if(self.__dictionary_get('Temp_df', 'index') ==
            False   
        ):
            self._set_error_message("Please index Temperature tables")                                       
            return redirect(url_for('_index'))
        df_list = self.__dictionary_get('Temp_df', 'list')            
        for region in df_list:
            df =  df_list[region]
            dictionary = {}
            dictionary['Date'] = []
            #Dictionary for creating a DataFrame 
            #storing new values
            for col in df:
                dictionary[col]=[]
            #Last date in the DataFrame, and will be
            #incremented until date reaches
            #the end date (prediction end)    
            current = df.index[len(df.index)-1]
            date_pred = None
            for key in self.__dictionary:
                if key['id'] == 'date_ranges':
                    date_pred = key['pred_end']
            while current < date_pred:
                current =  current + timedelta(days=1)
                prev_year = current - timedelta(days=365)
                #Checking to see if the previous
                #year is a leap year. If so, subtract
                #one extra day.
                if ((prev_year.year % 4 == 0) or 
                    (prev_year.year % 100 == 0) or 
                    (prev_year.year % 400 == 0)):
                    if prev_year.month <= 2:
                        prev_year = prev_year - timedelta(days=1)
                #Date for one year before 'current' date
                str_prev_year = ('' + str(prev_year.month) + 
                                 '/' + str(prev_year.day) + 
                                 '/' + str(prev_year.year))

                if ((prev_year in df.index) and 
                    (current.month == prev_year.month and 
                     current.day == prev_year.day)):
                    #Data may be represented as a series, and
                    #value needs to be extracted from the list
                    if isinstance(df.loc[str_prev_year][col], pd.Series):
                        dictionary['Date'].append(current)
                        dictionary[col].append(df.loc[str_prev_year][col][0])
                    else: 
                        #Data is not a series, add value
                        dictionary['Date'].append(current)
                        dictionary[col].append(df.loc[str_prev_year][col])
            df2 = pd.DataFrame.from_dict(data=dictionary)
            df2 = df2.set_index('Date')
            df3 = pd.concat([df, df2], ignore_index= False)
            df3 = df3.sort_index()    
            df_list[region] = df3 
        self._set_success_message('Temperature Tables Extended')
        return redirect(url_for('_index'))
      
      
        
    def _missing_rows(self): 
        """
        Check for missing rows indicated by dates in the date set. 
        For DataFrames containing the target variable, method will use 
        the indicated start date, and indicated end date for the date range. 
        For DataFrames without the target variable, method will use  
        the indicated start date, and indicated prediction end date. 
        
        :return: Return True if there are no missing rows in any of the \
        DataFrames, or return False otherwise
        :rtype: bool
          
        """
        date_start = self.__dictionary_get('date_ranges', 'start')
        date_end = self.__dictionary_get('date_ranges', 'end')
        date_pred = self.__dictionary_get('date_ranges', 'pred_end')
        range_data_set = pd.date_range(start=date_start, end=date_end)
        range_pred_set = pd.date_range(start=date_start, end=date_pred)
        df_list_covid = self.__dictionary_get('COVID19_df', 'list')
        df_list_temp = self.__dictionary_get('Temp_df', 'list')
        if(self.__dictionary_models_get('target_variable', 'target_df') == 
            'COVID19'
        ):  
            #Using the data set range
            for region in df_list_covid:
                df = df_list_covid[region]
                for date1 in range_data_set:
                    date1 = str(date1.date()) 
                    if date1 not in df.index:
                        self._set_error_message(
                            'No row found for ' + str(date1) +
                            ' in COVID-19 table for ' + region
                        )
                        return False
            #Using the prediction set range
            for region in df_list_temp:           
                df = df_list_temp[region]
                for date1 in range_pred_set:
                    date1 = str(date1.date())
                    if date1 not in df.index:
                        self._set_error_message(
                            'No row found for ' + str(date1) +
                            ' in Temperature table for ' + region
                        )
                        return False 

        #Our target variable is in the Temperature DataFrame
        elif (self.__dictionary_models_get('target_variable', 'target_df') == 
            'Temperature'
        ):  
            #Using the prediction range set
            for region in df_list_covid:
                df = df_list_covid[region]
                for date1 in range_pred_set:
                    date1 = str(date1.date()) 
                    if date1 not in df.index:
                        self._set_error_message(
                            'No row found for ' + str(date1) +
                            ' in COVID-19 table for ' + region
                        )
                        return False
            #Using the data set range
            for region in df_list_temp:           
                df = df_list_temp[region]
                for date1 in range_data_set:
                    date1 = str(date1.date())
                    if date1 not in df.index:
                        self._set_error_message(
                            'No row found for ' + str(date1) +
                            ' in Temperature table for ' + region
                        )
                        return False 
        return True
    
    #Check to see if rows in all tables are of
    #numeric types
    def _valid_rows(self):
        df_list_covid = self.__dictionary_get('COVID19_df', 'list')
        df_list_temp = self.__dictionary_get('Temp_df', 'list')
        for region in df_list_covid:
            df = df_list_covid[region]
            for col in df:
                for row in df[col]:
                    if isinstance(row, pd.Series):
                        if isinstance(row.values[0], np.int32) == False:
                            if isinstance(row.values[0], np.float64) == False:    
                                self._set_error_message(
                                    'Incompatible data type found in column ' +
                                    col + ' for ' + region
                                )   
                                return False
                    if type(row) is not int:
                        if type(row) is not float:
                            self._set_error_message(
                                    'Incompatible data type found in column ' +
                                    col + ' for ' + region
                            )  
                            return False
                    
        for region in df_list_temp:
            df = df_list_temp[region]
            for col in df:
                for row in df[col]:
                    if isinstance(row, pd.Series):
                        if isinstance(row.values[0], np.int32) == False:
                            if isinstance(row.values[0], np.float64) == False:    
                                self._set_error_message(
                                    'Incompatible data type found in column ' +
                                    col + ' for ' + region
                                )   
                                return False
                    if type(row) is not int:
                        if type(row) is not float:
                            self._set_error_message(
                                    'Incompatible data type found in column ' +
                                    col + ' for ' + region
                            )  
                            return False 
        return True
    
    
        
    def _tables_partitioned_filled(self):
        return self.__dictionary_get('tables_processed', 'value')          
    
    
      
    def _fill_partition_tables(self, request):
        """
        If HTTP request POST, then check to see the all DataFrames have 
        already been filled, and partitioned. If not, check to see if 
        valid dates have been selected. If so, proceed to slice all DataFrames
        by dates. Slicing range will depend on whether DataFrame contains the
        target variable, or not (Start to prediction end date for DataFrames 
        containing the target variable). Once the DataFrames have been sliced,
        method will fill the DataFrames.
        
        :return: Redirect for URL
        :rtype: Flask redirect
          
        """
        if request.method == 'POST':
            if(self.__dictionary_get('tables_partitioned_filled', 'value') ==
                True
            ):
                self._set_error_message(
                    'Tables have already been partitioned,' +
                    ' and missing rows are filled'
                )                                       
                return redirect(url_for('_index')) 
            if(self.__dictionary_get('date_ranges', 'valid') ==
               False
            ):
                self._set_error_message(
                    'Please select valid dates' +
                    ' before processing tables')                                       
                return redirect(url_for('_index'))
            #Checking to see if the current tables have 
            #been indexed. Index date columns are necessary 
            #for operation
            if(self.__dictionary_get('COVID19_df', 'index') ==
                    False
            ):
                self._set_error_message(
                    'Please index COVID-19 tables' +
                    ' before processing')
                return redirect(url_for('_index'))     
            if(self.__dictionary_get('Temp_df', 'index') ==
                    False
            ):
                self._set_error_message(
                    'Please index Temperature tables' +
                    ' before processing')
                return redirect(url_for('_index'))
            date_start = self.__dictionary_get('date_ranges', 'start')
            date_end = self.__dictionary_get('date_ranges', 'end')
            date_pred = self.__dictionary_get('date_ranges', 'pred_end') 
            df_list_covid = self.__dictionary_get('COVID19_df', 'list')
            df_list_temp = self.__dictionary_get('Temp_df', 'list')
            if(self.__dictionary_models_get('target_variable', 'target_df') ==
               'COVID19'   
            ):
                #Data set range
                for region in df_list_covid:
                    df = df_list_covid[region]
                    df = df.sort_index()
                    df = df.loc[str(date_start):str(date_end)]
                    df = self._fill_dataframe(df)
                    df_list_covid[region] = df
                #Prediction range
                for region in df_list_temp:
                    df = df_list_temp[region]
                    df = df.sort_index()
                    df = df.loc[str(date_start):str(date_pred)]
                    df = self._fill_dataframe(df)
                    df_list_temp[region] = df   
            elif (self.__dictionary_models_get('target_variable', 'target_df') ==
               'Temperature'   
            ):
                #Prediction range
                for region in df_list_covid:
                    df = df_list_covid[region]
                    df = df.sort_index()
                    df = df.loc[str(date_start):str(date_pred)]
                    df = self._fill_dataframe(df)
                    df_list_covid[region] = df
                #Data set range
                for region in df_list_temp:
                    df = df_list_temp[region]
                    df = df.sort_index()
                    df = df.loc[str(date_start):str(date_end)]
                    df = self._fill_dataframe(df)
                    df_list_temp[region] = df 
        self.__dictionary_set('tables_partitioned_filled', 'value', True)           
        self._set_success_message('Tables partitioned, and missing rows filled')
        return redirect(url_for('_index'))
                                    
                
                                                        
    def _fill_dataframe(self, df):
        """Fill in missing rows with the previous non-NULL row.
        DataFrame must be sorted, and indexed by Date type.
        
        :param df: DataFrame which may, or may not contain missing rows \
        within the DataFrame's index range.
        :type df: Pandas DataFrame
        
        :return: DataFrame with no missing date indexes
        :rtype: Pandas DataFrame
        
        """
        for index in range(0, len(df.index) - 1):
            current_date = datetime.strptime(str(df.index[index]), '%Y-%m-%d %H:%M:%S')
            next_date = datetime.strptime(str(df.index[index + 1]), '%Y-%m-%d %H:%M:%S')
            difference_days = (next_date - current_date).days
            if difference_days > 1:
                for num in range(1,difference_days):
                    new_date = current_date + timedelta(days=num)
                    dictionary = {'Date':[new_date]}
                    for col in df:
                        dictionary[col] = []
                        row = df.loc[df.index[index]]
                        dictionary[col].append(row[col])
                        new_df = pd.DataFrame.from_dict(dictionary)
                        new_df = new_df.set_index('Date')
                        df = df.append(new_df)
        return df
            
    
    
    def _index_column(self, request):
        """Index the column specified by the request form in all DataFrames, 
        and according to the current table view.
        
        :param df: DataFrame which may, or may not contain missing rows \
        within the DataFrame's index range.
        :type df: Pandas DataFrame
        
        :return: DataFrame with no missing date indexes
        :rtype: Pandas DataFrame
        
        """
        if request.method == 'POST':
            df_list_covid = self.__dictionary_get('COVID19_df', 'list')
            df_list_temp = self.__dictionary_get('Temp_df', 'list')
            if(self.__dictionary_get('display_table', 'table_view') ==
                'COVID19'    
            ):
                for region in df_list_covid:
                    df = df_list_covid[region]
                    #Check to see if the requested column has the
                    #appropriate data type
                    if df[request.form.get("index_column")].dtype != "datetime64[ns]":
                        self._set_error_message("Can only index columns with 'Date' values")
                        return redirect(url_for('_index')) 
                    #Check for duplicate date values
                    if self._contains_date_duplicates(df, request.form.get("index_column")):
                        self._set_error_message('Duplicate dates found for ' + region)
                        return redirect(url_for('_index')) 
                for region in df_list_covid:
                    df = df_list_covid[region]
                    df = df.set_index(request.form.get("index_column"))
                    df_list_covid[region] = df
                self.__dictionary_set('COVID19_df', 'index', True)
            elif(self.__dictionary_get('display_table', 'table_view') ==
                'Temperature'    
            ):            
                for region in df_list_temp:
                    df = df_list_temp[region]
                    #Check to see if the requested column has the
                    #appropriate data type
                    if df[request.form.get("index_column")].dtype != "datetime64[ns]":
                        self._set_error_message("Can only index columns with 'Date' values")
                        return redirect(url_for('_index')) 
                    #Check for duplicate date values
                    if self._contains_date_duplicates(df, request.form.get("index_column")):
                        self._set_error_message('Duplicate dates found for ' + region)
                        return redirect(url_for('_index')) 
                for region in df_list_temp:
                    df = df_list_temp[region]
                    df = df.set_index(request.form.get("index_column"))
                    df_list_temp[region] = df         
                self.__dictionary_set('Temp_df', 'index', True)        
            self._reset_column_headers()
            self._set_success_message('Column Indexed') 
            return redirect(url_for('_index'))
    
    def _contains_date_duplicates(self, df, column):
        __i = 0
        for __i in range(0, len(df.index)-1):
            if df[column][df.index[__i]] == df[column][df.index[__i+1]]:
                return True
        return False
            
    def _drop_nulls(self, request):
        if request.method == 'POST':
            df_list_covid = self.__dictionary_get('COVID19_df', 'list')
            df_list_temp = self.__dictionary_get('Temp_df', 'list')
            if(self.__dictionary_get('display_table', 'table_view') ==
                'COVID19'
            ):
                for region in df_list_covid:
                    df = df_list_covid[region]
                    df = df.dropna(subset=[request.form.get("null_drop")])
                    df_list_covid[region] = df
            elif(self.__dictionary_get('display_table', 'table_view') ==
                'Temperature'
            ):
                for region in df_list_temp:
                    df = df_list_temp[region]
                    df = df.dropna(subset=[request.form.get("null_drop")])
                    df_list_temp[region] = df
            self._reset_column_headers()
            self._set_success_message('NULLs Dropped')       
            return redirect(url_for('_index'))
        
    def _reset_tables(self):          
        """
        Reset all DataFrames to their original state.  
        
        :return: Redirect for URL
        :rtype: Flask redirect
        
        """
        df_list_covid = self.__dictionary_get('COVID19_df', 'list')
        df_list_temp = self.__dictionary_get('Temp_df', 'list')
        df_list_O_covid = self.__dictionary_get('COVID19_df', "original_list")
        df_list_O_temp = self.__dictionary_get('Temp_df', "original_list")
        
        for region in df_list_O_covid:
            df_list_covid[region] = df_list_O_covid[region].copy() 
        for region in df_list_O_temp:
            df_list_temp[region] = df_list_O_temp[region].copy()    
        self.__dictionary_set('tables_partitioned_filled', 'value', False)
        self.__dictionary_set('COVID19_df', 'index', False)
        self.__dictionary_set('Temp_df', 'index', False)
        #Reset our table view column headers 
        self._reset_column_headers()
        self._set_success_message('Tables Reset')
        return redirect(url_for('_index'))
    
    def _drop_column(self, request):
        if request.method == 'POST':
            if(self.__dictionary_get('display_table', 'table_view') ==
               'COVID19'    
            ):
                df_list_covid = self.__dictionary_get('COVID19_df', 'list')
                for region in df_list_covid:
                    df = df_list_covid[region]
                    #Drop the requested columns
                    df = df.drop(columns=[request.form.get("column_drop")])
                    df_list_covid[region] = df
                #Creating columns from one DataFrame. Columns should be
                #consistent across all DataFrames
                self.__dictionary_set('display_table', 'columns', 
                    self._convert_columns_to_JSONformat(
                        df_list_covid[region].columns                                
                    )
                )
            if(self.__dictionary_get('display_table', 'table_view') ==
               'Temperature'    
            ):
                df_list_temp = self.__dictionary_get('Temp_df', 'list')
                for region in df_list_temp:
                    df = df_list_temp[region]
                    df = df.drop(columns=[request.form.get("column_drop")])
                    df_list_temp[region] = df
                #Creating columns from one DataFrame. Columns should be
                #consistent across all DataFrames
                self.__dictionary_set('display_table', 'columns', 
                    self._convert_columns_to_JSONformat(
                        df_list_temp[region].columns                                
                    )
                )   
            self._set_success_message('Column Dropped')       
            return redirect(url_for('_index'))   
    
    def _page_number(self, request):
        if request.method == 'POST':
            num = self.__dictionary_get('page_number', 'number')
            if request.form.get('page_button') == "increase":
                self.__dictionary_set('page_number', 'number', num + 150)    
            elif request.form.get('page_button') == "decrease":
                if(self.__dictionary_get('page_number', 'number') ==
                    0
                ):   
                    return redirect(url_for('_index'))     
                else:
                    self.__dictionary_set('page_number', 'number', num - 150) 
            return redirect(url_for('_index'))
                             
    def _set_error_message(self, text):
        self.__dictionary_set('success', 'message', '')
        self.__dictionary_set('error', 'message', str(text))

                 
    def _set_success_message(self, text):
        self.__dictionary_set('success', 'message', str(text))
        self.__dictionary_set('error', 'message', '')
        
    def _clear_messages(self):
        self.__dictionary_set('success', 'message', '')
        self.__dictionary_set('error', 'message', '')
                                 
    def _get_error_message(self, request):
        if request.method == 'GET':
            return self.__dictionary_get('error', 'message')
    
    def _get_success_message(self, request):
        if request.method == 'GET':
            return self.__dictionary_get('success', 'message')
                 
    def _get_data_set_range(self):
        if(self.__dictionary_get('date_ranges', 'valid') ==
            False
        ):
            return ''
        else:
            start = self.__dictionary_get('date_ranges', 'start')
            end = self.__dictionary_get('date_ranges', 'end')
            return ' ' + str(start) + ' to ' + str(end)
                
    def _get_prediction_range(self):
        if(self.__dictionary_get('date_ranges', 'valid') ==
            False
        ):
            return ''
        else:
            pred_end = self.__dictionary_get('date_ranges', 'pred_end')
            return ' ' + str(pred_end)
      
    def _get_target_variable(self):
        if(self.__dictionary_models_get('target_variable', 'valid') == 
            False
        ):
            return ''
        else:
            tar = self.__dictionary_models_get('target_variable', 'target')
            return str(tar) 
                                         
    #Check to see if all DataFrames have been indexed          
    def _check_indexes(self):
        if(self.__dictionary_get('COVID19_df', 'index') ==
            False
        ):
            return False
        elif (self.__dictionary_get('Temp_df', 'index') ==
            False
        ):
            return False    
        return True 
    
    def _has_nulls(self, df, column):
        df_bool = pd.isna(df[column])
        for bool1 in df_bool:
            if bool1 is True:
                return True
        return False
    
    def _reset_column_headers(self):
        region = self.__dictionary_get('display_table', 'region')
        if(self.__dictionary_get('display_table', 'table_view') == 
            'COVID19'
        ):
            df_list_covid = self.__dictionary_get('COVID19_df', 'list')
            df = df_list_covid['df_COVID19_' + region]
            self.__load_col_headers(df)
        elif (self.__dictionary_get('display_table', 'table_view') == 
            'Temperature'
        ):
            df_list_temp = self.__dictionary_get('Temp_df', 'list')
            df = df_list_temp['daily_temp_' + region]
            self.__load_col_headers(df)
    
    def _convert_columns_to_JSONformat(self, columns):
        i = 0
        length = len(columns) - 1
        str1 = '{"column_names":['
        column1 = '"' + 'col' + '"'
        for c in columns:
            column_name = '"' + c + '"'
            str1 += '{' + column1 + ':' + column_name + '}'
            if i < length:
                str1 += ','
            i += 1
        str1 =  str1 + ']}'
        return str1;
    
    def __load_col_headers(self, df):
        json_columns = self._convert_columns_to_JSONformat(df.columns)
        self.__dictionary_set('display_table', 'columns', json_columns)
                
    def __dictionary_models_get(self, key_id, key_name):
        for key in self.__dictionary_model:
            if key['id'] == key_id:
                return key[key_name]
            
    def __dictionary_models_set(self, key_id, key_name, value):
        for key in self.__dictionary_model:
            if key['id'] == key_id:
                key[key_name] = value
                
    def __dictionary_get(self, key_id, key_name):
        for key in self.__dictionary:
            if key['id'] == key_id:
                return key[key_name]
            
    def __dictionary_set(self, key_id, key_name, value):
        for key in self.__dictionary:
            if key['id'] == key_id:
                key[key_name] = value 
                            