import pandas as pd
from Common import utils
from flask import redirect, url_for
from Main import Forecast, Data
from datetime import datetime, timedelta 
from sklearn.preprocessing import MinMaxScaler

class ForecastFlask:
    """Class for running forecasts. After class has been instantiated, call
    to _run_forecast will generate the forecasts.
        
    :param dictionary: Current region in dictionary_models(id - display)
    :type dictionary: Dictionary
    
    :param dictionary_model: Current region in dictionary_models(id - display)
    :type dictionary_model: Dictionary
    
    :param states_abr_list: Current region in dictionary_models(id - display)
    :type states_abr_list: String
    
    """    
    def __init__(self, dictionary, dictionary_model, states_abr_list):

        self.__dictionary = dictionary
        self.__dictionary_model = dictionary_model
        self.__states_abr_list = states_abr_list
        self.__data_factory = Data.DataProcessor()
        
    def _run_forecast(self, request):
        """"Method for running forecasts. Using forecast_days 
        (retrieved from dictionary_models), 
        the list of models (model_list_US), target
        variable (target), states abbreviation list (states_abr_list),
        merged DataFrame set (merge_df_set), start date (start_date),
        T (T), end date for data set (data_set_end), horizon (horizon),
        and tensor structure (tensor_structure), the method will generate
        forecasts by calling an instance of the Forecast class. 
        
        :param request: Data tracker 
        :type request: Request 
        """
        if self.__dictionary_models_get('model_list', 'valid') == False:
            self._set_error_message('Please generate models')
            return redirect(url_for('_index'))
        forecast_days = request.form.get('forecast_days')
        if forecast_days == '':
            self._set_error_message('Please select number of forecast days')
            return redirect(url_for('_index'))
        try:
            forecast_days = int(forecast_days)
        except:      
            self._set_error_message('Please select a valid number for forecast days')
            return redirect(url_for('_index'))
        if forecast_days < 0:
            self._set_error_message('Please select a number greater than 0 for forecast days')
            return redirect(url_for('_index'))
        data_set_end = self.__dictionary_get('date_ranges', 'end')
        data_pred_end = self.__dictionary_get('date_ranges', 'pred_end')
        if (data_set_end + timedelta(days=forecast_days)) > data_pred_end:
            self._set_error_message('Forecast cannot exceed the prediction range')
            return redirect(url_for('_index'))
        f = Forecast.Forecast(
                target = self.__dictionary_models_get(
                                   'target_variable', 'target'   
                ),
                states_abr_list =  self.__states_abr_list, 
                model_list_US = self.__dictionary_models_get(
                                   'model_list', 'list'
                ), 
                merge_df_set = self.__dictionary_models_get(
                                   'merged_df', 'df'    
                ),
                start_date = self.__dictionary_get(
                                   'date_ranges', 'start'   
                ), 
                T = self.__dictionary_models_get(
                                   'parameters', 't'  
                ), 
                forecast_days = forecast_days,
                data_set_end = self.__dictionary_get(
                                   'date_ranges', 'end'   
                ), 
                horizon =  self.__dictionary_models_get(
                                   'parameters', 'horizon'  
                ), 
                tensor_structure = self.__dictionary_models_get(
                                   'tensor_structure', 'tensor_structure' 
                )
            ) 
        forecast_df_list = f._get_forecast_df_list()
        
        #Obtain a DataFrame from forecast list.
        #This DataFrame will be use to create
        #columns, and obtains dates for 
        #creating a DataFrame for the US. 
        #Column names and dates should be consistent
        #across all DataFrames in the forecast list.
        forecast_end_date = self.__dictionary_get('date_ranges', 'end')
        forecast_end_date = datetime.strptime(str(forecast_end_date), '%Y-%m-%d') 
        forecast_end_date = forecast_end_date + timedelta(days=int(forecast_days)) 
        df_forecast_AL = forecast_df_list['AL_forecast'][:str(forecast_end_date)]
        US_total_df = {}
        US_total_df['date'] = []
        date_set = set()
        column_set = set() 
        for col_name in df_forecast_AL.columns:
            column_set.add(col_name)
            US_total_df[col_name] = []
        for date in df_forecast_AL.index:
            date_set.add(date)
        #A union of all DataFrames for
        #each individual state. 
        __df_union_forecast = self.__data_factory._create_US_df(
                US_total_df, forecast_df_list, 
                date_set, column_set, True, self.__states_abr_list 
        ) 
        US_total_df = pd.DataFrame.from_dict(US_total_df)
        US_total_df = US_total_df.set_index('date') 
        US_total_df = US_total_df.sort_index()
        
        self.__dictionary_models_set(
             'forecast_df', 'df_US_total', US_total_df
        )
        self.__dictionary_models_set(
             'forecast_df', 'df_list', forecast_df_list
        )
        self.__dictionary_models_set(
             'forecast_df', 'valid', True
        )  
        self._create_eval()
        self.__dictionary_models_set('display', 'region', 'US')        
        self.__dictionary_models_set('display', 'df', US_total_df)
        self.__dictionary_models_set('display', 'df_union', __df_union_forecast)    
        self._set_success_message('Forecasts Generated')
        return redirect(url_for('_index'))
     
    def _create_eval(self):
        """"Method for creating evaluations. Evaluations are created by using
        the specified testing range (from test_start to data_set_end), already
        defined models, and by generating predictions. The evaluation will be stored
        in a dictionary (eval_list).  
        
        """
        eval_list = {}
        model_list = self.__dictionary_models_get(
             'model_list', 'list'
        ) 
        df_list = self.__dictionary_models_get('merged_df', 'df')
        test_start = self.__dictionary_models_get('model_date_set', 'test_start')
        data_set_end = self.__dictionary_get('date_ranges', 'end')
        for state in self.__states_abr_list:
            df = df_list['df_' + state].copy()
            df = df[str(test_start):str(data_set_end)]
            
            y_scaler = MinMaxScaler()
            y_scaler.fit(df[[
                self.__dictionary_models_get(
                    'target_variable', 'target' 
                )    
            ]])
            X_scaler = MinMaxScaler()
            df[df.columns.tolist()] = X_scaler.fit_transform(df)
            
            df_inputs = utils.TimeSeriesTensor(
                dataset = df, 
                target = self.__dictionary_models_get(
                    'target_variable', 'target' 
                ),
                H = self.__dictionary_models_get(
                    'parameters', 'horizon' 
                ),                          
                tensor_structure = self.__dictionary_models_get(
                    'tensor_structure', 'tensor_structure' 
                ),
                freq='d'                         
            )
            model = model_list['model_' + state]
            
            test_predictions = model.predict(df_inputs['X'])
            
            df_evaluation = utils.create_evaluation_df(
                predictions = test_predictions, 
                test_inputs = df_inputs, 
                H = self.__dictionary_models_get(
                    'parameters', 'horizon' 
                ), 
                scaler = y_scaler
            )
            df_evaluation = df_evaluation.set_index('timestamp')
            df_evaluation = df_evaluation.drop(['h'],axis=1)
            eval_list[state + '_eval'] = df_evaluation
        self.__dictionary_models_set('evaluation_list', 'evaluation_list', eval_list)
        self.__dictionary_models_set('evaluation_list', 'valid', True) 
            
    def _set_error_message(self, text):
        self.__dictionary_set('error','message',text)
        self.__dictionary_set('success','message','')
                 
    def _set_success_message(self, text):
        self.__dictionary_set('error','message','')
        self.__dictionary_set('success','message',text)
            
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




