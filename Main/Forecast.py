from sklearn.preprocessing import MinMaxScaler
from Common import utils
import datetime as dt

class Forecast():
    
    def __init__(self, 
                 target, states_abr_list, 
                 model_list_US, merge_df_set,
                 start_date, T, forecast_days,
                 data_set_end, horizon, tensor_structure):
        self.__target = target
        self.__states_abr_list = states_abr_list
        self.__model_list_US = model_list_US
        self.__merge_df_set = merge_df_set
        self.__start_date = start_date
        self.__t = T
        self.__horizon = horizon
        self.__forecast_days = forecast_days
        self.__data_set_end = data_set_end
        self.__tensor_structure = tensor_structure
        self.__forecast_list = self.__list_forecast(self.__target,
                                                    self.__model_list_US,
                                                    self.__merge_df_set,
                                                    self.__tensor_structure,
                                                    self.__forecast_days,
                                                    self.__start_date,
                                                    self.__t,
                                                    self.__horizon,
                                                    self.__data_set_end,
                                                    self.__states_abr_list)
        self.__predictions = None
        
    def __forecast(self, 
                   model, 
                   df_original,
                   tensor_structure, 
                   forecast_days, 
                   start_date, 
                   T,
                   horizon,
                   data_set_end, 
                   count,
                   target):
        """Method will process the given DataFrame as well as
        creating the necessary inputs for generating predictions
        from the given model. The predicted value will be extracted
        from the predictions list (variable predictions), and the
        date for this predicted value will also be extracted. Using the
        original DataFrame (df_original), method will locate the
        appropriate index Date, and change the current stored value
        to the predicted value. Method will recursively call itself, and \
        continue to repeat the above process until variable count is equal
        to number of forecast days (forecast_days). Once the count variable
        reached the number of forecast days, the original DataFrame, which
        should include predicted values, will be returned.
         
        :param model: Model for a given data set
        :type model: Dictionary
        
        :param df_original: DataFrame for a given state 
        :type df_original: Pandas DataFrame
        
        :param tensor_structure: Structure for modeling
        :type tensor_structure: Tensor Structure
        
        :param forecast_days: Number of days to forecast
        :type forecast_days: String
        
        :param start_date: Date to start generating predictions
        :type start_date: String
        
        :param T: Windowing for  
        :type T: int
        
        :param horizon: Variable to predict 
        :type horizon: String
        
        :param t: Windowing set for training  
        :type t: int
        
        :param horizon: Time series frequency
        :type horizon: int
        
        :param data_set_end: Date to stop generating predictions
        :type data_set_end: String
        
        :param count: Number of recursive calls
        :type count: String
        
        :param target: Target variable to predict for 
        :type target: int
        """
        if(count == int(forecast_days)):
            return_df = df_original
            return return_df

        if len(str(start_date)) <= 10:
            shifted_date = dt.datetime.strptime(str(start_date), 
                                           "%Y-%m-%d"
                                            ) - dt.timedelta(days=T - 1)
        else: 
            shifted_date = dt.datetime.strptime(str(start_date), 
                                           "%Y-%m-%d %H:%M:%S"
                                            ) - dt.timedelta(days=T - 1)
        end_date = None

        if len(str(data_set_end)) <= 10:                                    
            end_date = dt.datetime.strptime(str(data_set_end), "%Y-%m-%d") 
        else:
            end_date = dt.datetime.strptime(str(data_set_end), "%Y-%m-%d %H:%M:%S") 
            
        columns = df_original.columns.tolist()
        df_copy = df_original.copy()[(df_original.index>= shifted_date) &
                        (df_original.index <=end_date)][columns]
        
        y_scaler = MinMaxScaler()
        y_scaler.fit(df_copy[[target]])
    
        X_scaler = MinMaxScaler()
        df_copy[columns] = X_scaler.fit_transform(df_copy)
    
        inputs = utils.TimeSeriesTensor(dataset = df_copy, 
                                        target=target, 
                                        H=horizon,
                                        tensor_structure=tensor_structure,
                                        freq="D",
                                        drop_incomplete=True,
                                        forecast = True)
        
        predictions = model.predict(inputs['X'])
        
        df_copy[columns] = X_scaler.inverse_transform(df_copy)
    
        #Obtaining the forecasted numerical value
        #from our DataFrame. The forecasted value
        #is nested in more than one dimensions, so we
        #will need to extract this value to one
        #dimension.
        forecast_value1 = y_scaler.inverse_transform(predictions[-1:])
        forecast_value2 = forecast_value1[0]
        forecast_value3 = forecast_value2[0]
    
        #Obtaining the date for the forecasted
        #numerical value from our DataFrame
        date_forecast1 = df_copy.index[-1:] + dt.timedelta(days=1)
        date_forecast2 = date_forecast1[::-1]
        date_forecast3 = date_forecast2[0]
        date_forecast4 = dt.datetime.strptime(str(date_forecast3), 
                                              "%Y-%m-%d %H:%M:%S")
        #Assign our predicted value at the corresponding date
        df_original.loc[date_forecast4][target] = forecast_value3
    
        count = count + 1
    
        #Adjust the start date of our forecasting 
        #by one day.                               
        if len(str(start_date)) <= 10:
            start_date = dt.datetime.strptime(str(start_date), 
                                           "%Y-%m-%d"
                                            ) + dt.timedelta(days=1)
        else: 
            start_date = dt.datetime.strptime(str(start_date), 
                                           "%Y-%m-%d %H:%M:%S"
                                            ) + dt.timedelta(days=1)
        #Adjust our data set by one day to account for
        #the additional forecasted value                              
        if len(str(data_set_end)) <= 10:                                    
            data_set_end = ( 
                dt.datetime.strptime(str(data_set_end), "%Y-%m-%d")  + dt.timedelta(days=1)
            )
        else:
            data_set_end = ( 
                dt.datetime.strptime(str(data_set_end), "%Y-%m-%d %H:%M:%S") + dt.timedelta(days=1)              
            )                             
                                      
                                      
                                      
        #Recursively call the method, and send our
        #data set through with the forecasted value
        return_df = self.__forecast(model, 
                                    df_original,
                                    tensor_structure, 
                                    forecast_days, 
                                    start_date, 
                                    T,
                                    horizon,
                                    data_set_end, 
                                    count, 
                                    target)
        return return_df
    
    def __list_forecast(self, 
                        target,
                        model_list_US, 
                        merge_df_set,
                        tensor_structure, 
                        forecast_days, 
                        start_date, 
                        T,
                        horizon,
                        data_set_end,
                        states_abr_list):
        
        states_forecast_list = {}
    
        for state in states_abr_list:
            #Obtain the model for a given state
            model = model_list_US['model_'+state]
            #Obtain the DataFrame for a given state
            df = merge_df_set['df_'+state]
            #Generate a list of forecast values for each
            #state
            states_forecast_list[state + '_forecast'] = (
                        self.__forecast(
                            model, 
                            df,
                            tensor_structure, 
                            forecast_days, 
                            start_date, 
                            T,
                            horizon,
                            data_set_end, 
                            0,
                            target
                        ) 
            )
                         
        return  states_forecast_list
    
    def _get_forecast_df_list(self):
        return self.__forecast_list
    
    def _get_predictions(self):
        return self.__predictions