from Main import Figure, Models
from flask import redirect, url_for
from datetime import datetime 


class ModelsFlask:
    
    def __init__(self, dictionary, dictionary_model, states_list):
        self.__dictionary = dictionary
        self.__dictionary_model = dictionary_model
        self.__states_list = states_list
        self.__fp = Figure.FigureProcessor()
    
        
    def _check_v_t_dates(self, request):
        """Method will receive request data assign to valid_start, and
        test_start. The method will then convert these String objects
        into DateTime objects for checking that the test_start date
        occurs after the valid_start date. If the dates are valid, then
        the dates are stored in dictionary_models.
         
        :param request: Data tracker 
        :type request: Request 
        
        :return: Redirect for URL
        :rtype: Flask redirect
        """
        if request.form.get('valid_start') == '' or request.form.get('test_start') == '':
            self._set_error_message('Please select valid start date')
            return redirect(url_for('_index')) 
        valid_start  = datetime.strptime(request.form.get('valid_start'), '%Y-%m-%d').date()
        test_start = datetime.strptime(request.form.get('test_start'), '%Y-%m-%d').date()
        data_set_start = None
        data_set_end = None
        #pred_end = None
        for key in self.__dictionary:
            if key['id'] == 'date_ranges':
                data_set_start = key['start']
                data_set_end = key['end']
                #pred_end = key['pred_end'] 
        if test_start == valid_start or test_start < valid_start:
            self._set_error_message('Test start date cannot be less than'
                                    + ' valid start date')
            return redirect(url_for('_index'))
        if test_start <= data_set_start or valid_start <= data_set_start:
            self._set_error_message('Test, and valid start date must be greater than'
                                    + ' the data set start date')
            return redirect(url_for('_index'))
        if test_start >= data_set_end or valid_start >= data_set_end:
            self._set_error_message('Test, and valid start date must be less than'
                                    + ' the data set end date')
            return redirect(url_for('_index'))
        if test_start == valid_start:
            self._set_error_message('Valid date cannot be equal to test date')
            return redirect(url_for('_index'))
        for key in self.__dictionary_model:
            if key['id'] == 'model_date_set':
                key['valid_start'] = valid_start
                key['test_start'] = test_start
                key['valid_dates'] = True
        self._set_success_message('Dates accepted')
        return redirect(url_for('_index'))
    
    def _check_parameters(self, request):
        """Method will receive request data:epochs, horizon,
        batch_size, and t. A check is done to ensure that all data
        are of type int, and all data are greater than zero. If all
        data are of type int, and greater than zero, then method will store
        data in dictionary_models.
         
        :param request: Data tracker 
        :type request: Request 
        
        :return: Redirect for URL
        :rtype: Flask redirect
        """
        epochs = request.form.get('epochs')
        horizon = request.form.get('horizon')
        batch_size = request.form.get('batch_size')
        t = request.form.get('t')
        try:
            int(epochs)
            int(horizon)
            int(batch_size) 
            int(t)
        except ValueError:
            self._set_error_message('Parameters must all be integers')
            return redirect(url_for('_index'))
        epochs = int(epochs)
        horizon = int(horizon)
        batch_size = int(batch_size) 
        t = int(t)
        if epochs <= 0 or horizon <= 0 or batch_size <= 0 or t <= 0:
            self._set_error_message('Parameters must all be greater than zero')
            return redirect(url_for('_index'))  
        for key in  self.__dictionary_model:
            if key['id'] == 'parameters':
                key['valid'] = True
                key['epochs'] = epochs
                key['horizon'] = horizon
                key['batch_size'] = batch_size
                key['t'] = t 
        self._set_success_message('Parameters accepted')
        return redirect(url_for('_index'))
    
    def _display(self, request):
        """If HTTP requests is POST, method will obtain the current
        region information, and obtain the request data from display_button.
        The data from display_button may come in the form of heatmap, graph, table,
        eval_table, and eval_graph. Depending on the display_button data, 
        dictionary_models will be adjusted to the data. Additionally, if
        either heat-map, graph, or table is selected, then the evaluation list
        from dictionary_models will be set to False (under clicked key) to ensure that
        the heat-map, graph, or table does not use evaluation data. 
        If HTTP requests is GET, method will retrieve the target variable,
        DataFrame for each individual state (df), union set of all DataFrames (df_all),
        and region information. Using the retrieved information, either a heat-map,
        graph, table, evaluation table, or evaluation graph is created, and returned
        depending on the current display type in dictionary_models.    
         
        :param request: Data tracker 
        :type request: Request 
        
        :return: Either Flask Redirect for URL, or a Plotly Figure object  
        :rtype: Flask redirect, Plotly Figure 
        """
        if request.method == 'GET':
            tar = self.__dictionary_models_get('target_variable', 'target')
            df = self.__dictionary_models_get('display', 'df')
            #df_all is used for creating the heat-map only. It contains
            #a union of all DataFrames.
            df_all = self.__dictionary_models_get('display', 'df_union')
            region = self.__dictionary_models_get('display', 'region')
            if self.__dictionary_models_get('display', 'display_type') == (
                'heatmap'         
            ):
                return self.__fp.create_heatmap(df_all, 
                                                tar,
                                                df_all['state'],
                                                df_all['date']
                ).to_html() 
            elif (self.__dictionary_models_get('display', 'display_type') 
                  == 'graph'):
                    return self.__fp.create_graph(
                        df, region, self.__dictionary_model, False, True
                    ).to_html()
            elif (self.__dictionary_models_get('display', 'display_type') 
                  == 'eval_graph'):      
                    return self.__fp.create_graph(
                        df, region, self.__dictionary_model, True, False
                    ).to_html()
            elif (self.__dictionary_models_get('display', 'display_type') 
                  == ('table')): 
                    page = self.__dictionary_models_get('page_number', 'number')
                    return self.__fp.create_table(
                    df, region, page, True, False, False, True, False                   
                  ).to_html()
            elif (self.__dictionary_models_get('display', 'display_type') 
                  == ('census_table')):      
                    page = self.__dictionary_models_get('page_number', 'number')
                    return self.__fp.create_table(
                        df, region, page, False, False, False, False, True                   
                    ).to_html() 
            elif (self.__dictionary_models_get('display', 'display_type') 
                  == ('eval_table')):      
                    page = self.__dictionary_models_get('page_number', 'number')
                    return self.__fp.create_table(
                        df, region, page, True, True, False, False, False                   
                    ).to_html()                  
        if request.method == 'POST':
            region = self.__dictionary_models_get('display', 'region')
            if request.form.get("display_button") == 'heatmap':
                self.__dictionary_models_set('evaluation_list', 'clicked', False)
                self._set_df(region)
                self.__dictionary_models_set('display', 'display_type','heatmap')
                
            elif request.form.get("display_button") == 'graph':
                self.__dictionary_models_set('evaluation_list', 'clicked', False)
                self._set_df(region)
                self.__dictionary_models_set('display', 'display_type','graph')
                
            elif request.form.get("display_button") == 'table':
                self.__dictionary_models_set('evaluation_list', 'clicked', False)
                self._set_df(region)
                self.__dictionary_models_set('display', 'display_type','table')
                
            elif request.form.get("display_button") == 'census_table':
                self.__dictionary_models_set('evaluation_list', 'clicked', False)
                self._set_census_df(region)
                self.__dictionary_models_set('display', 'display_type','census_table') 
                   
            elif request.form.get("display_button") == 'eval_table':
                if self.__dictionary_models_get('evaluation_list', 'valid') == False:
                    self._set_error_message('Please run a forecast before viewing evaluation tables')
                    return redirect(url_for('_index'))
                self.__dictionary_models_set('evaluation_list', 'clicked', True)
                self._set_df_eval(region)
                self.__dictionary_models_set('display', 'display_type','eval_table')
                
            elif request.form.get("display_button") == 'eval_graph':
                if self.__dictionary_models_get('evaluation_list', 'valid') == False:
                    self._set_error_message('Please run a forecast before viewing evaluation graphs')
                    return redirect(url_for('_index'))
                self.__dictionary_models_set('evaluation_list', 'clicked', True)
                self._set_df_eval(region)
                self.__dictionary_models_set('display', 'display_type','eval_graph')         
            #User selected to switch region views
            else:
                region = request.form.get('region')
                #Update current region in dictionary
                self.__dictionary_models_set('display', 'region', region) 
                #User is currently viewing evaluations. Use evaluation DataFrames 
                #for creating or table, or graph (heat map excluded)
                if self.__dictionary_models_get('evaluation_list', 'clicked') == True:
                    self._set_df_eval(region)   
                elif (self.__dictionary_models_get('display', 'display_type') == 'graph'
                or self.__dictionary_models_get('display', 'display_type') == 
                    'table'
                ):
                    self._set_df(region)
                elif (self.__dictionary_models_get('display', 'display_type') == 
                    'census_table'
                ):
                    self._set_census_df(region)
                    
            self._set_error_message('')
            self._set_success_message('')                
            return redirect(url_for('_index'))
        
          
    def _get_current_column_headers_model(self, request):
        if request.method == 'GET':
            return self.__dictionary_models_get('US_df', 'columns')
                
    def _generate_models(self):
        """Using dictionary_models, method will check to see if
        parameters, dates, a optimizer, features have been selected
        and/or is/are valid. Method will then retrieve parameter inputs,
        dates, the optimizer, features, and a list of column names from
        a sample DataFrame (column names are use for creating the tensor
        structure). A list of models is generated by calling an instance of
        the Model class (from Models file). 
        
        :return: Redirect for URL
        :rtype: Flask redirect
        """
        if self.__dictionary_models_get('parameters', 'valid') == False:
            self._set_error_message(
                'Please select parameters'
            )
            return redirect(url_for('_index'))    
        if self.__dictionary_models_get('model_date_set', 'valid_dates') == False:
            self._set_error_message(
                'Please select valid, and test dates'
            )
            return redirect(url_for('_index')) 
        if self.__dictionary_models_get('optimizer', 'valid') == False:
            self._set_error_message(
                'Please select a optimizer'
            )
            return redirect(url_for('_index')) 
        if self.__dictionary_models_get('features', 'valid') == False:        
            self._set_error_message(
                'Please select features'
            )
            return redirect(url_for('_index'))
                
        #Settings for model training
        dataset = self.__dictionary_models_get('merged_df', 'df')
        test_start_date = self.__dictionary_models_get(
                            'model_date_set', 'test_start'
                          ) 
        valid_start_date = self.__dictionary_models_get(
                            'model_date_set', 'valid_start'
        ) 
        target = self.__dictionary_models_get('target_variable', 'target')
        t = self.__dictionary_models_get('parameters', 't')
        horizon  = self.__dictionary_models_get('parameters', 'horizon')
        optimizer = self.__dictionary_models_get('optimizer', 'optimizer')
        epochs = self.__dictionary_models_get('parameters', 'epochs')
        batch_size = self.__dictionary_models_get('parameters', 'batch_size')
        #Obtaining column names for tensor structure.
        #Column names should be consistent for all
        #DataFrames, so obtaining column names from
        #one is sufficient. 
        throwaway_df_list = self.__dictionary_models_get('merged_df', 'df')
        throwaway_df = throwaway_df_list['df_AL']
        column_list = throwaway_df.columns.tolist()
        #Creating tensor structure for modeling
        tensor_structure = (
            {"X": (range(-t + 1, 1), column_list)}
        ) 
        self.__dictionary_models_set(
            'tensor_structure', 'tensor_structure', tensor_structure
        )
        #Creating models for each individual US state     
        Models.Model(
            dictionary_models = self.__dictionary_model,
            dataset=dataset,  
            tensor_structure = tensor_structure,
            test_start_date = test_start_date, 
            valid_start_date = valid_start_date,
            states_list = self.__states_list,
            target = target,
            t = t,
            horizon = horizon,
            optimizer = optimizer,
            loss ="mse",
            epochs = epochs,
            batch_size = batch_size,
            latent_dim = 5
        )                  
        self._set_success_message('Models generated')
        return redirect(url_for('_index'))     
                
    def _page_number(self, request):
        if request.method == 'POST':
            for key in self.__dictionary_model:
                if key['id'] == 'page_number': 
                    if request.form.get('page_button') == "increase":
                        key['number'] = key['number'] + 150
                    elif request.form.get('page_button') == "decrease":
                        if key['number'] == 0:
                            return redirect(url_for('_index'))
                        else:
                            key['number'] = key['number'] - 150
                    return redirect(url_for('_index'))     
                  
    
    def _set_df(self, region):
        """"Method for updating the DataFrame for creating, and viewing heat-map
        graph, or table. Method will first determine if a forecast has been 
        generated, and if so, retrieve the DataFrame list from the forecast_df set. 
        If not, then retrieve the DataFrame list from the merged_df set. DataFrames
        are created using the DataFrame list, as well as using the region 
        information. Once DataFrame has been created, update the DataFrame for
        id=Display in dictionary_models.
        
        :param region: Current region in dictionary_models(id - display)
        :type region: String  
        """
        #Forecast has yet to be generated, use original data sets for creating
        #heat map, or table, or graph
        if self.__dictionary_models_get('forecast_df', 'valid') == False:
            if region == 'US':
                df = self.__dictionary_models_get('US_df', 'df') 
            else:
                df_list = self.__dictionary_models_get('merged_df', 'df')
                df = df_list['df_' + region]
        #Forecast has been generated, use forecast data sets for creating
        #heat map, or table, or graph
        elif self.__dictionary_models_get('forecast_df', 'valid') == True:
            if region == 'US':
                df = self.__dictionary_models_get('forecast_df', 'df_US_total') 
            else:
                df_list = self.__dictionary_models_get('forecast_df', 'df_list')
                df = df_list[region+'_forecast']
        #Update current DataFrame in dictionary        
        self.__dictionary_models_set('display', 'df', df)
        #Update current region in dictionary
        self.__dictionary_models_set('display', 'region', region)
        
    def _set_census_df(self, region):
        #Obtain the list of DataFrames containing census data for states
        df_census_list = self.__dictionary_models_get('census_list', 'df_list') 
        #Obtain the DataFrame for the specified region
        df = df_census_list['census_' + region]
        #Update current DataFrame in dictionary        
        self.__dictionary_models_set('display', 'df', df)
        #Update current region in dictionary
        self.__dictionary_models_set('display', 'region', region) 
        
    def _set_df_eval(self, region):
        """"Method for updating the DataFrame for creating, viewing 
        and graph, or table. Method will retrieve the evaluation DataFrame
        list, and then the DataFrame using the region information.  
        
        :param region: Current region in dictionary_models(id - display)
        :type region: String  
        """
        if region == 'US':
            df_list = self.__dictionary_models_get(
                'evaluation_list', 'evaluation_list'
            ) 
            #Currently, there is no model for the US to
            #evaluate from, and DataFrame will default to
            #Alabama's evaluation.
            df = df_list['AL_eval']
        else:
            df_list = self.__dictionary_models_get(
                'evaluation_list', 'evaluation_list'
            )
            df = df_list[region + '_eval']
        #Update current DataFrame in dictionary    
        self.__dictionary_models_set('display', 'df', df)
        #Update current region in dictionary
        self.__dictionary_models_set('display', 'region', region)
                       
    def _set_error_message(self, text):
        self.__dictionary_set('error','message',text)
        self.__dictionary_set('success','message','')
                 
    def _set_success_message(self, text):
        self.__dictionary_set('error','message','')
        self.__dictionary_set('success','message',text)
    
    def _select_features(self, request):
        """If HTTP is POST, then request will receive data. This data
        is a selected feature, and a check is done to the feature is not
        the target variable, or has not already been selected. If the
        feature is not the target variable, or has not already been
        selected, then the feature is added to the feature list in
        dictionary_models as well as setting valid equal to
        True (id=features, key=features, key=valid). 
         
        :param request: Data tracker 
        :type request: Request 
        
        :return: Redirect for URL
        :rtype: Flask redirect
        """
        if request.method == 'POST':
            tar_var = None
            for key in self.__dictionary_model:
                if key['id'] == 'target_variable':
                    tar_var = key['target']
            feature = request.form.get('select_features')
            for key in self.__dictionary_model:
                if key['id'] == 'features':
                    for f in key['features']:
                        if f == feature:
                            self._set_error_message('Feature has already been selected')
                            return redirect(url_for('_index'))   
            if feature == tar_var:
                self._set_error_message('Cannot select target variable as a feature')
                return redirect(url_for('_index'))
            for key in self.__dictionary_model:
                if key['id'] == 'features':
                    key['valid'] = True
                    key['features'].append(feature)
            self._set_success_message('Feature added')
            return redirect(url_for('_index'))
    
    def _select_optimizer(self, request):
        for key in self.__dictionary_model:
            if key['id'] == 'optimizer':
                key['valid'] = True
                key['optimizer'] = request.form.get('optimizer') 
        self._set_success_message('Optimizer selected')
        return redirect(url_for('_index'))
    
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