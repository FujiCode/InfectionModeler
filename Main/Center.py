from Main import Data, DataFlask, Figure, ModelsFlask, ForecastFlask
import pandas as pd
import plotly.express as px
pd.options.plotting.backend = "plotly"
from flask import Flask, redirect, url_for, request, render_template
from datetime import timedelta
app = Flask(__name__)
app.secret_key = '&h374((s03'


def __dictionary_models_get(key_id, key_name):
    for key in __dictionary_models:
        if key['id'] == key_id:
            return key[key_name]
def __dictionary_models_set(key_id, key_name, value):
    for key in __dictionary_models:
        if key['id'] == key_id:
            key[key_name] = value
def __dictionary_get(key_id, key_name):
    for key in __dictionary:
        if key['id'] == key_id:
            return key[key_name]
def __dictionary_set(key_id, key_name, value):
    for key in __dictionary:
        if key['id'] == key_id:
            key[key_name] = value

@app.route('/')
def _index():
    if __dictionary_get('page', 'page_view') == 'login':
        return render_template('LoginHTML.html')
    elif __dictionary_get('page', 'page_view') == 'file':
        return render_template('FilesHTML.html')
    elif __dictionary_get('page', 'page_view') == 'data':
        row_front = (__dictionary_get('page_number', 'number') 
        )
        row_back = (__dictionary_get('page_number', 'number') 
        ) + 150
        rows = "Rows: " + str(row_front) + " - " + str(row_back)
        return render_template('DataHTML.html',
                               fig = '/display_table', 
                               column_headers = __dictionary_get('display_table', 'columns'),
                               error_message = _get_error_message(),
                               data_set_range = _get_data_set_range(),
                               prediction_range_end = _get_prediction_range(),
                               target_variable = _get_target_variable(),
                               success_message = _get_success_message(),
                               rows = rows,
                               open_options = __dictionary_get('open_options', 'value')
                               )
    elif __dictionary_get('page', 'page_view') == 'model':   
        data_set_start = __dictionary_get('date_ranges', 'start')
        data_set_end = __dictionary_get('date_ranges', 'end')
        prediction_end = __dictionary_get('date_ranges', 'pred_end')
        row_front = __dictionary_models_get('page_number', 'number')
        row_back = __dictionary_models_get('page_number', 'number') + 150
        rows = "Rows: " + str(row_front) + " to " + str(row_back)
        training_range = None
        validation_range = None
        test_range = None
        #Calculating training, validation, and
        #test ranges for visualization purposes
        if __dictionary_models_get('model_date_set', 'valid_dates') == False:
            training_range = ''
            validation_range = ''
            test_range = ''
        elif __dictionary_models_get('model_date_set', 'valid_dates') == True:   
            valid_start = __dictionary_models_get('model_date_set', 'valid_start')
            test_start = __dictionary_models_get('model_date_set', 'test_start')
            training_range = (
                str(data_set_start) + ' to ' + str(valid_start - timedelta(days=1))
            ) 
            validation_range = (
                str(valid_start) + ' to ' + str(test_start - timedelta(days=1))
            )        
            test_range = (
                str(test_start) + ' to ' + str(data_set_end)
            )
        #Displaying valid parameters
        parameters = None
        if __dictionary_models_get('parameters', 'valid') == False:
            parameters = ''    
        elif __dictionary_models_get('parameters', 'valid') == True:
            parameters = (
                'Epochs = ' + str(__dictionary_models_get('parameters', 'epochs')) +
                ', Horizon = ' + str(__dictionary_models_get('parameters', 'horizon')) +
                ', Batch Size = ' + str(__dictionary_models_get('parameters', 'batch_size')) +
                ', t = ' + str(__dictionary_models_get('parameters', 't')) 
        )
        #Displaying optimizer
        optimizer = None
        if  __dictionary_models_get('optimizer', 'valid') == False:
            optimizer = ''   
        elif  __dictionary_models_get('optimizer', 'valid') == True:   
            optimizer = __dictionary_models_get('optimizer', 'optimizer')
        #Displaying error message
        error_message = __dictionary_get('error', 'message')
        #Displaying success message
        success_message = __dictionary_get('success', 'message')
        #Displaying features
        features = None
        if __dictionary_models_get('features', 'valid') == False:
            features = ''
        elif __dictionary_models_get('features', 'valid') == True:
            length = len(__dictionary_models_get('features', 'features'))   
            f_list = __dictionary_models_get('features', 'features')    
            if length == 1:
                features =  f_list[0]
            else: 
                for f in range(0, length - 2):
                    features += str(f) + ', '
                features += f_list[length - 1]     
                   
        return render_template('ModelHTML.html',
                               fig = '/infection_display',
                               column_headers = _get_current_column_headers_model(),
                               data_set_start = data_set_start,
                               data_set_end = data_set_end,
                               prediction_end = prediction_end,
                               training_range = training_range,
                               validation_range = validation_range,
                               test_range = test_range, 
                               parameters = parameters,
                               optimizer = optimizer,
                               features = features,
                               rows = rows, 
                               error_message = error_message,
                               success_message = success_message,
                               open_options = __dictionary_get('open_options', 'value')
        ) 
        
@app.route('/model')
def _modeling():
    __merged_df = {}
    #Set for gathering dates from incoming 
    #DataFrames. Dates will be used for
    #adding data from different DataFrames.
    __date_set = set()
    
    __column_set = set()
    #DataFrame containing information
    #from all states
    __US_total_df = {}
    __US_total_df['date'] = []
    #Merge COVID19 and Temperature DataFrames
    __data_factory._merge_all_dfs(
                       __merged_df,
                       __dictionary_get('COVID19_df', 'list'),
                       __dictionary_get('Temp_df', 'list') 
                    ) 
    #Obtain column names, and dates for
    #creating US DataFrame
    df_AL = __merged_df['df_AL']
    for col in df_AL:
        __column_set.add(col)
        __US_total_df[col] = []
    for date1 in df_AL.index:
        __date_set.add(date1)
    #__df_union is a union of all DataFrames, and will be
    #used for creating the heatmap. __US_total_df will
    #be modified within the '_create_US_df' method.
    __df_union = __data_factory._create_US_df(
                            __US_total_df, __merged_df, 
                            __date_set, __column_set, False,
                            __states_abr)     
    __US_total_df = pd.DataFrame.from_dict(__US_total_df)
    __US_total_df = __US_total_df.set_index('date') 
    __US_total_df = __US_total_df.sort_index()
    #Partitioning data set to include the specified
    #data set end date
    __US_total_df = __US_total_df[:str(__dictionary_get('date_ranges', 'end'))]
    
    __dictionary_models_set('merged_df', 'df', __merged_df)

    __dictionary_models_set('US_df', 'df', __US_total_df)
    __dictionary_models_set('US_df', 'columns', 
                               _convert_columns_to_JSONformat(
                                   __US_total_df.columns
                               )
                            )              
    __dictionary_models_set('display', 'df', __US_total_df)
    __dictionary_models_set('display', 'df_union', __df_union)   
    #Redirect to use model HTML in index method    
    __dictionary_set('page', 'page_view', 'model')
    #Clear message for new page    
    __dictionary_set('error', 'message', '')
    #Clear messages for new page
    __dictionary_set('success', 'message', '')   
    return redirect(url_for('_index'))
  
                      
@app.route('/average_duplicates', methods=['POST'])                
def _average_duplicates():
    return __d_flask._average_duplicates(request)                   

@app.route('/check', methods=['POST'])
def _check():
    return __d_flask._check(request)  

@app.route('/check_data_set_dates', methods=['POST'])
def _check_data_set_dates():
    return __d_flask._check_data_set_dates(request)  

@app.route('/check_target_var', methods=['POST'])
def _check_target_var():
    return __d_flask._check_target_var(request) 

@app.route('/check_parameters', methods=['POST'])
def _check_parameters():
    return __m_flask._check_parameters(request)

@app.route('/check_v_t_dates', methods=['POST'])
def _check_v_t_dates():
    return __m_flask._check_v_t_dates(request)

@app.route('/drop_column', methods=['POST'])   
def _drop_column():     
    return __d_flask._drop_column(request)
    
@app.route('/drop_nulls', methods=['POST'])
def _drop_nulls():
    return __d_flask._drop_nulls(request) 

@app.route('/extend_temp_tables', methods=['POST'])
def _extend_temp_tables():           
    return __d_flask._extend_temp_tables()

@app.route('/fill_partition_tables', methods=['POST'])
def _fill_partition_tables():           
    return __d_flask._fill_partition_tables(request)

@app.route('/get_current_column_headers_model', methods=['GET'])      
def _get_current_column_headers_model():
    return __m_flask._get_current_column_headers_model(request)
             
@app.route('/get_error_message', methods=['GET'])
def _get_error_message():
    return __d_flask._get_error_message(request)

@app.route('/get_success_message', methods=['GET'])
def _get_success_message():
    return __d_flask._get_success_message(request)

@app.route('/get_data_set_range', methods=['GET'])
def _get_data_set_range():
    return __d_flask._get_data_set_range()

@app.route('/get_prediction_range', methods=['GET'])
def _get_prediction_range():
    return __d_flask._get_prediction_range()

@app.route('/get_target_variable', methods=['GET'])
def _get_target_variable():
    return __d_flask._get_target_variable()

@app.route('/generate_models', methods=['Post'])
def _generate_models():
    return __m_flask._generate_models()
    
@app.route('/index_column', methods=['POST'])   
def _index_column():
    return __d_flask._index_column(request) 

@app.route('/login', methods=['POST'])   
def _login():
    user_name = request.form.get('user_name')
    password = request.form.get('password')
    if user_name == 'user1':
        if password == '8&a!nM?29y2u':
            __dictionary_set('page', 'page_view', 'file')
            return redirect(url_for('_index'))
    __dictionary_set('page', 'page_view', 'login')
    return redirect(url_for('_index')) 

@app.route('/open_options', methods=['POST'])   
def _open_options():
    if __dictionary_get('open_options', 'value') == False:
        __dictionary_set('open_options', 'value', True)
    else:
        __dictionary_set('open_options', 'value', False)
    return redirect(url_for('_index'))
          

@app.route('/infection_display', methods=['Post', 'GET'])
def _infection_display():
    return __m_flask._display(request)

@app.route('/reset_tables', methods=['POST'])
def _reset_tables():           
    return __d_flask._reset_tables()

@app.route('/run_forecast', methods=['POST'])
def _run_forecast():           
    return __f_flask._run_forecast(request)

@app.route('/select_features', methods=['Post', 'GET'])
def _select_features():
    return __m_flask._select_features(request)

@app.route('/select_optimizer', methods=['Post', 'GET'])
def _select_optimizer():
    return __m_flask._select_optimizer(request)

@app.route('/sort_column', methods=['POST'])   
def _sort_column():
    return __d_flask._sort_column(request)

@app.route('/display_table', methods=['POST', 'GET'])
def _display_table():
    return __d_flask._display_table(request)

@app.route('/page_number', methods=['POST'])
def __page_number():
    return __d_flask._page_number(request)

@app.route('/page_number_model', methods=['POST'])
def __page_number_model():
    return __m_flask._page_number(request)

@app.route('/process_files', methods=['POST'])
def __process_files():
    project_location = str(request.form.get('project'))
    __COVID_19_file_location = project_location + r'\COVID19Project\COVID19_US\US_COVID19.xlsx'
    __temp_file_location = project_location + r'\COVID19Project\Daily_Temp'
    __census_file_location = project_location + r'\COVID19Project\Census'
    __data_factory_location = Data.DataProcessor()
    try:
        __df_census_list = ( 
             __data_factory._get_states_census_df_list(__census_file_location)
        )
        __dictionary_models_set('census_list', 'df_list', __df_census_list)
        #DataFrame that contains U.S. COVID-19 information from
        #the CDC. 
        #__COVID_19_file = r'C:\Users\jtfuj\Documents\COVID19Project\COVID19_US\US_COVID19.xlsx'
        __US_COVID_19_df_original_list= (
              __data_factory._convert_file_to_dataframe(__COVID_19_file_location)
        )        
        #Single DataFrame for each individual state which that 
        #contains COVID-19 information from the CDC.          
        __COVID19_df = (
            __data_factory._US_df_into_state_dfs (
            __US_COVID_19_df_original_list,
            __states_abr.keys()
           )
        )  
        __dictionary_set('COVID19_df', 'list',  __COVID19_df)
        __dictionary_set('COVID19_df', 'original_list',  __COVID19_df.copy())
        #List containing DataFrames for individual states.
        #DataFrames contain daily temperature values for a 
        #given date, and given weather station within a state.
        __daily_temp_df = ( 
            __data_factory._get_states_daily_temp_df_list(False, __temp_file_location)
        )
        __dictionary_set('Temp_df', 'list',  __daily_temp_df)
        __dictionary_set('Temp_df', 'original_list',  __daily_temp_df.copy())
        __dictionary_set('display_table', 'table',  __figure_factory.create_table(
                                                    __COVID19_df['df_COVID19_AL'], 
                                                   'AL', 
                                                    0, 
                                                    False,
                                                    False,
                                                    False,
                                                    True,
                                                    False
                                                    )
        )
        __dictionary_set('display_table', 'columns', _convert_columns_to_JSONformat(
                                                    __COVID19_df['df_COVID19_AL'].columns
                                                    ) 
        )
        __dictionary_set('page', 'page_view', 'data')
        return redirect(url_for('_index'))
    except:
        return redirect(url_for('_index'))
        
def _convert_columns_to_JSONformat(columns):
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
                                                       
if __name__ == "__main__":
    
    __data_factory = Data.DataProcessor()
    
    __figure_factory = Figure.FigureProcessor() 
    
    __original_df_COVID19 = pd.DataFrame()
    
    __states_abr = {'AL':'Alabama', 'AK':'Alaska', 'AZ': 'Arizona', 
                    'AR':'Arkansas', 'CA':'California', 'CO':'Colorado', 
                    'CT':'Connecticut', 'DE':'Delaware', 'FL':'Florida',
                    'GA':'Georgia','HI':'Hawaii', 'ID':'Idaho', 
                    'IL':'Illinois', 'IN':'Indiana', 'IA':'Iowa', 
                    'KS':'Kansas', 'KY':'Kentucky', 'LA':'Louisiana', 
                    'ME':'Maine', 'MD':'Maryland', 'MA':'Massachusetts', 
                    'MI':'Michigan', 'MN':'Minnesota', 'MS':'Mississippi', 
                    'MO':'Missouri', 'MT':'Montana', 'NE':'Nebraska', 
                    'NV':'Nevada', 'NH':'New Hampshire', 'NJ':'New Jersey', 
                    'NM':'New Mexico', 'NY':'New York', 'NC':'North Carolina', 
                    'ND':'North Dakota', 'OH':'Ohio', 'OK':'Oklahoma', 
                    'OR':'Oregon', 'PA':'Pennsylvania', 'RI':'Rhode Island', 
                    'SC':'South Carolina', 'SD':'South Dakota', 
                    'TN': 'Tennessee', 'TX':'Texas', 'UT':'UTAH',
                    'VT':'Vermont', 'VA':'Virginia', 'WA':'Washington', 
                    'WV':'West Virginia', 'WI':'Wisconsin', 'WY':'Wyoming'}
    
    __dictionary = [
          { 
            'id': 'display_table',
            'region': 'AL',
            'table_view': 'COVID19',
            'table': None,
            'columns': None
          },
          {
            'id':'error',
            'message':''    
          },
          {
            'id':'success',
            'message':''    
          },
          {          
            'id': 'display_end',
            'display_end': None
          },
          {          
            'id': 'COVID19_df',
            'list': None,
            'original_list': None,
            'index': False 
          },
          {          
            'id': 'Temp_df',
            'list': None,
            'original_list': None,
            'index': False
          },
          {
            'id': 'tables_partitioned_filled',
            'value': False    
          },
          { 
            'id': 'page_number',
            'number': 0
          },
          {
            'id': 'page',
            'page_view': 'login'
          },
          {
            #Date ranges for the data set
            'id': 'date_ranges',
            'valid': False,
            'start': None,
            'end': None,
            'pred_end': None    
          },
          {
            'id': 'open_options',
            'value': False    
          }
    ]
    
    __data_page = True
    
    __model_page = False
    
    __dictionary_models = [      
                    {
                        'id': 'target_variable',
                        'valid': False,
                        'target_df': None,
                        'target': None    
                    },
                    {
                        'id': 'merged_df',
                        'df': None    
                    },
                    {
                        'id': 'US_df',
                        'df': None,
                        'columns': None  
                    },
                    {
                        'id': 'forecast_df',
                        'valid': False,
                        'df_list': None,
                        'df_US_total': None,
                        'forecast_days': None  
                    },
                    { 
                        'id': 'display',
                        'region': 'US',
                        'display_type': 'heatmap',
                        'df': None,
                        #Union of all DataFrame
                        #for each individual state.
                        #df_union will be used 
                        #for generating heatmap
                        'df_union': None,
                        'fig': px.choropleth(locations=["CA", "TX", "NY"], 
                                    locationmode="USA-states", 
                                    color=[1,2,3], scope="usa")
                    },
                    { 
                        'id': 'page_number',
                        'number': 0
                    },
                    {
                        'id': 'model_date_set',
                        'valid_dates': False,
                        'valid_start': None,
                        'test_start': None
                    },
                    {
                        'id': 'parameters',
                        'valid': False,
                        'epochs': None,
                        'horizon': None,
                        'batch_size': None,
                        't': None,
                        'loss': None    
                    },
                    {
                        'id': 'optimizer',
                        'valid': False,
                        'optimizer': None
                    },
                    {
                        'id': 'features',
                        'valid': False,
                        'features':[]    
                    },
                    {
                        'id': 'tensor_structure',
                        'tensor_structure': None    
                    },
                    #A list of created models for each
                    #individual US state
                    {
                        'id': 'model_list',
                        'valid': False,
                        'list': None    
                    },
                    {
                        'id': 'evaluation_list',
                        'clicked': False,
                        'valid': False,
                        'evaluation_list': False    
                    },
                    {
                        'id': 'census_list',
                        'df_list': None  
                    }
                ]

    __d_flask = DataFlask.DataPrepFlask(__dictionary, 
                                        __dictionary_models,
                                        __states_abr.keys()
    )
    __m_flask = ModelsFlask.ModelsFlask(__dictionary, 
                                        __dictionary_models, 
                                        __states_abr.keys()
    )
    __f_flask = ForecastFlask.ForecastFlask(__dictionary, 
                                            __dictionary_models,
                                            __states_abr.keys()
    )
    app.debug = False
    app.run()
  

       

    
