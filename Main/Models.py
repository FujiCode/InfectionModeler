import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import pandas as pd
pd.options.mode.chained_assignment = None 
from Common import utils
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.layers import GRU, Dense
from tensorflow.keras.models import Sequential
from sklearn.preprocessing import MinMaxScaler
import datetime as dt

class Model():
    """When this class is instantiated, the class will automatically divide
    the data set into the training (__train_dataset), validation (__valid_dataset),
    and testing (__test_dataset) data set with using the test (test_start_date) and 
    valid start dates (valid_start_date). The training inputs (__train_inputs),
    validation inputs (__valid_inputs), and testing inputs (__test_inputs) are then
    created. Using the training, validation, and testing data set, and using the 
    created inputs, models are created for each individual state. Models (__model), 
    along with their model history (__model_history), are saved in dictionaries.
    
    :param dictionary_models: Dictionary containing information for modeling \
    and forecasting
    :type dictionary_models: Dictionary
    
    :param dataset: Dictionary of DataFrames for each individual state
    :type dataset: Dictionary
    
    :param tensor_structure: Structure for modeling
    :type tensor_structure: Tensor Structure
    
    :param test_start_date: Start date of test data set
    :type test_start_date: String
    
    :param valid_start_date: Start date of valid data set
    :type valid_start_date: String
    
    :param states_list: List of state abbreviations
    :type states_list: List
    
    :param target: Variable to predict
    :type target: String
    
    :param t: Windowing set
    :type t: int
    
    :param horizon: Time series frequency
    :type horizon: int
    
    :param optimizer: Type of algorithm for updating weights and biases
    :type optimizer: String
    
    :param loss: Penalty for an incorrect prediction
    :type loss: String
    
    :param epochs: Number of passes for the training data set
    :type epochs: int
    
    :param batch_size: Number of samples processed before updating model
    :type batch_size: int
    
    :param latent_dim: Compression space
    :type latent_dim: int
    
    """
    
    def __init__(self,
                 dictionary_models, 
                 dataset,  
                 tensor_structure,
                 test_start_date, 
                 valid_start_date,
                 states_list,
                 target,
                 t = 14,
                 horizon=1,
                 optimizer='Adam',
                 loss="mse",
                 epochs= 100,
                 batch_size = 32,
                 latent_dim = 5
                 ):
        self.__dictionary_models = dictionary_models
        #Data set - Dictionary of keys (state name) and values (state's DataFrame)
        self.__dataset = dataset
        self.__tensor_structure = tensor_structure
        self.__test_start_date = test_start_date
        self.__valid_start_date = valid_start_date
        self.__states_list = states_list
        #target - Target variable in string data type
        self.__target = target
        self._t = t
        self.__horizon = horizon
        self.__optimizer = optimizer
        self.__loss = loss
        self.__target = target
        self.__epochs = epochs
        self.__batch_size = batch_size
        self.__latent_dim = latent_dim
        self.__train_dataset = self.__paritioned_dataset(self.__dataset,
                                                         'train',
                                                         self.__test_start_date,
                                                         self.__valid_start_date
                                                         )
        self.__valid_dataset = self.__paritioned_dataset(self.__dataset,
                                                         'valid',
                                                         self.__test_start_date,
                                                         self.__valid_start_date
                                                         )
        self.__test_dataset = self.__paritioned_dataset(self.__dataset,
                                                         'test',
                                                         self.__test_start_date,
                                                         self.__valid_start_date
                                                         )
        self.__train_inputs, self.__scaler_train_list = self.__input(
                                                            self.__train_dataset,
                                                            self.__target,
                                                            self.__horizon,
                                                            self.__tensor_structure
                                                        ) 
        self.__valid_inputs, self.__scaler_valid_list = self.__input(
                                                            self.__valid_dataset,
                                                            self.__target,
                                                            self.__horizon,
                                                            self.__tensor_structure)
        self.__test_inputs, self.__scaler_test_list = self.__input(
                                                            self.__test_dataset,
                                                            self.__target,
                                                            self.__horizon,
                                                            self.__tensor_structure)
        self.__model, self.__model_history = self.__generate_model(
                                                 self.__train_inputs,
                                                 self.__valid_inputs,
                                                 self.__latent_dim,
                                                 self._t,
                                                 self.__horizon,
                                                 self.__optimizer,
                                                 self.__loss,
                                                 self.__batch_size,
                                                 self.__epochs,
                                                 self.__states_list
                                             )
        
    def __generate_model(self,
                          train_inputs,
                          valid_inputs,
                          latent_dim,
                          t,
                          horizon,
                          optimizer,
                          loss,
                          batch_size,
                          epochs,
                          states_list):
        
        model_list = {}
        for state in states_list:
            state_input_tr = train_inputs['input_df_' + state]
            state_input_v = valid_inputs['input_df_' + state]
            
            model = Sequential()
            model.add(GRU(latent_dim, input_shape=(t, 2)))
            model.add(Dense(horizon))
            model.compile(optimizer=optimizer, loss=loss)
            GRU_earlystop = EarlyStopping(monitor="loss", min_delta=0, patience=5)

            model_history = model.fit(
                                      state_input_tr["X"],
                                      state_input_tr["target"],
                                      batch_size=batch_size,
                                      epochs=epochs,
                                      validation_data=(state_input_v["X"], state_input_v["target"]),
                                      callbacks=[GRU_earlystop],
                                      verbose=1
                                     )   
            model_list['model_' + state] = model
        self.__dictionary_models_set('model_list', 'valid', True)
        self.__dictionary_models_set('model_list', 'list', model_list)
        return model, model_history
  
        
    def __paritioned_dataset(self, dataset, 
                                   set_type,
                                   test_start_date,
                                   valid_start_date):
        dataset_train = {}
        test_start_date = dt.datetime.strptime(str(test_start_date), "%Y-%m-%d")
        valid_start_date = dt.datetime.strptime(str(valid_start_date), "%Y-%m-%d")  
        for state in dataset:
            df_state = dataset[state]
            if set_type == 'train':
                df_state = df_state[df_state.index <valid_start_date]
           
            elif set_type == 'valid':
                df_state = df_state[(df_state.index >= valid_start_date) &
                                    (df_state.index < test_start_date)]              
            elif set_type == 'test':
                df_state = df_state[df_state.index > test_start_date]
            dataset_train[state] = df_state
        return dataset_train
            
        
    def __input(self, dataset, target, horizon, tensor_structure):
        input_list = {}
        scaler_list = {}
        for state in dataset:
            df_state = dataset[state]
            y_scaler = MinMaxScaler()
            y_scaler.fit(df_state[[target]])
            X_scaler = MinMaxScaler()
            df_state[df_state.columns] = X_scaler.fit_transform(df_state)
            scaler_list['y_scaler_' + state] = y_scaler
            scaler_list['X_scaler_' + state] = X_scaler
            new_input = utils.TimeSeriesTensor(
                           dataset=df_state,
                           target=target,
                           H=horizon,
                           tensor_structure=tensor_structure,
                           freq="D",
                           drop_incomplete=True,
                           forecast = False
                         )
            input_list['input_' + state] = new_input
        return input_list, scaler_list 
    
    def __dictionary_models_get(self, key_id, key_name):
        for key in self.__dictionary_models:
            if key['id'] == key_id:
                return key[key_name]
            
    def __dictionary_models_set(self, key_id, key_name, value):
        for key in self.__dictionary_models:
            if key['id'] == key_id:
                key[key_name] = value 
          
    def _get_model(self):
        """Retrieve the dictionary list containing models
        
        """
        return self.__model    
    
    def _get_model_history(self):
        """Retrieve the dictionary list containing the model's history
        
        """
        return self.__model_history 
     
    def _get_test_inputs(self):
        return self.__test_inputs
    
    def _get_test_scaler(self):
        return self.__scaler_test_list
        
 
