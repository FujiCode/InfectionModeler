# InfectionModeler

Langauges: Python, JavaScript/HTML/CSS 

Non-standard Libraries: NumPy, Plotly, Flask, Tensorflow, Keras, Sclearn

This is an application for generating models, and for generating COVID-19 infection forecasts using Tensorflow 
as a Machine Learning framework. Appication will pre-process the user-specified project folder which 
should contain census data, COVID-19 data, and temperature data. 

Application will have the ability to wrangle data such as handling missing data by using data the previous known
date, and extending all temperature tables by using the previous year values. 

Once the data has been properly formatted for modeling, the user may select to generate models (for each state)
based on parameter settings, and run forecasts for predicting future COVID-19 infections. 
