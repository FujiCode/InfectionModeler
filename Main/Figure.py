import plotly.graph_objects as go
import plotly.express as px

class FigureProcessor():
    
    def __init__(self):
        self.__object = None
        
    
    def create_table(self, df, region, page, display_indexes, evaluation, temp, covid, 
                     census):
        df = df[page:(page+150)].copy()
        values_list = []
        values={}
        if display_indexes == True:
            df['Date'] = df.index
        for e in df:
            values_list.append(df[e])
        values['values'] = values_list
        fig = go.Figure(data=[
                          go.Table( 
                                    header=dict(
                                       values=list(df.columns)
                                    ),
                                    cells=dict(values
                                    )
                         )
                    ]
                    )
        str_title = ''
        if evaluation == True:
            str_title = 'Model Evaluation Table: '
            if region == 'US':
                region = 'AL'
        elif temp == True:
            str_title = 'Temperature Table: '
        elif covid == True:
            str_title = 'COVID-19 Table: '
        elif census == True:
            str_title = 'Census Data: '
        fig.update_layout(
                          title= str_title + region,
                          paper_bgcolor="#FFFFFF"
                        )
        return fig
    
    def create_heatmap(self, df, target, location, animation_column):
        fig = px.choropleth(df, 
                            locations=location, 
                            locationmode='USA-states', 
                            color=target, 
                            scope='usa',
                            color_continuous_scale=["#00f5ff", "#0b0778"],
                            animation_frame=animation_column
                           )
        return fig
        
    def create_graph(self, df, region, dictionary_model, evaluation, covid):
        str_title = ''
        if evaluation == True:
            str_title = 'Model Evaluation Graph: '
            if region == 'US':
                region = 'AL'
        if covid == True:
            str_title = 'COVID-19 Graph: '
        for key in dictionary_model:
            if key['id'] == 'target_variable':
                tar = key['target']
                graph_figure = df.plot()
                graph_figure.update_layout(
                  title= str_title + region,
                  yaxis_title = tar,
                  xaxis_title = 'Date',
                  plot_bgcolor ="#d4d4d4",
                  paper_bgcolor ="#FFFFFF",
                  title_font_color='#032c73'
                )
                return graph_figure