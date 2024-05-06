import dash                     # pip install dash
from dash.dependencies import Input, Output, State
from dash import dcc, html
import plotly.express as px     # pip install plotly==5.2.2

import pandas as pd             # pip install pandas
from flask_sqlalchemy import SQLAlchemy
from flask import Flask
from sqlalchemy import create_engine
import pymysql
import pickle

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
server = Flask(__name__)
app = dash.Dash(__name__, server=server,
                external_stylesheets=external_stylesheets, suppress_callback_exceptions=True)


with server.app_context():
    sqlEngine = create_engine(
        "mysql+pymysql://admin:rsa12345@opvd.ceeldi77ur7c.us-west-2.rds.amazonaws.com", pool_recycle=3600)
    dbConnection = sqlEngine.connect()
    type_num_df = pd.read_sql(
        "select * from openvancoverpolicedata.opvd_type_num_cleaned_data order by year", dbConnection)
    type_num_df.head()
    neighbourhood_num_df = pd.read_sql(
        "select * from openvancoverpolicedata.opvd_neighbourhood_num_cleaned_data order by year", dbConnection)
    month_num_df = pd.read_sql(
        "select * from openvancoverpolicedata.opvd_month_num_cleaned_data order by year", dbConnection)
    hour_num_df = pd.read_sql(
        "select * from openvancoverpolicedata.opvd_hour_num_cleaned_data order by year", dbConnection)
    type_neighbourhood_num_df = pd.read_sql(
        "select * from openvancoverpolicedata.opvd_type_neighbourhood_num_df_cleaned_data order by year", dbConnection)
    districtwise_highcrimerate_df = pd.read_sql(
        "select * from openvancoverpolicedata.opvd_districtwise_highcrimerate", dbConnection)
    opvd_holiday_crime_data_df = pd.read_sql(
        "select * from openvancoverpolicedata.opvd_holiday_crime_data order by df1_year", dbConnection)
    opvd_mostcrime_data_df = pd.read_sql(
        "select * from openvancoverpolicedata.opvd_mostcrime_data order by year", dbConnection)
    dbConnection.close()

app.layout = html.Div([
    html.H1("Analytics Dashboard of Vancover Police Department (Dash Plotly)", style={
            "textAlign": "center"}),
    html.Hr(),
    html.Div(children=[

        html.Span(children=[
            html.P("Enter Year:"),
            html.Div(
                dcc.Input(
                    id="input_year",
                    type="number",
                    placeholder="year",
                ))
        ], style={"display": "inline-block", "margin": "5px"}),

        html.Span(children=[
            html.P("Enter Month:"),
            html.Div(
                dcc.Input(
                    id="input_month",
                    type="number",
                    placeholder="month",
                )),
        ], style={"display": "inline-block", "margin": "5px"}),

        html.Span(children=[
            html.P("Enter Hour:"),
            html.Div(
                dcc.Input(
                    id="input_hour",
                    type="number",
                    placeholder="hour",
                )),
        ], style={"display": "inline-block", "margin": "5px"}),

        html.Span(children=[
            html.P("Enter Neighbourhood:"),
            html.Div(
                dcc.Input(
                    id="input_neighbourhood",
                    type="text",
                    placeholder="neighbourhood",
                )),
        ], style={"display": "inline-block", "margin": "5px"}),

        html.Span(children=[
            html.P("Enter Hundred Block:"),
            html.Div(
                dcc.Input(
                    id="input_hundred_block",
                    type="text",
                    placeholder="hundred block",
                )),
        ], style={"display": "inline-block", "margin": "5px"}),

    ], className="inline"),

    html.Br(),
    html.Button('Submit', id='submit-val', n_clicks=0,
                style={"display": "inline-block", "margin": "5px"}),
    html.Hr(),
    html.Div(id="predicted-div", children=[]),

    html.Hr(),
    html.P("Choose Year:"),
    html.Div(html.Div([
        dcc.Dropdown(id='crime-type', clearable=False,
                     value=2003,
                     options=[{'label': x, 'value': x} for x in
                              type_num_df.sort_values(by=['year'])["year"].unique()]),
    ], className="two columns"), className="row"),
    html.Hr(),
    dcc.Interval(
        id='interval-component',
        interval=1*1000*3600*60,  # in milliseconds
        n_intervals=0
    ),
    html.Div(id="output-div", children=[]),
])


@app.callback(Output(component_id="predicted-div", component_property="children"),
              Input('submit-val', 'n_clicks'),
              State('input_year', 'value'),
              State('input_month', 'value'),
              State('input_hour', 'value'),
              State('input_neighbourhood', 'value'),
              State('input_hundred_block', 'value'),
              )
def make_prediction(submit_val, input_year, input_month, input_hour, input_neighbourhood, input_hundred_block):
    if input_year != None and input_month != None and input_hour != None and input_neighbourhood != None and input_hundred_block != None:
        pickled_model = pickle.load(open('model.pkl', 'rb'))
        pickled_target_label = pickle.load(open('target_label.pkl', 'rb'))
        pickled_neighbourhood_label = pickle.load(
            open('neighbourhood_label.pkl', 'rb'))
        pickled_hundred_block_label = pickle.load(
            open('hundred_block_label.pkl', 'rb'))
        if input_neighbourhood not in pickled_neighbourhood_label and input_hundred_block not in pickled_hundred_block_label:
            return [
                html.H2("Wrong Input", style={
                    "textAlign": "center"}),
            ]
        predicted_label = pickled_model.predict(
            [[input_year, input_month, input_hour, pickled_neighbourhood_label[input_neighbourhood], pickled_hundred_block_label[input_hundred_block]]])

        key = [k for k, v in pickled_target_label.items() if v ==
               predicted_label]
        return [
            html.H2(key[0].capitalize(), style={
                "textAlign": "center"}),
        ]
    else:
        return [
            html.H2("", style={
                "textAlign": "center"}),
        ]


@app.callback(Output(component_id="output-div", component_property="children"),
              Input(component_id="crime-type", component_property="value"),
              Input(component_id="interval-component",
                    component_property="n_intervals")
              )
def make_bars(year_given, interval):
    # HISTOGRAM
    type_num_df_hist = type_num_df[type_num_df.year == year_given]
    type_num_df_hist.head()
    fig_hist = px.histogram(type_num_df_hist, y="type", x='count', text_auto=True,
                            title="Total Number of Crime By Type")
    fig_hist.update_yaxes(categoryorder="total ascending")

    # HISTOGRAM
    neighbourhood_num_df_hist = neighbourhood_num_df[neighbourhood_num_df.year == year_given]
    fig_strip = px.histogram(neighbourhood_num_df_hist, x="neighbourhood", y="count",
                             text_auto=True, title="Total Crime By Neighbourhood")
    fig_strip.update_xaxes(categoryorder="total descending")

    # HISTOGRAM
    type_neighbourhood_num_df_hist = type_neighbourhood_num_df[
        type_neighbourhood_num_df.year == year_given]
    fig_stacked = px.bar(type_neighbourhood_num_df_hist, x="neighbourhood", y="count",
                         color="type", title="Stacked Type of Crime by Neighbourhood")

    # # LINE CHART
    month_num_df_line = month_num_df[month_num_df.year == year_given].sort_values(
        by='month')
    fig_line_month = px.line(month_num_df_line, x="month", y="count",
                             markers=True, text="count", title="Total Crime By Month")
    fig_line_month.update_traces(textposition="bottom right")

    # # LINE CHART
    hour_num_df_line = hour_num_df[hour_num_df.year ==
                                   year_given].sort_values(by='hour')
    fig_line_hour = px.line(hour_num_df_line, x="hour", y="count",
                            markers=True, text="count", title="Total Crime By Hour")
    fig_line_hour.update_traces(textposition="bottom right")

    # Histogram
    fig_histogram = px.histogram(districtwise_highcrimerate_df, x="District", y=["2020", "2021", "2022"], barmode='group', title="Total Crime Count Per District for the years 2020-2022")
    
    fig_multiline = px.line(opvd_holiday_crime_data_df, x="df1_year", y="sum(counts)", color='holiday')
    
    fig_histogram_block = px.histogram(opvd_mostcrime_data_df, x="hundred_block", y="count_sum", barmode='group', color='year', title="Total Crime Count Per Hundred Block for the years 2020-2022")

    return [
        html.Div([
            html.Div([dcc.Graph(figure=fig_hist)], className="six columns"),
            html.Div([dcc.Graph(figure=fig_strip)], className="six columns"),
        ], className="row"),
        html.Div([
            html.Div([dcc.Graph(figure=fig_stacked)],
                     className="twelve columns"),
        ], className="row"),
        html.Div([
            html.Div([dcc.Graph(figure=fig_line_month)],
                     className="twelve columns"),
        ], className="row"),
        html.Div([
            html.Div([dcc.Graph(figure=fig_line_hour)],
                     className="twelve columns"),
        ], className="row"),
        
        html.Div([
            html.Div([dcc.Graph(figure=fig_histogram)], className="six columns"),
            html.Div([dcc.Graph(figure=fig_histogram_block)], className="six columns"),
        ], className="row"),
        
        html.Div([
            html.Div([dcc.Graph(figure=fig_multiline)]),
        ], className="row"),
        
        html.H5("The Folium Map depicts the most Crime Hit Hundred_Blocks in District 2(The Least Total Crime Count district since 2020-2022)"),
        html.Div([
            html.Iframe(id='map', srcDoc=open("District2Map.html",
                        "r").read(), width='80%', height='600')
        ], className="row"),
        
    ]


if __name__ == '__main__':
    app.run_server(debug=True)
