import matplotlib.pyplot as plt
from flask import Flask, render_template
import pandas as pd
import plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from google.cloud import bigquery
import os

# Set default settings
plt.rcParams["figure.figsize"] = [7.50, 3.50]
plt.rcParams["figure.autolayout"] = True

# Setting Google Credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'dags/ServiceKey_GoogleCloud.json'

# Creating a BigQuery client
bigquery_client = bigquery.Client()

# Fetching the data from BigQuery
query = "SELECT * FROM Forex_Platform.rates;"

# Retrieving the results from BigQuery
query_job = bigquery_client.query(query)
results = query_job.result()

# Creating a Pandas DataFrame
df = results.to_dataframe()

# Extracting the dates and Symbols
dates = df.date.unique()
symbols = df.symbol.unique()

app = Flask(__name__)

@app.route('/')
@app.route('/home')
def home():
    return render_template('home.html', title = 'home')

@app.route('/')
@app.route('/plots')
def plots():
    return render_template('plots.html', title = 'plots', symbols= symbols)


# Web Page #1 - Plot a line chart according to the symbol in the URL
@app.route('/plots/<symbol>')
def plot_png(symbol):

    
    # Extract the rates of the symbol in the URL
   rates = df[df.symbol == symbol].rate
   
   # Create a figure with 4 graphs
   fig = make_subplots(rows=1, cols=2)

   # Populate the figure with Scatter, Histogram
   fig.add_trace(go.Scatter(x=dates, y=rates, name='Line Plot'), row=1, col=1)
   fig.add_trace(go.Histogram(x=rates, name = 'Histogram', histnorm = 'probability'), row=1, col=2)

   # Customizing Plots
   # Customize the appearance of the histogram
   fig.update_layout(
    title_font_size=20,  # Increase title font size
    xaxis_title_font_size=16,  # Increase x-axis title font size
    yaxis_title_font_size=16,  # Increase y-axis title font size
    barmode="overlay",  # Display bars on top of each other
    bargroupgap=0.01,  # Reduce gap between bar groups
    xaxis_tickangle=-45,  # Rotate x-axis tick labels
    plot_bgcolor="#E5E5E5",  # Set plot background color
    paper_bgcolor="#E5E5E5",  # Set paper background color
    legend_title_font_size=16,  # Increase legend title font size
    legend_font_size=14  # Increase legend font size
)

   ## Adding title, xaxis and yaxis
   fig.update_layout(title = f'Symbol: USD vs {symbol}', xaxis_title = 'Date', yaxis_title = 'Rate')
   
   ## Adding legend
  
   # Set the size of the figure
   fig.update_layout(width=1200, height=600)

   # Plot figure and render in HTML
   plot_html = plotly.offline.plot(fig, include_plotlyjs=True, output_type='div')

   return render_template('symbol_plot.html', plot=plot_html, title = 'Symbol Dashboard')


if __name__ == '__main__':
    app.run(debug=True)
