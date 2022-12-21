import io
from flask import Response
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from flask import Flask
import pandas as pd

# Set default settings
plt.rcParams["figure.figsize"] = [7.50, 3.50]
plt.rcParams["figure.autolayout"] = True

# Grab the processed rates DataFrame
df = pd.read_csv('dags/processed_rates.csv', index_col = 'Unnamed: 0')
dates = df.date.unique()

app = Flask(__name__)

@app.route('/')
@app.route('/home')
def home():
    return "Welcome to the Bar's Forex Investing Platform"

@app.route('/')
@app.route('/plots')
def plots():
    return 'Enter a symbol in the URL to check a line chart'


# Web Page #1 - Plot a line chart according to the symbol in the URL
@app.route('/plots/<symbol>')
def plot_png(symbol):

   # Create a figure and an axis objects
   fig = Figure()
   axis = fig.add_subplot(1, 1, 1)

   # Extract the rates of the symbol in the URL
   rates = df[df.symbol == symbol].rate.values

   # Create a line chart
   axis.plot(dates, rates)

   # Output the chart as PNG
   output = io.BytesIO()
   FigureCanvas(fig).print_png(output)

   # Return a rendered response of the PNG image
   return Response(output.getvalue(), mimetype='image/png')

if __name__ == '__main__':
    app.run(debug=True)
