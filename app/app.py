import io
from flask import Response
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from flask import Flask, render_template
import pandas as pd

# Set default settings
plt.rcParams["figure.figsize"] = [7.50, 3.50]
plt.rcParams["figure.autolayout"] = True

# Grab the processed rates DataFrame
df = pd.read_csv('dags/processed_rates.csv', index_col = 'Unnamed: 0')
dates = df.date.unique()
symbols = df.symbol.values

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

   plt.style.use('ggplot')
    
    # Extract the rates of the symbol in the URL
   rates = df[df.symbol == symbol].rate

   # Create a figure and an axis objects
   fig, ax = plt.subplots(2,1, figsize = [15,8])

   # Create a line chart
   ax[0].plot(rates, dates,  marker = 'v', linestyle= '--' ,c = 'r')
   ax[0].set_xlabel('Rate', size = 16)
   ax[0].set_ylabel('Date', size = 16, rotation = 0)
   ax[0].set_title(f'Line Chart - {symbol}', size = 20)

   # Create a histogram plot
   ax[1].hist(rates)
   ax[1].set_xlabel('Rate', size = 16)
   ax[0].set_ylabel('Density', size = 16, rotation = 0)
   ax[1].set_title(f'Histogram - {symbol}', size = 20)
   
   # Output the chart as PNG
   output = io.BytesIO()
   FigureCanvas(fig).print_png(output)

   # Return a rendered response of the PNG image
   return Response(output.getvalue(), mimetype='image/png')

if __name__ == '__main__':
    app.run(debug=True)
