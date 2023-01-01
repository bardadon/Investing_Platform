import matplotlib.pyplot as plt
from flask import render_template, url_for, flash, redirect, request, Flask
from app import app, db, bcrypt
from app.forms import RegistrationForm, LoginForm
from app.models import User, Post
from flask_login import login_user, current_user, logout_user, login_required
from app import db
import plotly
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from app_Helper import get_rates

# Set default settings
plt.rcParams["figure.figsize"] = [7.50, 3.50]
plt.rcParams["figure.autolayout"] = True

# Get rates from BigQuery
df = get_rates()

# Extracting the dates and Symbols
dates = df.date.unique()
symbols = df.symbol.unique()

# Page #1 - Home page
@app.route('/')
@app.route('/home')
def home():
    return render_template('home.html', title = 'home')

# Page #2 - Plots page. Showcase all the currencies
@app.route('/')
@app.route('/plots')
def plots():
    return render_template('plots.html', title = 'plots', symbols= symbols)


# Page #3 - Single plot page. Showcase various analytics for a single currency
@app.route('/plots/<symbol>')
def plot_png(symbol):
    
    # Extract the rates of the symbol in the URL
   rates = df[df.symbol == symbol].rate
   
   # Create a figure with 2 columns
   fig = make_subplots(rows=1, cols=2)

   # Populate the figure with Scatter, Histogram
   fig.add_trace(go.Scatter(x=dates, y=rates, name='Line Plot', ), row=1, col=1)
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
    plot_bgcolor="#d6f0f7",  # Set plot background color
    paper_bgcolor="#d6f0f7",  # Set paper background color
    legend_title_font_size=16,  # Increase legend title font size
    legend_font_size=14  # Increase legend font size
)

   ## Adding title, xaxis and yaxis
   fig.update_layout(title = f'Symbol: USD vs {symbol}', xaxis_title = 'Date', yaxis_title = 'Rate')
     
   # Set the size of the figure
   fig.update_layout(width=1200, height=600)

   # Plot figure and render in HTML
   plot_html = plotly.offline.plot(fig, include_plotlyjs=True, output_type='div')

   return render_template('symbol_plot.html', plot=plot_html, title = 'Symbol Dashboard')

# Page #5 - Registration Form 
@app.route("/register", methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('home'))
    form = RegistrationForm()
    if form.validate_on_submit():
        hashed_password = bcrypt.generate_password_hash(form.password.data).decode('utf-8')
        user = User(username=form.username.data, email=form.email.data, password=hashed_password)
        db.session.add(user)
        db.session.commit()
        flash('Your account has been created! You are now able to log in', 'success')
        return redirect(url_for('login'))
    return render_template('register.html', title='Register', form=form)


@app.route("/login", methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('home'))
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        if user and bcrypt.check_password_hash(user.password, form.password.data):
            login_user(user, remember=form.remember.data)
            next_page = request.args.get('next')
            return redirect(next_page) if next_page else redirect(url_for('home'))
        else:
            flash('Login Unsuccessful. Please check email and password', 'danger')
    return render_template('login.html', title='Login', form=form)


@app.route("/logout")
def logout():
    logout_user()
    return redirect(url_for('home'))


@app.route("/account")
@login_required
def account():
    return render_template('account.html', title='Account')
