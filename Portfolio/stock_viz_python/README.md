# 📊 **Apple Stock Analysis 2023** 🍏

## 📝 **Project Description:**

In this project, I analyze **Apple Inc. (AAPL) stock data** for the year **2023** using **Yahoo Finance API** (`yfinance` package). The analysis focuses on visualizing the historical stock prices, identifying key trends, and providing insights on Apple's stock performance over the year.

## 🔧 **Topics Covered:**

- **Data Acquisition**: Using `yfinance` to retrieve Apple’s stock data from Yahoo Finance, including historical prices, volume, and adjusted prices.
  
- **Data Preprocessing**: Cleaning and formatting the stock data for analysis by handling missing values and ensuring proper data types.

- **Visualization**:
  - **Stock Price Trends**: Plotting Apple’s stock prices over the year to analyze its performance and trends.
  - **Moving Averages**: Adding and visualizing moving averages to identify long-term trends.
  - **Volume Analysis**: Analyzing trading volumes alongside price trends to gain insights into market behavior.

- **Performance Insights**:
  - Identifying key periods of price growth or decline.
  - Evaluating **historical volatility** and price fluctuations.

## 📈 **Key Features**:
- **Yahoo Finance API** integration for stock data retrieval.
- **Matplotlib & Pandas** for data visualization and analysis.
- Focus on **price trends, moving averages, and trading volumes**.

## 🧩 **Skills Demonstrated**:
- **Financial Data Analysis**: Retrieving and analyzing stock data to identify market trends.
- **Data Visualization**: Creating insightful and informative visualizations to understand stock price movements.
- **Python Programming**: Utilizing libraries such as `yfinance`, `Matplotlib`, and `Pandas` for financial analysis.

## 🧩 **Skills Demonstrated**:

## 📈 **Results**
- [View Project](Portfolio/stock_viz_python/src/stock_analysis.ipynb)

### Stock Price Analysis of Apple (AAPL)

```python
import yfinance as yf
import matplotlib.pyplot as plt

# Download Apple stock data for 2023
stock_data = yf.download('AAPL', start='2023-01-01', end='2023-12-31')

# Plotting the closing prices
plt.figure(figsize=(10,5))
plt.plot(stock_data['Close'], label='AAPL Closing Price')
plt.title('Apple Stock Price (2023)')
plt.xlabel('Date')
plt.ylabel('Price (USD)')
plt.legend()
plt.show()


