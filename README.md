# big-data-extra-credit

This repository is split up into three code sections: One cleaning process on the British Retail dataset, and two profiling processes on the British Retail dataset and the Kaggle E-commerce dataset. 

## British Retail Cleaning

Cleaned original data by dropping unnecessary columns, removing "bad" rows with extra commas, and removing rows that had negative purchase quantities (indicating a discount)

## British Retail Profiling

Wordcount program to sum up most popular Products by quantity purchased

## Product Categories by Date for E-commerce Dataset 

With this code we are using a wordcount-like program to sum up activity for each product category and sorted by order date. Later we join this data with COVID indicators to create analytics and regressions. The output is split between events (including viewings, cart additions, and purchases) and purchases alone. 