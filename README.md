# Investing Platfrom

Hello!

Welcome to my Forex Investing Platform.

The Forex rates were extracted from Fixer.io using their awesome API.

The platform was first populated with two months worth of Forex rates using the dag: "populating_platform". 

After the first batch of data, the pipeline "insert_new_rates" is in charge of inserting the latest EOD(end of day) data.

All the rates are compared vs a US Dollar base rate. 

## Platform Architecture
![Untitled Diagram drawio(5)](https://user-images.githubusercontent.com/65648983/210175503-8af728a4-b000-46c0-9eff-f0b0edd22848.png)



### Pipeline #1 - populating_platform
![workflow](https://user-images.githubusercontent.com/65648983/209153010-170cfa40-1cc0-4908-9bd6-1f87e6e01eb1.png)

### Pipeline #2 - insert_new_rates
![workflow_2](https://user-images.githubusercontent.com/65648983/210083958-01878e18-4d56-47ee-8ddf-4a0a140c569c.png)


## The home page
![Home_page](https://user-images.githubusercontent.com/65648983/210172554-70dd28ba-28fd-4c09-a71d-383157915dd7.png)


## Available Currencies
![available_currencies](https://user-images.githubusercontent.com/65648983/210172561-9f1d6cdd-c3f5-47e2-9492-068c83adbfa0.png)

## Registration/Login Forms
![register_form](https://user-images.githubusercontent.com/65648983/210172619-7b38ae88-6016-4781-bf34-3f2fa69bfbcb.png)
![login_form](https://user-images.githubusercontent.com/65648983/210172620-6d86bd6a-8fe1-41ea-89ce-2184cc1e398e.png)


## Interactive Graphs
![example_analysis](https://user-images.githubusercontent.com/65648983/210172603-2a3abb35-cc82-421a-b8b7-f15d7309fe06.png)

## Medium Article
  1. Pipeline #1 - populating_platform - https://medium.com/@bdadon50/data-engineering-project-creating-an-investing-platform-part-1-e777b5bd27cd
  2. Pipeline #2 - insert_new_rates - https://medium.com/@bdadon50/data-engineering-project-creating-an-investing-platform-part-2-3c41ff7de9df
