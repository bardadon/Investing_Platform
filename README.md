# Investing Platfrom

Hello!

Welcome to Bar's Forex Investing Platform.

The Forex rates were extracted from Fixer.io using their awesome API.

The platform was first populated with two months worth of Forex rates using the dag: "populating_platform". 

After the first batch of data, the pipeline "insert_new_rates" is in charge of inserting the latest EOD(end of day) data.

All the rates are compared vs a US Dollar base rate. 

## Platform Architecture
![Untitled Diagram drawio(3)](https://user-images.githubusercontent.com/65648983/210087841-a6789329-e4ad-42ad-b29f-d82525b68eb2.png)


### Pipeline #1 - populating_platform
![workflow](https://user-images.githubusercontent.com/65648983/209153010-170cfa40-1cc0-4908-9bd6-1f87e6e01eb1.png)

### Pipeline #2 - insert_new_rates
![workflow_2](https://user-images.githubusercontent.com/65648983/210083958-01878e18-4d56-47ee-8ddf-4a0a140c569c.png)


## The home page
![home](https://user-images.githubusercontent.com/65648983/209152417-dd3d6ad9-1cd4-4425-b26c-6f560913950b.png)


## Available Currencies
![symbols](https://user-images.githubusercontent.com/65648983/209988258-fbde2d07-9627-46b0-96ee-39c5dad45b91.png)


## Interactive Graphs
![Analysis](https://user-images.githubusercontent.com/65648983/209988511-d285aa2b-3fc1-42a0-9c10-ff7ae38da4f1.png)
