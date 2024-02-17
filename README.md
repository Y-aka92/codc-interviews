# KommatiPara Data Processing

## Overview
This Python script is designed to process and analyze datasets for a company named KommatiPara, which specializes in bitcoin trading. The script merges two datasets: one containing client information and the other containing financial details. The objective is to collate these datasets for a marketing initiative, focusing on clients from specific countries.

## Features
- **Country Filtering**: Selects clients based on specified countries.
- **Data Privacy**: Removes personal identifiers, keeping only email addresses.
- **Financial Data Handling**: Excludes credit card numbers from the financial dataset.
- **Data Joining**: Merges data using the 'id' field.
- **Column Renaming**: Enhances readability by renaming columns.


## Requirements
- Python 3.8
- PySpark
- Pandas

## Usage

Run the script from the command line with the paths to the client and financial information datasets and the countries of interest. For example:

python main_test.py "C:/codc-interviews/data/client_info.csv" "C:/codc-interviews/data/financial_info.csv" "Netherlands,United Kingdom"

This command will process the datasets based on the specified parameters and save the output in the 'client_data' directory.


## Output Details
The script outputs a CSV file named `final_output.csv` in the 'client_data' directory. The file contains the following columns:
- `client_identifier`: A unique identifier for each client.
- `email`: Email address of the client.
- `bitcoin_address`: Bitcoin address associated with the client.
- `credit_card_type`: Type of credit card held by the client.
- `country`: Country of residence of the client.

The final output is a CSV file named 'final_output.csv', located in the 'client_data' directory. This file contains the processed data with selected fields and renamed columns for better understanding and readability.