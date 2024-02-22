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
- Python 3.10
- PySpark
- Pandas

## Usage

Run the script from the command line with the paths to the client and financial information datasets and the countries of interest. For example:
```bash
python main.py "C:/codc-interviews/data/client_info.csv" "C:/codc-interviews/data/financial_info.csv" "Netherlands,United Kingdom"

This command will process the datasets based on the specified parameters and save the output in the 'client_data' directory.


## Output Details
The script outputs a CSV file named `final_output.csv` in the 'client_data' directory. The file contains the following columns:
- `client_identifier`: A unique identifier for each client.
- `email`: Email address of the client.
- `bitcoin_address`: Bitcoin address associated with the client.
- `credit_card_type`: Type of credit card held by the client.
- `country`: Country of residence of the client.

The final output is a CSV file named 'final_output.csv', located in the 'client_data' directory. This file contains the processed data with selected fields and renamed columns for better understanding and readability.

## Testing Procedures
- **Unit Testing**: Validates individual functions for accuracy and correctness, ensuring each component functions as expected independently.
- **Integration Testing**: Tests the interactions between different components of the application to verify the integrated system works as intended.

## Automated Workflow
- **Code Scanning - Workflow**: Utilizes GitHub Actions for automated code scanning to detect security vulnerabilities and coding errors, promoting a secure codebase.
- **Python Application CI - Workflow**: This automated workflow ensures continuous integration through setting up Python environments, dependency management, code linting with flake8, and running tests with pytest.

Both workflows are triggered with every push or pull request to the master branch, ensuring consistent code quality and security checks throughout the development process.
