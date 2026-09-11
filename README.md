# ASG Airlines – End-to-End Data Engineering Case Study

## Project Overview

This project implements an end-to-end data engineering pipeline for ASG Airlines flight operational data.

The pipeline ingests the source Excel dataset, performs data validation and cleaning, applies transformations such as flight-duration calculation and overnight-flight handling, and produces a cleaned dataset for analytical reporting in Power BI.

## Architecture

The data pipeline follows this flow:

Source Excel Dataset  
↓  
ADLS Gen2 – Raw Container  
↓  
Azure Data Factory – Copy Activity  
↓  
ADLS Gen2 – Processed Container  
↓  
Azure Databricks / PySpark  
↓  
Cleaned & Transformed Dataset  
↓  
final_flights.csv  
↓  
Power BI Dashboard & Reporting

## Technologies Used

- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark
- Power BI
- Microsoft Excel

## Data Processing

The data processing workflow includes:

- Data validation and quality checks
- Duplicate-record detection and removal
- Missing-value handling
- Whitespace trimming
- Airline-value standardization
- Flight identifier validation
- Flight-duration calculation
- Overnight/cross-day flight handling
- Route creation
- Anomaly identification
- Invalid flight ID identification

When the arrival time is earlier than the departure time, the flight is treated as an overnight flight and 24 hours are added to the arrival time before calculating the duration.

## Data Quality Results

| Metric | Result |
|---|---:|
| Initial Records | 1,020 |
| Columns | 7 |
| Missing Airline Values | 41 |
| Duplicate Records Removed | 15 |
| Final Records | 1,005 |
| Total Routes | 30 |
| Airlines | 5 |
| Average Flight Duration | 164.62 minutes |
| Total Anomalies | 0 |
| Invalid Flight IDs | 0 |

## Power BI Dashboard

The cleaned dataset is used to create an interactive Power BI report containing five analytical pages:

1. Executive Overview
2. Duration Analysis
3. Route Performance
4. Airline Trends
5. Delay & Anomaly Insights

The dashboard includes KPI cards, charts, slicers, route analysis, airline analysis, duration analysis, and data-quality insights.

## Repository Structure

text
ASG_Airlines_Data_Engineering/
│
├── data/
│   └── final_flights.csv
│
├── notebooks/
│   └── Airline_Data_Engineering.py
│
├── powerbi/
│   ├── ASG_Airlines_Dashboard.pbix
│   └── dashboard.png
│
└── documentation/
    └── ASG_Airlines_Project_Documentation.docx
