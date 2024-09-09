# Building Data Pipeline to Google BigQuery

## Project Description
This project demonstrates how to build both batch and streaming data pipelines and store the data in Google BigQuery using the following tools:

* Python
* SQL
* Apache Beam
* Apache Airflow
* Google Cloud Pub/Sub

## Project Goal

To create two tables (Ask_HN and Show_HN) including a temporary view to be used for creating a dashboard and other analytics. 

## Source Data: Hacker News Dataset
The dataset used for this project comes from Hacker News, a popular technology and startup news website operated by Y Combinator. The site features user-submitted stories (or "posts") that receive votes and comments, similar to Reddit. Posts reaching the top of the Hacker News listings can garner hundreds of thousands of visitors.

Since the original Hacker News dataset does not include streaming data, Google Cloud Pub/Sub is used to create a streaming topic and generate messages to simulate a streaming data source.

## Installation Instructions
To run this project, ensure you have the following tools installed:

* Python
* SQL
* Apache Beam
* Apache Airflow
* Google Cloud Pub/Sub
* You may also need to configure Google Cloud credentials and set up a BigQuery dataset to store the processed data.

## Usage
Clone repository



