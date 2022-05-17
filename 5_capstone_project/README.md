# Data Engineering: Capstone Project
## Project introduction
We want to build a ETL pipeline that takes data from kaggle that was uploaded to AWS S3 bucket, transforms them into Fact and dimension tables, then uploads them  back as staging tables into a different s3 bucket. This data may be used by the team to predict how reviews after a product seller's store performance.

## Table of contents:

- [Objective](#objectives)
- [Dimension and fact Table](#starschema)
- [The primary scripts of the project](#scripts)
- [Datasets](#datasets)
- [Additional Steps that can be taken](#additionalsteps)


## Objectives
- Transform Amazon bestsellers and reviews data to predict how reviews determine a sellers store performance

## StarSchema
![Interface](https://github.com/Kondwani7/Udacity-DataEngineering-Projects/blob/main/5_capstone_project/star_schema.png)

### Scripts
the main scripts in this project are
- udacity_capstone: It contains the scripts used to create the entire ETL pipeline
- config_write: contains the script used to set your AWS credentials 
- AWS_local_creds : It contains your access and key secret key provided for your IAM user. when creating the IAM user on AWS, attach the "AmazonS3FullAcess" policy to have complete control over reading and writing to S3 buckets. 



## Datasets
### Amazon Bestseller data
The first data set is sourced from [kaggle](https://www.kaggle.com/datasets/waqarahmad101/amazon-best-seller-product-data) It contains data on the best sellers' store performance.and is webscrapped from the amazon website. it has over 6.7 million rows.

### Aamzon Product Reviews
Th second dataset is also sourced from [kaggle](https://www.kaggle.com/datasets/waqarahmad101/amazon-best-seller-product-data). It contains product reviews on various Amazon products. It has 568k rows.


## AdditionalSteps
- In a real world setting, the data would have been sourced from Amazon's APIs
- As the data scales, A data lake such as Amazon's EMR would have been used
- Airflow would have been leveraged to visualize and automate running the entire ETL process
- A time scheduler would be set to handle the batch processing daily 
