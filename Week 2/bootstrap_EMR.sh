#!/bin/bash
aws s3 cp s3://covid-19-tracker-2020/data/CA__covid19__latest.csv /home/hadoop/data/
aws s3 cp s3://covid-19-tracker-2020/data/time-series-19-covid-combined.csv /home/hadoop/data/
aws s3 cp s3://covid-19-tracker-2020/python/Spark-Job-EMR.py /home/hadoop/python/