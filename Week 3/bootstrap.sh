#!/bin/bash
sudo yum update -y
sudo yum install git -y
git clone https://github.com/dlanger/coronavirus-hc-infobase-archive.git
git clone https://github.com/datasets/covid-19.git
aws s3 cp coronavirus-hc-infobase-archive/CA__covid19__latest.csv s3://covid-19-tracker-2020/data/CA__covid19__latest.csv
aws s3 cp covid-19/data/time-series-19-covid-combined.csv s3://covid-19-tracker-2020/data/time-series-19-covid-combined.csv