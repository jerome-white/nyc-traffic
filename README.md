# nyc-traffic
New York City traffic prediction

## Environment variables

The following environment variables should be set:

* `NYCTRAFFIC` Path to top level directory of this project. In most cases, the path to where the repository has been cloned
* `PYTHONPATH` Must include `$NYCTRAFFIC`
* `NYCTRAFFICLOG` Location where program output should be put

## System requirements

Programs are a collection of Bash scripts and Python3 files. Required
Python packages:

* matplotlib
* numpy
* pandas
* pymysql
* scipy
* seaborn
* sklearn
* statsmodels

There is also a reliance on MySQL (version 5+ is sufficient; version
5.6+ is ideal).
