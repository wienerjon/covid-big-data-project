# The Effects of COVID-19 on NYC Transportation

Authors: Ignacio Calvera Maldonado, Jonathan Wiener, and Xuelei Guo
</br></br>

## Introduction

<blockquote>
In this project, we will  discuss and analyze the effects of the COVID-19 pandemic on NYC’s transportation industry through a wide range of city indicators, as well as the extent to which future case counts can be predicted through changes in transportation behaviours. The indicators we will be working with are CitiBike traffic and travels, for-hire taxis, MTA Subway turnstiles, pedestrian traffic across the Brooklyn Bridge, vehicle traffic across MTA bridges and tunnels, and national TSA air passenger travel data, as they cover the wide spectrum of transportation options in NYC and together can provide robust estimates of overall transportation volumes and distribution throughout NYC. 
</blockquote>
</br></br>

## Problem Formulation

<blockquote>
Our first hypothesis is the following: It is expected to find that fewer people are traveling this past year than pre-COVID.

By observing the transportation data from the years 2019 to 2020, it is theorized that a drastic decrease in travel will take place in 2020 when compared to 2019.
This is what is being expected since during the COVID-19 pandemic, many of the residents in the greater New York City area were quarantined once coronavirus cases started to be reported.

Our second hypothesis is the following: It is expected to find a correlation between transportation and COVID-19 cases, when accounting for the necessary virus incubation periods specified by the CDC.

If it proves to be true, and predictions can be made with a high level of accuracy, these findings could be used to help health services employees better prepare for incoming surges in cases. Mortality rates were at their worst when hospitals and other health centers suddenly found themselves without enough capacity to handle the surges in infections. Therefore, a highly accurate predictor that could anticipate these surges some days in advance and provide an estimate of cases for said days could prove to be a great resource in making sure hospitals are aware of the amount of cases that will be coming in soon, and so anticipate and increase medical supplies if needed.
</blockquote>
</br></br>

## Dataset References

- Citibike (2020). System Data. Retrieved from https://www.citibikenyc.com/system-data 

- Transportation Security Administration (TSA) (2020). TSA checkpoint travel numbers for 2020 and 2019. Retrieved from http://www.tsa.gov/coronavirus/passenger-throughput 

- Taxi and Limousine Commission (TLC), (2020). TLC Trip Record Data. Retrieved from https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

- Metropolitan Transportation Authority (MTA) (2019). Turnstile Usage Data: 2019. Retrieved from https://data.ny.gov/Transportation/Turnstile-Usage-Data-2019/xfn5-qji9 

- Metropolitan Transportation Authority (MTA) (2020). Turnstile Usage Data: 2020. Retrieved from https://data.ny.gov/Transportation/Turnstile-Usage-Data-2020/py8k-a8wg 

- Department of Transportation (DOT), NYC OpenData (2020). Brooklyn Bridge Automated Pedestrian Counts Demonstration Project. Retrieved from
https://data.cityofnewyork.us/Transportation/Brooklyn-Bridge-Automated-Pedestrian-Counts-Demons/6fi9-q3ta

- Police Department (NYPD) (2020). Crash-Pedestrians-2020. Retrieved from
https://data.cityofnewyork.us/Public-Safety/Crash-Pedestrians-2020/anau-hnpq#__sid=js0
</br></br>

## Dependancies and Libraries

- Apache Spark v2.4.0: https://spark.apache.org/
- Pandas v1.14: https://pandas.pydata.org/
- Matplotlib v2.1.0: https://matplotlib.org/
- NumPy v1.19.4: https://numpy.org/install/
- Scikit Learn v0.0: https://scikit-learn.org/stable/install.html
- Statsmodels v0.12.1: https://www.statsmodels.org/stable/index.html