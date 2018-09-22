# INF553 2018F HW1 Description  

Getting familiar with the environment and **MapReduce**

-  Count the number of countries with correct salary 
-  Compare the speed of reduce method after processing partition function with origin one  
-  Calculate the count, min, max, mean of yearly salary of each country

## Environment  

* Spark: 2.3.1  
* Scala: 2.11.11 

## Use Instruction  

open your **terminal**, input commands following this format   
**spark-submit --class <main_class> <jar_name> absolute_path/to/survey_results_public.csv absolute_path/to/task1.csv**  
  
## Example  

spark-submit --class task1 hw1.jar absolute_path/to/survey_results_public.csv absolute_path/to/task1.csv  

## [Dataset](https://www.kaggle.com/stackoverflow/stack-overflow-2018-developer-survey/downloads/survey_results_public.csv/2)

