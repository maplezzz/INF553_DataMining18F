# INF553 2018F HW2 Description  

ModelBased and ItemBased Recommend System  
 
 - Task1, using Spark MLlib, [ALS](http://spark.apache.org/docs/latest/mllib-collaborative-filtering.html) algorithm to build ModelBased Recommend System.  
 
 - Task2, build ItemBased Recommend System     
 
  
Person Correlation between items i,j:
 <div align=center><img  src="https://latex.codecogs.com/gif.latex?w_{u,v}&space;=&space;\frac{\sum_{i&space;\in&space;I}&space;(r_{u,i}&space;-&space;\bar{r}_{u})(r_{v,i}&space;-&space;\bar{r}_{v})}%20{\sqrt{\sum_{i&space;\in&space;I}&space;(r_{u,i}&space;-&space;\bar{r}_{u})^2}%20\sqrt{\sum_{i&space;\in&space;I}&space;(r_{v,i}&space;-&space;\bar{r}_{v})^2}}"></div>
 
 Make Item-Based Predictions Using a Simple Weighted Average:  
 <div align=center><img src="https://latex.codecogs.com/gif.latex?P_{a,i}&space;=\frac{\sum_{n&space;\in&space;N}r_{u,n}&space;\cdot&space;w_{i,n}}{\sum_{n&space;\in&space;N}\left&space;|&space;w_{i,n}&space;\right&space;|}"></div>
  
  Loss Function:  
  <div align=center><img src="https://latex.codecogs.com/gif.latex?RMSE&space;=&space;\sqrt{\frac{1}{n}\sum_{i}({Prediction}_i&space;-&space;{Rate}_i)^2}"></div>
  
   
## Environment  
  
  * Spark:  2.3.1
  * Scala:  2.11.11  
  
## Use Instruction  

open your **terminal**, input commands following this format   
**spark-submit --class <main_class> <jar_name> absolute_path/to/rating_file.csv absolute_path/to/testing_file.csv**  
  
## Example  

spark-submit --class ModelBasedCF Fengyu_Zhang_hw2.jar absolute_path/to/train_review.csv absolute_path/to/test_review.csv  
  
## Result  

 * ModelBased CF   

  |Range of errors|Count 
  | :------: | :------: |    
  |>= 0 and < 1|29042   
  |>= 1 and < 2|13797  
  |>= 2 and < 3|2192  
  |>= 3 and < 4|198 
  |>= 4|7  
  |RMSE|1.0554034545951134  
  |Time|90 sec 
  |    |   
  |BaseLine  |  
  | RMSE | 1.08        
      
      
 * ItemBased CF    
 
  |Range of errors|Count 
  | :------: | :------: |    
  |>= 0 and < 1|28458   
  |>= 1 and < 2|13911  
  |>= 2 and < 3|2437 
  |>= 3 and < 4|362 
  |>= 4|68  
  |RMSE|1.0998070682933019
  |Time|126 sec 
  |    |
  |BaseLine|  
  | RMSE | 1.11
  | Time | 450 sec
 

