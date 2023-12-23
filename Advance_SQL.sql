create database ad_sql;
use ad_sql;

SELECT * FROM ad_sql.rank_denrank_rownum;
## Rank, Dense_rank and row_num

select *, rank() over(order by sales desc) rnk,
dense_rank() over(order by sales desc) dense_rnk,
row_number() over(order by sales desc) row_num from rank_denrank_rownum;
-------------------------------------------------------------------------
## Partition by 
select *, rank() over(partition by Department order by sales desc) as emp_rank,
dense_rank() over(partition by Department order by sales desc) as emp_dense_rank,
 row_number() over(partition by Department order by sales desc) as emp_rownumber from rank_denrank_rownum

## Rows between function 
## Sales amount sum based on yester day, present day and next day 

select *, sum(Sales) over(order by Date rows between 1 preceding  and 2 following ) as Total_sales  from rank_denrank_rownum;
select *, sum(Sales) over(order by Date rows between unbounded preceding  and unbounded following ) as Total_sales  from rank_denrank_rownum;

## Runing sum 
select*, sum(sales) over( order by date rows between unbounded preceding and current row) as running_sum from rank_denrank_rownum;
select*, sum(sales) over(partition by Department order by date rows between unbounded preceding and current row) as Departent_runing_sum from rank_denrank_rownum;

## Fisrt and last value of sales  
select*, first_value(sales) over(partition by State order by date) as state_first_sales from rank_denrank_rownum;
select*, last_value(sales) over(partition by State order by date rows between unbounded preceding and unbounded following) as state_last_sales from rank_denrank_rownum;

## nth_value finding that particular sales of 4th day is out put 
select *, nth_value(Sales, 4) over(partition by State rows between unbounded preceding and unbounded following) 4th_day_sales from rank_denrank_rownum;

select *, nth_value(Sales, 4) over(partition by State rows between unbounded preceding and unbounded following)-
first_value(Sales) over(partition by State rows between unbounded preceding and unbounded following) diff_sales_from_1stday from rank_denrank_rownum;

## n-th tile concept 
select *, case when Sales_group = 1 then "High_Sales" when Sales_group = 2 then "Mid_Sales" else "Low_Sales" end as Sales_based_identification from
(select*, ntile(3) over (partition by state order by sales desc) Sales_group from rank_denrank_rownum)a


## Partion by and sub quary  making alias is important 
select * from 
(select*, rank() over(partition by Department order by sales desc) rank_dep from rank_denrank_rownum where Department ="IT") par_rank
where par_rank.rank_dep = 1;


## Highest and loweat perormer of Bihar 
# Highest performer 
select *, first_value(ï»¿Per_ID) over(partition by Department order by sales desc) hiest_performer from rank_denrank_rownum where State ="Bihar";
# Lowest performer 
select *, last_value(ï»¿Per_ID) over(partition by Department order by sales desc rows between unbounded preceding and unbounded following) lowest_performer 
from rank_denrank_rownum where State ="Bihar";

## Get this diffrent netween top performare 
select *, first_value(sales) over(partition by Department order by sales desc) - sales perfomance_diff from rank_denrank_rownum where State ="Bihar";

## How to calcualte moving average 3 years amd 5 years 
select * from rank_denrank_rownum;

select Date, Sales, avg(Sales) over(order  by date rows between  2 preceding and current row) three_year_MA,
avg(Sales) over(order  by date rows between  4 preceding and current row) five_year_MA 
from rank_denrank_rownum;

## Lead and Las function 
create database players;
use players
select * from players_details;

## Total run scord by Virat nad Rohit 
select player_Name, sum(runs) from players_details group by player_Name;

## which year scored what percentage run 
select *, player_Name, Year, runs, sum(runs) over( partition by player_Name order by Year rows between 
unbounded preceding and unbounded following) Total_runs from players_details;

## percenatge of each yaer 
select *, player_Name, Year, runs, (runs/sum(runs) over( partition by player_Name order by Year rows between 
unbounded preceding and unbounded following))*100 Total_runs from players_details;

## Count years in how years they scored run less than prev years 
select player_name, year, runs, lag(runs) over(partition by player_name order by year) previous_year_runs,
lag(runs) over(partition by player_name order by year) - runs run_diff_prev_year from players_details;

##Identification of years
select *, case when run_diff_prev_year >0 then 1 else 0  end as result from 
(select player_name, year, runs, lag(runs) over(partition by player_name order by year) previous_year_runs,
lag(runs) over(partition by player_name order by year) - runs run_diff_prev_year from players_details) a

## Now count years score less than previous Year that means 
select player_name, sum(result) from 
(select *, case when run_diff_prev_year >0 then 1 else 0  end as result from 
(select player_name, year, runs, lag(runs) over(partition by player_name order by year) previous_year_runs,
lag(runs) over(partition by player_name order by year) - runs run_diff_prev_year from players_details) a)b
group by player_name;

## Count number of years where Rohit score more than virat 
select player_name, sum(Rohit_score_higher) from 
(select *, case when extra_run_Rohit < 0 then 1 else 0 end as Rohit_score_higher from
(select * from 
(select player_name, year, runs, lead(runs) over(partition by Year order by player_name)- runs  extra_run_Rohit
from players_details)a
where extra_run_Rohit is not null)b)c
group by player_name;

## Players runs scored in prev year and next year, count number of times in which score in increasing for continous 3 years
select player_name, sum(incrs_runs) from 
(select *, case when previous_year<runs and runs< next_year then 1 else 0 end as incrs_runs from
(select player_name, year, runs, lag(runs) over(partition by player_name order by year) as previous_year,
lead(runs) over(partition by player_name order by year) as next_year
from players_details)a)b
group by player_name

select * from players_details

	




