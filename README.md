# Data Engineering with AWS - Project: Data Warehouse
## Purpose
In this project, we create an AWS Redshift Data Warehouse to support the analytics team at Sparkify's music streaming app. The source of the data resides in AWS S3, using python, the data is loaded to staging tables and then it is transform to populate a star schema design. Users from the analytics team could analyze the data within the star schema and answer important business questions.

## How to Run Python Scripts
Python scripts are fully automatic, it is only necessary to set the connection parameters for the redshift database, and the data paths for the logs and songs metadata in S3, and then run the scripts from the console as shown:

    python create_tables.py
    python etl.py

## Files in the repo
### dwh.cfg
This file is used to set the Redshift database's parameters, the IAM ARN of the role used to run queries in the cluster, and the data paths of the logs and songs metadata in S3.

### sql_queries.py
This script define all que queries and copy commands that will be executed in the Redshift database.

### create_tables.py 
This script runs all the queries to create both staging tables and star schema tables.

### sql_queries.py
This script runs all the queries to populate staging tables and then use these tables to populate the star schema tables.

## Example query
*Question:* What's the percentage of women and men who uses Sparkify for each day of the week?
    
    example_query = """
    with tb_male as ( select t.weekday, count(distinct sp.user_id) as male_users  
    from songplays sp 
        join users u
            on (sp.user_id = u.user_id 
                and sp.level = u.level)
        join time t 
            on (sp.start_time = t.start_time) 
    where u.gender = 'M'
    group by 1 
    order by 1 ), 
    tb_female as ( select t.weekday, count(distinct sp.user_id) as female_users  
    from songplays sp 
        join users u
            on (sp.user_id = u.user_id 
                and sp.level = u.level)
        join time t 
            on (sp.start_time = t.start_time) 
    where u.gender = 'F'
    group by 1 
    order by 1 )
    select tb_male.weekday, 
        tb_male.male_users, 
        tb_female.female_users, 
        tb_male.male_users + tb_female.female_users as total_users, 
        cast(tb_male.male_users as float)/total_users as male_percentage, 
        cast(tb_female.female_users as float)/total_users as female_percentage
    from tb_male, tb_female 
    where tb_male.weekday=tb_female.weekday;
    """

    example_answer = """
    weekday	male_users	female_users	total_users	male_percentage	female_percentage
    1	28	31	59	1	28	31	59	0.4745762711864407	0.5254237288135594
    6	19	26	45	0.4222222222222222	0.5777777777777777
    2	24	33	57	0.42105263157894735	0.5789473684210527
    3	27	33	60	0.45	0.55
    4	25	31	56	0.44642857142857145	0.5535714285714286
    0	19	20	39	0.48717948717948717	0.5128205128205128
    5	28	35	63	0.4444444444444444	0.5555555555555556
    """