# Noaat Tasks
## ETL & Queries Task
You will find the code of ETL file divided into 4 parts:
- Create DB in PostgreSQL and load the Datasets to it.
- Create Spark Session and Extract Data from PostgreSQL.
- Transform Data by applying the required queries.
- Create the Data Warehouse and initiate the required Schema.
- Load the data results to the Data Warehouse

For the Dashboard, there is a photo of it in this repository. However, you can find it itself at the following link:

https://public.tableau.com/views/Noaat/Dashboard1?:language=en-US&:sid=&:display_count=n&:origin=viz_share_link

## AI-based Abuse Detection Task
For this task, it is not stated what are the patterns that can be detected if there is a user that abuse the promocodes. Therefore, I assumed that if there is a user that abuse the promocode, he should create one or more fake accounts besides its original account. In this case, we may found different accounts (different emails & phone numbers) created using the same one of the following:
- IP Address
- Device ID
- User Agent (Not always, but it can be used as indicator)

In this case, I may use Agglomerative Clustering or Graph-based solution to group accounts with similar data. As a result the single accounts with no similarity with others will not be suspected, while those which entered a cluster or group will be suspected.
However, I always start my work by applying simple step but also ver important which is Data Exploration, in which I found that in the entire dataset, there is no any duplicated IP address nor duplicated Device ID.Also, I found that User Agents replicated many times in non-abusers more than in abusers.

Therefore, I found that non of these 3 pieces of information can be used to find similarity between different accounts to find the abusers.
Also, of course other types of data like: Data-of-Birth, phone number, time-zone, city, and others can not be used as indicator for abuse.
Accordingly, in my opinion, I consider this data not helpful to apply any kind of ML algorithms.

It is very important here to refer to that I master my tools very well and I can of course apply all types of ML models, however, I believe that the problem in the provided data, or there is some technical piece of information related to user tracing in web pages that I miss.

## Final Note
If there is anything missing let me know and I will complete it.
