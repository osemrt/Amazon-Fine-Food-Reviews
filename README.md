
# Amazon-Fine-Food-Reviews

### General information about the application

This project is based on the Analysis of the ‘Amazon Fine Food Review’ dataset. The raw dataset consists of reviews of fine foods from Amazon. The data span a period of more than 10 years, including all ~500,000 reviews up to October 2012. Reviews include product id and user information, ratings, and a plaintext review.

The dataset is available at [kaggle.com](https://www.kaggle.com/snap/amazon-fine-food-reviews).

In the project, we use MapReduce is a programming framework to perform some descriptive statistics functions on ‘Amazon Fine Food Review’ dataset in a distributed environment.

![enter image description here](https://github.com/image-assets/png/blob/master/windows.png?raw=true)




### Use case scenario of the application

The size of the databases used in today's enterprises has been growing at exponential rates day by day. Simultaneously, the need to process and analyze the large volumes of data for big companies has also increased. This has contributed to the big data problem faced by the industry due to the inability of conventional database systems and software tools to manage or process the big data sets within tolerable time limits.

As an example scenario, we can think amazon.com is the world's largest online retailer. Sold products on amazon.com gets reviews and these reviews also are shown the users that review the product. Amazon offers many filters to the users in the product review section and product search section. The users can filter the products and comments according to their requirements like show first the most helpful comment, the product that has the most average score, etc. To perform these kinds of filters, I implemented their major time-consuming part of them in a multi-node cluster environment to reduce the computation time. The function objectives that are implemented in the application are listed below.

1. Average rating of each product 
2. The most and least helpful comments for each product. 
3. Pearson correlation between product scores and their helpfulness ratio 
4. Product review count of each product 
5. Standard deviation of each product scores

### Explanation of function implementations in the application 
In this section, we will explain the implementation each of function respectively

1. **Average rating of each product** 
- Mapper: returns product id with the user score 
- Reducer: computes the average value of product scores 
2. **The most and least helpful comments for each product.** 
- Mapper: returns product id with the CommentWritable object 
- Reducer: finds the most and least helpful comment of each product 
3. **Pearson correlation between product scores and their helpfulness ratio** 
- Mapper: returns product id with the CoupleWritable object 
- Reducer: performs the pearson’s correlation formula for each product 
4. **Product review count of each product** 
- Mapper: returns product id with the IntWritable object 
- Reducer: computes the total review count of each product 
5. **Standard deviation of each product scores** 
- Mapper: returns product id with the user score 
- Reducer: computes the standard deviation of product scores

### Experience and Discussion

After completing the project, I have an understanding of MapReduce framework. I have realized how the MapReduce framework facilitates us to write code to process huge data present in the HDFS, Hadoop gave me the skills needed to analyze massive datasets using distributed computing techniques.

