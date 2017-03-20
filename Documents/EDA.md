# Exploratory Data Analysis

## Dataset Schema

### Books Review Schema
    root
     |-- asin: string (nullable = true)
     |-- helpful: array (nullable = true)
     |    |-- element: long (containsNull = true)
     |-- overall: double (nullable = true)
     |-- reviewText: string (nullable = true)
     |-- reviewTime: string (nullable = true)
     |-- reviewerID: string (nullable = true)
     |-- reviewerName: string (nullable = true)
     |-- summary: string (nullable = true)
     |-- unixReviewTime: long (nullable = true)

### Books Metadata Schema

    root
    |-- _corrupt_record: string (nullable = true)
    |-- asin: string (nullable = true)
    |-- brand: string (nullable = true)
    |-- categories: array (nullable = true)
    |    |-- element: array (containsNull = true)
    |    |    |-- element: string (containsNull = true)
    |-- description: string (nullable = true)
    |-- imUrl: string (nullable = true)
    |-- price: double (nullable = true)
    |-- related: struct (nullable = true)
    |    |-- also_bought: array (nullable = true)
    |    |    |-- element: string (containsNull = true)
    |    |-- also_viewed: array (nullable = true)
    |    |    |-- element: string (containsNull = true)
    |    |-- bought_together: array (nullable = true)
    |    |    |-- element: string (containsNull = true)
    |    |-- buy_after_viewing: array (nullable = true)
    |    |    |-- element: string (containsNull = true)
    |-- salesRank: struct (nullable = true)
    |    |-- Arts, Crafts & Sewing: long (nullable = true)
    |    |-- Books: long (nullable = true)
    |    |-- Cell Phones & Accessories: long (nullable = true)
    |    |-- Clothing: long (nullable = true)
    |    |-- Health & Personal Care: long (nullable = true)
    |    |-- Home &amp; Kitchen: long (nullable = true)
    |    |-- Kitchen & Dining: long (nullable = true)
    |    |-- Movies & TV: long (nullable = true)
    |    |-- Music: long (nullable = true)
    |    |-- Office Products: long (nullable = true)
    |    |-- Shoes: long (nullable = true)
    |    |-- Sports &amp; Outdoors: long (nullable = true)
    |    |-- Toys & Games: long (nullable = true)
    |-- title: string (nullable = true)

**Record Counts**
* reviews_Books_5.json.gz - `8898041`
* metadata.json.gz - `2370585`

### Reviewer Review Statistics
    +-------+------------------+
    |summary|             count|
    +-------+------------------+
    |  count|            603668|
    |   mean|14.739958056415116|
    | stddev| 51.77640136694963|
    |    min|                 5|
    |    max|             23222|
    +-------+------------------+

### Overall Helpfulness Ratio Distribution

## Temporal Analysis
### Monthly
### Weekly
### Day of Week

## Review Text Analysis
### Word Count Distribution
### Top-100 Word Frequency
(With Stopwords Removed)

## Price Distribution
## Brand Distribution
## Category Distribution
