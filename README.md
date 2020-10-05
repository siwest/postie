# Scenario

It is July 3rd, 2017. A business analyst just started at your company on July 1st and is getting to know their way around the sales data. They desperately need your data engineering skills!

## Technical Details

The system log data is located publically via http in us-east-1 on AWS S3 in the
postie-testing-assets bucket.

Each file contains rows with the following data:

`timestamp` - the date of the transaction

`website_id` - the integer value of the website the transaction took place on

`customer_id` - the integer identifier for the customer

`app_version` - the version of the software

`placeholder` - a placeholder column

`checkout_amount` - the amount in dollars of the complete transaction

`url` - the full website url that instantiated the transaction, with product names and product count params


### Task 1

Create a small python script/app that loads the data into a sqlite database.

**Solution to Task 1 can be called with the following command line python program:** 

```shell
python load_transactions.py
```

### Task 2

Create a small python script/app that takes a range of dates and sums the sales data ingested into the sqlite db for that time period.

**Solution to Task 2 can be called with any of the following options**


```shell
python query_transactions.py
```


> Total sales from 2015-10-07 11:00:42.385359 to 2020-10-05 11:00:42.385359 is $588,850.00




```shell
python query_transactions.py -s '2017-07-03'
```


> Total sales from 2017-07-03 to 2020-10-05 11:02:22.684169 is $181,583.00



```shell
python query_transactions.py -e '2017-07-02'
```


> Total sales from 2012-07-03 to 2017-07-02 is $407,267.00




```shell
python query_transactions.py -s '2017-07-01' -e '2017-08-01'
```

> Total sales from 2017-07-01 to 2017-08-01 is $588,850.00



If no end date is set, the default end date is the current date. If no start date is set, the start date defaults to 5 years before the end date.

