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

**Solution to Task 1 can be called with `python load_transactions.py`**

### Task 2

Create a small python script/app that takes a range of dates and sums the sales data ingested into the sqlite db for that time period.

**Solution to Task 2 can be called with any of the following options**

`python query_transactions.py`
`python query_transactions.py -s '2017-07-03'`
`python query_transactions.py -start '2017-07-01'`
`python query_transactions.py -end '2017-08-01'`
`python query_transactions.py -s '2017-07-01' -e '2017-08-01'`

If no end date is set, the default end date is the current date. If no start date is set, the start date defaults to 5 years before the end date.

