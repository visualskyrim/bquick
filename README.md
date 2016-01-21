# bquick

[![Join the chat at https://gitter.im/visualskyrim/bquick](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/visualskyrim/bquick?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Command line tools for Google BigQuery


> This project assumes you have installed [Google Cloud SDK](https://cloud.google.com/sdk/), and have logged in.


## Prerequisite

To use bquick, you have to and only have to install [Google Cloud SDK](https://cloud.google.com/sdk/) and login with your account.

To login, use following command and follow the instruction given by the tool:

`gcloud auth login`


> For detailed information, refer to [Google Cloud SDK](https://cloud.google.com/sdk/).

## Install

### From pip

`pip install bquick`

> You might need root permission.


### From source code

***Step 1***: Get the source code:

```
git clone git@github.com:visualskyrim/bquick.git
```

***Step 2***: Install

```
python setup.py install
```


## Usage

### List Table

***List all tables:***

```
bquick <dataset> ls [-l <rows-to-show>]
```

```
bquick test_dateset ls -l 20
```

> The `<rows-to-show>` is default 50


***List tables that match wildcard prefix***

```
bquick <dataset> ls -w <wildcard-prefix> [-l <rows-to-show>]
```

Get all the tables with given wildcard prefix.

```
bquick <dataset> ls -w <wildcard-prefix> <start-date> <end-date> [-l <rows-to-show>]
```

Get all the table with given wildcard and within the given date range.

Arguments `<start-date>` and `<end-date>` should be in format of `YYYY-mm-dd`.

```
bquick test_dateset ls -w test_table 2015-12-12 2015-12-24
```

> About wildcard functions, please refer to [BigQuery Query Reference](https://cloud.google.com/bigquery/query-reference?hl=en#tablewildcardfunctions).


***List tables that match regex expression***

`bquick <dataset> ls -r <reg-string> [-l <rows-to-show>]`

```
bquick test_dataset ls -r 'test_table_\w{32}\d{8}'
```

> The `<reg-string>` should be enclosed within single-quote.

### Delete Table

Delete tables in BigQuery.

All the tables to be deleted will be confirmed before the deletion.

***Delete table by name:***

```
bquick <dataset> del -n <table-name>
```

Delete the table with given name.


***Delete tables in the file***

```
bquick <dataset> del -f <tables-file>
```

All the tables with the name in given file will be deleted.
Table name should be one line in the file.

> `<tables-file>` could be either absolute path or relative path pointing to the file.


***Delete tables matching the regex expression:***

```
bquick <dataset> del -r <table-name-pattern>
```

Delete the tables with the name that matches the regex expression.

> `<table-name-pattern>` should be enclosed by single-quote.

***Delete tables matching the wildcard prefix***

```
bquick <dataset> del -w <wildcard-prefix> <start-date> <end-date>
```

Delete all the tables that match `<wildcard-prefix>` and between the `<start-date>` and `<end-date>`.


### Copy Table

Copy tables in BigQuery within the dataset and cross the dataset.


***Copy tables in the file***

Copy tables from one dataset to another.

> In BigQuery, copying tables actually consumes the job.
> To make sure the copying doesn't over used, there is running jobs check before copy any table.
> Script will wait until the job execute is availible in your project.
>
> More details about the limit on jobs, please refer to [quota policy](https://cloud.google.com/bigquery/quota-policy).

```
bquick <dataset> cp -d <dest-dataset> -f <tables-file>
```

All the tables with the name in given file and the `<dataset>` will be copied into `<dest-dataset>`.
Table name should be one line inthe file.

> `<tables-file>` could be either absolute path or relative path pointing to the file.


***Copy tables that match regex pattern***

```
bquick <dataset> cp -d <dest-dataset> -r <table-name-pattern>
```

Copy all the tables in the `<dataset>` to the `<dest-dataset>` that match the `<table-name-pattern>`.

> `<table-name-pattern>` should be enclosed by single-quote.


***Copy tables that match wildcard pattern***

```
bquick <dataset> cp -d <dest-dataset> -w <wildcard-prefix> <start-date> <end-date>
```

Copy all the tables that match `<wildcard-prefix>` and between the `<start-date>` and `<end-date>`.
