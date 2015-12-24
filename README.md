# bquick

[![Join the chat at https://gitter.im/visualskyrim/bquick](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/visualskyrim/bquick?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Command line tools for Google BigQuery


> This project assumes you have installed [Google Cloud SDK](https://cloud.google.com/sdk/), and have logged in.


## Prerequisite

To use bquick, you have to and only have to install [Google Cloud SDK](https://cloud.google.com/sdk/) and login with your account.

To login, use following command and follow the instruction given by the tool:

`gcloud login`


> For detailed information, refer to [Google Cloud SDK](https://cloud.google.com/sdk/).

## Install

### From source code

***Step 1***: Get the source code:

`git clone git@github.com:visualskyrim/bquick.git`

***Step 2***: Install

`python setup.py install`


## Usage

### List Table

***List all tables:***

`bquick <dataset> ls [-l <rows-to-show>]`

```
bquick test_dateset ls -l 20
```

> The `<rows-to-show>` is defaultly 50


***List tables that match wildcard prefix***

`bquick <dataset> ls -w <wildcard-prefix> [-l <rows-to-show>]`

Get all the tables with given wildcard prefix.

`bquick <dataset> ls -w <wildcard-prefix> <start-date> <end-date> [-l <rows-to-show>]`

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
