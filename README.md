# bquick

[![Join the chat at https://gitter.im/visualskyrim/bquick](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/visualskyrim/bquick?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Command line tools for Google BigQuery


> This project assumes you have installed [Google Cloud SDK](https://cloud.google.com/sdk/)

## Install

## Usage

### List Table

`bquick ls -n [<num-of-tables>]`

`bquick ls -w [<wildcard-prefix>] -n [50]`

`bquick ls -w [<wildcard-prefix>] [<start-date>] [<end-date>] -n [50]`

`bquick ls -r [<reg-string>] -n [50]`

`bquick ls -m [<replace-string-with-$-mark>] [<replacement-array>] -n [50]`

### Delete Table

`bquick del -f [<table-list-file>]`

`bquick del -w [<wildcard-prefix>]`

`bquick del -w [<wildcard-prefix>] [<start-date>] [<end-date>]`
