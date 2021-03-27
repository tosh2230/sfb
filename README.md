# sfb

[![CI](https://github.com/tosh223/sfb/actions/workflows/ci.yml/badge.svg)](https://github.com/tosh223/sfb/actions/workflows/ci.yml)

sfb helps SQL testing and estimating the cost of services that depend on scan volume.

## Description

- Check SQL syntax
- Estimate query costs for free
    - per Run
    - per Month
- Replace query parameters automatically
- Be useful on continuous integration
- Use dryrun include [Google BigQuery API Client Libraries](https://cloud.google.com/bigquery/docs/reference/libraries)

## Install

```sh
$ pip install sfb
```

## Requirements

- Python >= 3.6
    - Jupyter Notebook
    - Google Colaboratory
- google-cloud-bigquery >= 2.6.1
- pyyaml >= 5.4.1

## Usage

### Estimate Query Costs

```sh
# If runs with no arguments, execute files in './sql/*.sql'.
$ sfb
{
  "Succeeded": [
    {
      "SQL File": "/home/admin/project/sfb_test/sql/covid19_open_data.covid19_open_data.sql",
      "Total Bytes Processed": "1.9 GiB",
      "Estimated Cost($)": {
        "per Run": 0.009414,
        "per Month": 0.28242
      },
      "Frequency": "Daily"
    },
    {
      ...
    }
  ],
  "Failed": [
    {
      "SQL File": "/home/admin/project/sfb_test/sql/test_failure_badrequest_01.sql",
      "Errors": [
        {
          "message": "Unrecognized name: names; Did you mean name? at [9:5]",
          "domain": "global",
          "reason": "invalidQuery",
          "location": "q",
          "locationType": "parameter"
        }
      ]
    },
    {
      ...
    }
  ]
}
```

```sh
# Others
$ sfb -f ./sql/*.sql
$ sfb -q "select * from test;"
$ echo "select * from test;" | sfb | jq
$ find ./sql -type f | sfb
```

### Arguments

```sh
$ sfb -h
usage: sfb [-h] [-f [FILE [FILE ...]] | -q QUERY] [-c CONFIG] [-v] [-d]
           [-p PROJECT] [-k KEY]

optional arguments:
  -h, --help            show this help message and exit
  -f [FILE [FILE ...]], --file [FILE [FILE ...]]
                        sql file path
  -q QUERY, --query QUERY
                        query string
  -c CONFIG, --config CONFIG
                        sfb config path
  -p PROJECT, --project PROJECT
                        GCP project
  -k KEY, --key KEY
                        GCP service account key path
  -v, --verbose         return verbose results
  -d, --debug           run as debug mode
```

### Directory (Optional)

```sh
$ tree .
.
├── config
│   └── sfb.yaml
├── log
│   └── sfb.log (if runs as debug mode)
└── sql
    └── [SQL files here]
```

### Configuration

```sh
$ cat ./config/sfb.yaml

# Default settings
Globals:
  Service: BigQuery
  Location: US
  Frequency: Daily

QueryFiles:
  [your_sql_file_name]:
    Frequency: Weekly
    Parameters:
    - name: ds_start_date
      type: DATE
      value: '2020-01-01'
    - name: ds_end_date
      type: DATE
      value: '2020-01-31'
  ...
```

#### Type

Name of query parameter type. Select one of types below.

- STRING
- INT64
- FLOAT64
- NUMERIC
- BOOL
- TIMESTAMP
- DATETIME
- DATE

#### Frequency

For calculating monthly cost estimation.

- Hourly
    - (cost_per_run) * 30(days) * 24(h)
- Daily
    - (cost_per_run) * 30(days)
- Weekly
    - (cost_per_run) * 4(weeks)
- Monthly
    - cost_per_run
