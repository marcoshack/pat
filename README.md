# PAT: Performance Analysis Tools

## Setup

    export PATH=.env/bin:$PATH
    pip install -r requirements.txt
    make test

## Usage

### Load data from access.log files

First you need to transform access log to CSV:

    python -m pat.nginx.access_log_to_csv <access_log_files_pattern> > my_requests.csv

now you can use `pat.nginx` module functions to load the request data:

    from pat import nginx

    reqs = nginx.load_access_log_data('my_requests') # don't put .csv extension
    rps_aggr = rps = nginx.aggregate_rps(day_requests)

### Plot RPS

    nginx.plot_rps(rps_aggr)

### Find load periods

    load_periods = nginx.find_load_periods(rps)

### Split data for each load period

    l1_reqs, l1_rps = nginx.error_summary(reqs)

### Error summary

    nginx.error_summary(l1_reqs)
