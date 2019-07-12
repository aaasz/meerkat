"""
Collects benchmark suite results into a CSV.

When you run a benchmark suite, you get a directory structure that looks
something like this:

    2018-09-30_12:45:46.941341_JYIBPXLYRX/
        2018-09-30_12:45:46.941438/
            args.json
            client.0.log
            client_10.101.0.13_0_cmd.txt
            client.log
            end_time.txt
            parameters.json
            results.json
            server_10.101.0.7_cmd.txt
            start_time.txt
        2018-09-30_12:47:01.632129/
        2018-09-30_12:48:16.579942/
        ...

There's a root benchmark suite directory, one directory for each benchmark, and
files within each benchmark directory. This script takes in a list of the
benchmark directories and outputs a CSV with the parameters and results. For
example, with the above example, I'd run the following

    python collect_results.py 2018-09-30_12:45:46.941341_JYIBPXLYRX/*

The resulting CSV is written to stdout.
"""

from meerkat_benchmarks import Parameters, ParametersAndResult
from process_logs import BenchmarkResult
import csv
import json
import os
import sys


def parse_json_file(filename):
    if not os.path.exists(filename):
        return None

    with open(filename, 'r') as f:
        return json.load(f)


def main():
    csv_writer = csv.writer(sys.stdout)
    csv_writer.writerow(ParametersAndResult._fields)

    for directory in sys.argv[1:]:
        assert os.path.exists(directory)
        parameters = parse_json_file(os.path.join(directory, 'parameters.json'))
        results = parse_json_file(os.path.join(directory, 'results.json'))
        if parameters is None or results is None:
            continue

        parameters.update(results)
        parameters_and_result = ParametersAndResult(**parameters)
        csv_writer.writerow(parameters_and_result)


if __name__ == '__main__':
    main()
