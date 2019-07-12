# process_logs.py is a module and library to process the results of a TAPIR
# benchmark. The script takes in the log produced by a TAPIR client (or the
# concatenation of many of these logs) and outputs a JSON object that
# summarizes the results of the benchmark (e.g., average latency, average
# throughput).

import argparse
import collections
import json

LogEntry = collections.namedtuple('LogEntry', [
    'txn_id',
    'start_time_sec',
    'end_time_sec',
    'latency_micros',
    'success',
    'txn_type',
    'extra',
])

BenchmarkResult = collections.namedtuple('BenchmarkResult', [
    # Counts.
    'num_transactions',
    'num_successful_transactions',
    'num_failed_transactions',
    'abort_rate',

    # Throughputs.
    'throughput_all',
    'throughput_success',
    'throughput_failure',

    # Latencies.
    'average_latency_all',
    'median_latency_all',
    'average_latency_success',
    'median_latency_success',
    'average_latency_failure',
    'median_latency_failure',
    'follow_txn_avg_latency_success',
    'tweet_txn_avg_latency_success',

    # Extra.
    'extra_all',
    'extra_success',
    'extra_failure',
])


def mean(xs):
    if len(xs) == 0:
        return 0
    else:
        return float(sum(xs)) / len(xs)


def median(xs):
    if len(xs) == 0:
        return 0
    else:
        return xs[len(xs) / 2]


def process_client_logs(client_log_filename, warmup_sec, duration_sec):
    """Processes a concatenation of client logs.

    process_client_logs takes in a file, client_log, of client log entries that
    look something like this:

        1 1540674576.757905 1540674576.758526 621 1
        2 1540674576.758569 1540674576.759168 599 1
        3 1540674576.759174 1540674576.759846 672 1
        4 1540674576.759851 1540674576.760529 678 1

    or like this:

        4 1540674576.759851 1540674576.760529 678 1 2 4

    where
        - the first column is incremented per client,
        - the second column is the start time of the txn (in seconds),
        - the third column is the end time of the txn (in seconds),
        - the fourth column is the latency of the transaction in microseconds,
        - the fifth column is 1 if the txn was successful and 0 otherwise,
        - the sixth column is the transaction type,
        - the seventh column is the number of extra retries.

    process_client_logs outputs a summary of the results. The first `warmup`
    seconds of data is ignored, and the next `duration` seconds is analyzed.
    All data after this duration is ignored.
    """
    # Extract the log entries, ignoring comments and empty lines.
    log_entries = []
    with open(client_log_filename, 'r') as f:
        for line in f:
            if line.startswith('#') or line.strip() == "":
                continue

            parts = line.strip().split()
            assert len(parts) == 5 or len(parts) == 7, parts

            if len(parts) == 7:
                txn_type = int(parts[5])
                extra = int(parts[6])
            else:
                txn_type = -1
                extra = 0

            log_entries.append(LogEntry(
                txn_id=int(parts[0]),
                start_time_sec=float(parts[1]),
                end_time_sec=float(parts[2]),
                latency_micros=int(parts[3]),
                success=bool(int(parts[4])),
                txn_type=txn_type,
                extra=extra,
            ))

    if len(log_entries) == 0:
        raise ValueError("Zero transactions logged.")

    # Process the log entries.
    #log_entries.sort(key=lambda x: x.end_time_sec)
    #start_time_sec = log_entries[0].end_time_sec + warmup_sec
    #end_time_sec = start_time_sec + duration_sec

    all_latencies = []
    success_latencies = []
    failure_latencies = []
    follow_txn_success_latencies = []
    tweet_txn_success_latencies = []

    all_num_extra = 0.0
    success_num_extra = 0.0
    failure_num_extra = 0.0

    for entry in log_entries:
        #if entry.end_time_sec < start_time_sec:
        #    continue

        #if entry.end_time_sec > end_time_sec:
        #    break

        all_latencies.append(entry.latency_micros)
        all_num_extra += entry.extra

        if entry.success:
            success_latencies.append(entry.latency_micros)
            success_num_extra += entry.extra
            if entry.txn_type == 2:
                follow_txn_success_latencies.append(entry.latency_micros)
            if entry.txn_type == 3:
                tweet_txn_success_latencies.append(entry.latency_micros)
        else:
            failure_latencies.append(entry.latency_micros)
            failure_num_extra += entry.extra

    if len(all_latencies) == 0:
        raise ValueError("Zero completed transactions.")

    all_latencies.sort()
    success_latencies.sort()
    failure_latencies.sort()

    num_transactions = len(all_latencies)
    num_successful_transactions = len(success_latencies)
    num_failed_transactions = len(failure_latencies)

    return BenchmarkResult(
        num_transactions = num_transactions,
        num_successful_transactions = num_successful_transactions,
        num_failed_transactions = num_failed_transactions,
        abort_rate = float(num_failed_transactions) / num_transactions,

        throughput_all = float(num_transactions) / duration_sec,
        throughput_success = float(num_successful_transactions) / duration_sec,
        throughput_failure = float(num_failed_transactions) / duration_sec,

        average_latency_all = mean(all_latencies),
        median_latency_all = median(all_latencies),
        average_latency_success = mean(success_latencies),
        median_latency_success = median(success_latencies),
        average_latency_failure = mean(failure_latencies),
        median_latency_failure = median(failure_latencies),
        follow_txn_avg_latency_success = mean(follow_txn_success_latencies),
        tweet_txn_avg_latency_success = mean(tweet_txn_success_latencies),

        extra_all = all_num_extra,
        extra_success = success_num_extra,
        extra_failure = failure_num_extra,
    )

def main(args):
    result = process_client_logs(args.client_log, args.warmup, args.duration)
    print(json.dumps(result._asdict(), indent=4))

def parser():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--warmup',
        type=float,
        default=10,
        help='The initial warmup time (in seconds) to ignore.')
    parser.add_argument(
        '--duration',
        type=float,
        required=True,
        help='The total number of seconds of log entries to analyze.')
    parser.add_argument(
        'client_log',
        type=str,
        help='The client log to parse.')
    return parser

if __name__ == '__main__':
    main(parser().parse_args())
