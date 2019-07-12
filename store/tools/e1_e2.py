from collections import defaultdict
from meerkat_benchmarks import (
    Parameters, ParametersAndResult, parse_args, run_suite, zookeeper_clients,
    zookeeper_servers)
from sheets_query import header
import benchmark


"""
    The scalability graph.
"""

def main(args):
    # Set benchmark parameters.
    base_parameters = Parameters(
        config_file_directory=args.config_file_directory,
        f=0,
        key_file=args.key_file,
        num_keys=None,
        num_server_threads=None,
        repl_scheme=None,
        server_binary=args.server_binary,
        client_binary=args.client_binary,
        benchmark_duration_seconds=20,
        benchmark_warmup_seconds=5,
        transaction_length=1,
        write_percentage=0,
        zipf_coefficient=None,
        num_client_machines=None,
        num_clients_per_machine=1,
        num_threads_per_client=None,
        suite_directory=args.suite_directory,
    )

    parameters_list = [
      base_parameters._replace(
          num_server_threads = num_server_threads,
          num_keys = num_keys,
          num_client_machines = num_client_machines,
          num_threads_per_client = num_threads_per_client,
          zipf_coefficient = zipf_coefficient,
          repl_scheme = repl_scheme,
      )

      for repl_scheme in ['ir']
      # Keep load 24 clients per core
      for (num_server_threads,
           num_keys,
           num_client_machines,
           num_threads_per_client) in [
                                       #(1, 1 * 1000 * 1000, 1, 1),
                                       #(2, 2 * 1000 * 1000, 11, 80),
                                       #(4, 4 * 1000 * 1000, 11, 80),
                                       #(8, 8 * 1000 * 1000, 11, 80),
                                       #(12, 12 * 1000 * 1000, 11, 80),
                                       #(16, 16 * 1000 * 1000, 11, 80),
                                       #(20, 20 * 1000 * 1000, 13, 80),
                                       #(24, 24 * 1000 * 1000, 13, 80),
                                       #(28, 28 * 1000 * 1000, 14, 80),
                                       #(32, 32 * 1000 * 1000, 14, 80),
                                       #(36, 36 * 1000 * 1000, 14, 80),
                                       (1, 1 * 1000 * 1000, 1, 1),
                                       #(40, 40 * 1000 * 1000, 13, 120)
                                       ]

      for zipf_coefficient in [0]
    ]

    # Run every experiment three times.
    #parameters_list = [q for p in parameters_list for q in [p] * 3]

    # Run the suite.
    suite_dir = benchmark.SuiteDirectory(args.suite_directory, 'e1_and_e2')
    suite_dir.write_dict('args.json', vars(args))
    run_suite(suite_dir, parameters_list, zookeeper_clients(), zookeeper_servers())

if __name__ == '__main__':
    main(parse_args())
