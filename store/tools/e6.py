from collections import defaultdict
from multisilo_benchmarks import (
    Parameters, ParametersAndResult, parse_args, run_suite, zookeeper_clients,
    zookeeper_servers)
from sheets_query import header
import benchmark

def main(args):
    # Set benchmark parameters.
    base_parameters = Parameters(
        config_file_directory=args.config_file_directory,
        f=1,
        key_file=args.key_file,
        num_keys=1000 * 1000,
        num_server_threads=12,
        repl_scheme=None,
        server_binary=args.server_binary,
        client_binary=args.client_binary,
        benchmark_duration_seconds=60,
        benchmark_warmup_seconds=15,
        transaction_length=1,
        write_percentage=100,
        zipf_coefficient=None,
        num_client_machines=7,
        num_clients_per_machine=32,
        num_threads_per_client=1,
        suite_directory=args.suite_directory,
    )

    parameters_list = [
      base_parameters._replace(
          repl_scheme=repl_scheme,
          zipf_coefficient=zipf_coefficient,
      )
      for repl_scheme in ['ir', 'lir']
      for zipf_coefficient in [
          0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0
      ]
    ]

    # Run every experiment three times.
    parameters_list = parameters_list * 3

    # Run the suite.
    suite_dir = benchmark.SuiteDirectory(args.suite_directory, 'e6')
    suite_dir.write_dict('args.json', vars(args))
    run_suite(suite_dir, parameters_list, zookeeper_clients(), zookeeper_servers())

if __name__ == '__main__':
    main(parse_args())
