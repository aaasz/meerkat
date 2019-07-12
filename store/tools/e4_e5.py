from collections import defaultdict
from multisilo_benchmarks import (
    Parameters, ParametersAndResult, parse_args, run_suite, zookeeper_clients,
    zookeeper_new_servers)
from sheets_query import header
import benchmark

def main(args):
    # Set benchmark parameters.
    base_parameters = Parameters(
        config_file_directory=args.config_file_directory,
        f=1,
        key_file=args.key_file,
        num_keys=38 * 1000 * 1000,
        num_server_threads=38,
        repl_scheme=None,
        server_binary=args.server_binary,
        client_binary=args.client_binary,
        benchmark_duration_seconds=80,
        benchmark_warmup_seconds=35,
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
          repl_scheme=repl_scheme,
          zipf_coefficient=zipf_coefficient,
          num_client_machines=num_client_machines,
          num_threads_per_client=num_threads_per_client,
      )
      for repl_scheme in ['ir', 'lir']
      for zipf_coefficient in [0.75]
      for (num_client_machines, num_threads_per_client) in [
          (1,38),
          (2,38),
          (3,38),
          (4,38),
          (5,38),
          (6,38),
          (7,38),
          (8,38),
          (9,38),
          (10,38),
          (11,38),
          (12,38),
          (13,38),
      ]
    ]

    # Run every experiment three times.
    parameters_list = [q for p in parameters_list for q in [p] * 3]

    # Run the suite.
    suite_dir = benchmark.SuiteDirectory(args.suite_directory, 'e4_and_e5')
    suite_dir.write_dict('args.json', vars(args))
    run_suite(suite_dir, parameters_list, zookeeper_clients(), zookeeper_new_servers())

if __name__ == '__main__':
    main(parse_args())
