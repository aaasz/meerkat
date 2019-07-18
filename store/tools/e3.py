from collections import defaultdict
from meerkat_benchmarks import (
    Parameters, ParametersAndResult, parse_args, run_suite, zookeeper_clients,
    zookeeper_servers)
from sheets_query import header
import benchmark

def main(args):
    # Set benchmark parameters.
    base_parameters = Parameters(
        config_file_directory=args.config_file_directory,
        f=0,
        key_file=args.key_file,
        num_keys=1 * 1000 * 1000,
        num_server_threads=1,
        repl_scheme=None,
        server_binary=args.server_binary,
        client_binary=args.client_binary,
        benchmark_duration_seconds=20,
        benchmark_warmup_seconds=5,
        transaction_length=1,
        write_percentage=0,
        zipf_coefficient=None,
        num_client_machines=1,
        num_clients_per_machine=1,
        num_threads_per_client=1,
        suite_directory=args.suite_directory,
    )

    parameters_list = [
      base_parameters._replace(
          repl_scheme=repl_scheme,
          zipf_coefficient=zipf_coefficient,
          num_client_machines=num_client_machines,
          num_threads_per_client=num_threads_per_client,
      )
      for repl_scheme in ['ir']
      #for zipf_coefficient in [0.2, 0.4, 0.6, 0.8, 0.9, 1, 1.1, 1.2]
      for zipf_coefficient in [0]
      for (num_client_machines, num_threads_per_client) in [
          (1,1),
          (1,2),
          (1,3),
          (1,4),
          (1,5),
          (1,6),
          (1,7),
          (1,8),
          (1,9),
          (1,10),
          (1,11),
          (1,12),
          #(2,38), # 418
          #(3,38), # 418
          #(4,38), # 418
          #(5,38), # 418
          #(6,38), # 418
          #(7,38), # 418
          #(8,38), # 418
          #(9,38), # 418
          #(10,38), # 418
          #(11,38), # 418
          #(12,38), # 456
          #(13,38), # 456
          #(7,80), # 456
          #(14,80), # 456
          #(11,80), # 456
          #(12,80), # 456
          #(13,80), # 456
          #(14,80), # 456
          #(13,38), # 494
          #(13,40), # 520
          #(13,42), # 546
          #(14,60), # 560
          #(13,44), # 572
          #(13,46), # 598
          #(14,100), # 644
          #(14,120), # 644
          #(10, 50),  # 500
          #(10, 100), # 1000
      ]
    ]

    # Run every experiment three times.
    parameters_list = [q for p in parameters_list for q in [p] * 3]

    # Run the suite.
    suite_dir = benchmark.SuiteDirectory(args.suite_directory, 'e3')
    suite_dir.write_dict('args.json', vars(args))
    run_suite(suite_dir, parameters_list, zookeeper_clients(), zookeeper_servers())

if __name__ == '__main__':
    main(parse_args())
