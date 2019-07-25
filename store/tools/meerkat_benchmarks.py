from collections import namedtuple
from process_logs import BenchmarkResult, process_client_logs
from pyrem.host import RemoteHost
from pyrem.task import Parallel
import argparse
import benchmark
import csv
import datetime
import time
import json
import os
import os.path
import subprocess
import time

# A set of benchmark parameters.
Parameters = namedtuple('Parameters', [
    # Client and server parameters. ############################################
    # The directory in which TAPIR config files are stored.
    'config_file_directory',
    # The maximum number of allowable failures.
    'f',
    # The text file that contains the keys that are preloaded into TAPIR.
    'key_file',
    # The number of keys to read from the key file.
    'num_keys',
    # The number of server threads.
    'num_server_threads',
    # Replication scheme.
    'repl_scheme',

    # Server parameters. #######################################################
    # The server binary.
    'server_binary',

    # Client parameters. #######################################################
    # The client binary.
    'client_binary',
    # The number of seconds that the clients run (must be bigger than
    # 2*benchmark_warmup_seconds).
    'benchmark_duration_seconds',
    # The number of seconds that the clients warmup.
    'benchmark_warmup_seconds',
    # The number of operations (i.e., reads and writes) within a transaction.
    'transaction_length',
    # The percentage of operations in a transaction that are writes.
    'write_percentage',
    # The zipfian coefficient used to select keys.
    'zipf_coefficient',
    # The number of client machines on which we run clients. Can be between 1
    # and 5 since we have 5 client machines.
    'num_client_machines',
    # The number of clients to run on every machine. There are
    # `num_client_machines * num_clients_per_machine` total clients.
    'num_clients_per_machine',
    # The number of threads to run on every client.
    'num_threads_per_client',
    # The directory into which benchmark results are written. If the output
    # directory is foo/, then output is written to a directory foo/1247014124
    # where 1247014124 is a unique id for the benchmark.
    'suite_directory',
])


# ParametersAndResult is a concatenation of a Parameters (the input) and a
# BenchmarkResult (the output).
ParametersAndResult = namedtuple(
    'ParametersAndResult',
    Parameters._fields + BenchmarkResult._fields)


def boxed(s):
    msg = '# {} #'.format(s)
    return '\n'.join(['#' * len(msg), msg, '#' * len(msg)])


# Remote clients and hosts.
def azure_clients():
    return [
        RemoteHost('10.101.0.13'), # multitapir-client-1
        RemoteHost('10.101.0.14'), # multitapir-client-2
        RemoteHost('10.101.0.10'), # multitapir-client-3
        RemoteHost('10.101.0.12'), # multitapir-client-4
        RemoteHost('10.101.0.11'), # multitapir-client-5
    ]


def azure_servers():
    return {
        RemoteHost('10.101.0.7') : None, # multitapir-server-1
        RemoteHost('10.101.0.8') : None, # multitapir-server-2
        RemoteHost('10.101.0.9') : None, # multitapir-server-3
    }


def zookeeper_clients():
    return [
        RemoteHost('10.100.5.153'), # vicuna-1g
        #RemoteHost('10.100.1.2'), # anteater
        #RemoteHost('10.100.1.3'), # bongo
        #RemoteHost('10.100.1.4'), # capybara
        #RemoteHost('10.100.1.5'), # dikdik
        #RemoteHost('10.100.1.6'), # eland
        #RemoteHost('10.100.1.7'), # fossa
        #RemoteHost('10.100.1.10'), # ibex
        #RemoteHost('10.100.1.13'), # lemur
        #RemoteHost('10.100.1.14'), # mongoose
        #RemoteHost('10.100.1.15'), # nyala
        #RemoteHost('10.100.1.17'), # platypus
        #RemoteHost('10.100.1.19'), # rhinoceros
        #RemoteHost('10.100.1.20'), # sloth
        #RemoteHost('10.100.1.39'), # tradewars
        #RemoteHost('10.100.1.35'), # pitfall
        #RemoteHost('10.100.1.22'), # unicorn
        #RemoteHost('10.100.1.23'), # vicuna
    ]


def zookeeper_servers():
    return {
        #RemoteHost('10.100.1.27') : {'iface'       : 'ens1',
        #                             'irq_numbers' : range(83, 99),
        #                             'start_port'  : 51736}, # platypus
        #RemoteHost('10.100.1.19') : {'iface'       : 'ens1',
        #                             'irq_numbers' : range(123, 139),
        #                             'start_port'  : 51736}, # rhino
        RemoteHost('10.100.5.174') : {'iface'       : 'ens1',
                                     'irq_numbers' : range(123, 139),
                                     'start_port'  : 51736}, # tapir-1g
    }

def zookeeper_new_servers():
    return {
        RemoteHost('10.100.1.21') : {'iface'       : 'enp94s0',
                                     'irq_numbers' : range(283, 342),
                                     'start_port'  : 51736}, # tapir
        RemoteHost('10.100.1.22') : {'iface'       : 'enp94s0',
                                     'irq_numbers' : range(283, 342),
                                     'start_port'  : 51736}, # unicorn
        RemoteHost('10.100.1.23') : {'iface'       : 'enp94s0',
                                     'irq_numbers' : range(283, 342),
                                     'start_port'  : 51736}, # vicuna
    }

def num_clients_to_triple(num_clients):
    """Converts a number of clients to a client triple.

    num_clients_to_triple converts a number of clients into a triple
    (a, b, c) where a = num_client_machines, b = num_clients_per_machine, and c
    = num_threads_per_client. It may not be that a * b * c = num_clients, but
    hopefully it's close.
    """
    max_num_client_machines = 5
    max_num_clients_per_machine = 40
    max_num_threads_per_machine = 1

    # Math is hard, so we just brute force all possible triples and use the one
    # the best one. Note that the order of enumerating all triples is important
    # because the min will take the first one.
    all_triples = [
        (a, b, c)
        for c in range(1, max_num_threads_per_machine + 1)
        for b in range(1, max_num_clients_per_machine + 1)
        for a in range(1, max_num_client_machines + 1)
    ]
    return min(all_triples, key=lambda (a, b, c): abs(a * b * c - num_clients))


def compute_num_clients(num_server_threads):
    """Returns the number of clients to use for a number of server threads.

    Given a number of server threads, compute_num_clients returns a list of
    numbers of clients that are reasonable for the number of servers.
    """
    # num_server_threads_to_client_num_range maps the number of server threads
    # to a low (inclusive) and high (inclusive) number of clients to use.
    num_server_threads_to_client_num_range = {
        1:  (5, 25),
        2:  (10, 40),
        3:  (10, 60),
        4:  (20, 100),

        5:  (30, 100),
        6:  (30, 150),
        7:  (25, 175),
        8:  (40, 200),

        9:  (50, 150),
        10: (50, 150),
        11: (50, 150),
        12: (50, 150),

        13: (75, 325),
        14: (75, 325),
        15: (75, 325),
        16: (30, 150),

        17: (100, 400),
        18: (100, 400),
        19: (100, 400),
        20: (30, 150),

        21: (125, 475),
        22: (125, 475),
        23: (125, 475),
        24: (30, 150),

        25: (150, 550),
        26: (150, 550),
        27: (150, 550),
        28: (30, 150),

        29: (175, 625),
        30: (175, 625),
        31: (175, 625),
        32: (30, 150),
    }

    assert num_server_threads in num_server_threads_to_client_num_range
    low, high = num_server_threads_to_client_num_range[num_server_threads]
    num_clients = range(low, high + 1)

    # We don't want to sweep over every single possible number of clients
    # because it takes too long. Instead, we sweep over some of the values in
    # the range. num_entries is the number of entries in num_clients to keep.
    # We include the first and last values always.
    num_entries = 230
    num_clients = ([num_clients[0], num_clients[-1]] +
                   num_clients[::len(num_clients) / num_entries])
    return sorted(set(num_clients))

# servers is a dictionary indexed by server host name/ip address;
# each entry contains information about the rx queue names and
# their irq numbers
def setup_rx_queues(servers, queues_cnt):
    server_tasks = []
    ncpus_per_numa = 20
    for server in servers:
        iface = servers[server]['iface']
        start_port = servers[server]['start_port']
        create_rx_queues_cmd = ['sudo -k',
                                'ethtool',
                                '-L',
                                str(iface),
                                'combined',
                                str(queues_cnt)]
        enable_ntuples_cmd = ['sudo -k ethtool -K',
                              str(iface),
                              'ntuple on']
        stop_irqbalance_cmd = ['sudo service irqbalance stop']
        set_smp_affinity_cmd = []
        set_flow_rules_cmd = []
        for i in range(queues_cnt):
            # TODO: different mapping strategy on Azure
            # Following the pinning strategy of alternatively pinning on the
            # two NUMA nodes
            if i % 2 == 0:
                mask = hex(int('1' + '0'*(i/2), 2))[2:]
            else:
                mask = hex(int('1' + '0'*(ncpus_per_numa + i/2), 2))[2:]
            # insert commas if more than 8 hex chars
            if (len(mask) > 8):
                mask = mask[0:len(mask) - 8] + ',' + mask[len(mask) - 8:]
            irq = servers[server]['irq_numbers'][i]
            set_smp_affinity_cmd += ['echo {} | sudo -k tee /proc/irq/{}/smp_affinity > /dev/null'.format(mask, irq)]
            set_smp_affinity_cmd += [';']
            set_flow_rules_cmd += ['sudo ethtool -U {} flow-type udp4 dst-port {} action {} loc {}'.format(iface, start_port + i, i, i)]
            set_flow_rules_cmd += [';']

        cmd = ['('] + create_rx_queues_cmd + ['&&'] + \
              enable_ntuples_cmd + ['&&']  +          \
              stop_irqbalance_cmd + ['&&'] +          \
              ['('] + set_smp_affinity_cmd +          \
              set_flow_rules_cmd + [')'] + [')']
        print('Running {} on {}.'.format(' '.join(cmd), server.hostname))

        server_tasks.append(server.run(cmd))
    parallel_server_tasks = Parallel(server_tasks, aggregate=True)
    parallel_server_tasks.start(wait = True)

def run_benchmark(bench_dir, clients, servers, parameters):
    # Clear the clients' and servers' out and err files in /mnt/log.
    print(boxed('Clearing *_out.txt and *_err.txt'))
    clear_out_files = Parallel([host.run(['rm', '/mnt/log/*_out.txt'])
                                for host in clients + list(servers.keys())],
                                aggregate=True)
    clear_out_files.start(wait=True)
    clear_err_files = Parallel([host.run(['rm', '/mnt/log/*_err.txt'])
                                for host in clients + list(servers.keys())],
                                aggregate=True)
    clear_err_files.start(wait=True)

    # Clear the clients' log files in /mnt/log.
    print(boxed('Clearing *.log'))
    clear_log_files = Parallel([client.run(['rm', '/mnt/log/*.log'])
                                for client in clients],
                                aggregate=True)
    clear_log_files.start(wait=True)

    # Configure RX queues at the servers
    # setup_rx_queues(servers, parameters.num_server_threads)

    bench_dir.write_string('logs_cleared_time.txt', str(datetime.datetime.now()))

    # Start the servers.
    print(boxed('Starting servers.'))
    server_tasks = []
    # command to enable core dump on the server in case of SIGSEGV
    core_dump_cmd = [
        "ulimit -c unlimited; ",
        "sudo sysctl -w kernel.core_pattern=/tmp/core-%e.%p.%h.%t; ",
    ]
    # command to free buff/cache
    drop_caches_cmd = [
        "sudo bash -c \"echo 3 > /proc/sys/vm/drop_caches\"; "
    ]
    for (replica_index, server) in enumerate(sorted(list(servers.keys()), key=lambda h: h.hostname)[:2*parameters.f + 1]):
        cmd = [
            "sudo",
            # Here, we use perf to record the performance of the server. `perf
            # record` writes profiling information to the -o file specified
            # below. -g enables the collection of stack traces. --timestamp
            # allows us to use flamescope [1].
            #
            # [1]: https://github.com/Netflix/flamescope
            "perf", "record",
                "-o", "/mnt/log/server_{}_perf.data".format(replica_index),
                "-g",
                "--timestamp",
                "--",
            #"LD_PRELOAD=/homes/sys/aaasz/tapir/store/tools/Hoard/src/libhoard.so",
            #"DEBUG=all",
            #"valgrind --tool=callgrind --callgrind-out-file=/tmp/callgrind.txt",
            #"LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes",
            #"perf c2c record -F 10000 -a -g -o /tmp/perf.data --delay 60000 -- ",
            #"perf record --cpu 0 -g -o /tmp/perf.data -- ",
            #"perf stat -B -e cache-references,cache-misses,page-faults,context-switches,instructions --delay 150000",
            #"mutrace",
            parameters.server_binary,
            "--configFile", os.path.join(
                parameters.config_file_directory,
                'f{}.shard0.config'.format(parameters.f)),
            "--replicaIndex", str(replica_index),
            "--keysFile", parameters.key_file,
            "--numKeys", str(parameters.num_keys),
            "--numShards", "1",
            "--shardIndex", "0",
            "--numServerThreads", str(parameters.num_server_threads),
            "--replScheme", str(parameters.repl_scheme),
        ]

        # We capture the stdout and stderr of the servers using the trick
        # outlined in [1]. pyrem has some support for capturing stdout and
        # stderr as well, but it doesn't alway work.
        #
        # [1]: https://stackoverflow.com/a/692407/3187068
        cmd.append(('> >(tee /mnt/log/server_{0}_out.txt) ' +
                    '2> >(tee /mnt/log/server_{0}_err.txt >&2)')
                   .format(server.hostname))

        # Record (and print) the command we run, so that we can re-run it later
        # while we debug.
        print('Running {} on {}.'.format(' '.join(core_dump_cmd + cmd), server.hostname))
        bench_dir.write_string(
            'server_{}_cmd.txt'.format(server.hostname),
            ' '.join(cmd) + '\n')

        server_tasks.append(server.run(drop_caches_cmd + core_dump_cmd + cmd))
    parallel_server_tasks = Parallel(server_tasks, aggregate=True)
    parallel_server_tasks.start()

    # Wait for the servers to start.
    print(boxed('Waiting for servers to start.'))
    time.sleep(10 + 3 * parameters.num_server_threads)
    bench_dir.write_string('servers_started_time.txt', str(datetime.datetime.now()))

    # Start the clients and wait for them to finish.
    print(boxed('Starting clients at {}.'.format(datetime.datetime.now())))
    seconds = int(time.time()) # total seconds that passed since unix epoch
    client_tasks = []
    for host_i, client in enumerate(clients[:parameters.num_client_machines]):
        for client_i in range(parameters.num_clients_per_machine):
            cmd = [
                "sudo",
                #"ulimit -n 4096;" , # increase how many fds we can open
                #"DEBUG=all",
                parameters.client_binary,
                "--configFile", os.path.join(
                    parameters.config_file_directory,
                    'f{}.shard0.config'.format(parameters.f)),
                "--keysFile", parameters.key_file,
                "--numKeys", str(parameters.num_keys),
                "--numShards", "1",
                "--duration", str(parameters.benchmark_duration_seconds),
                "--warmup", str(parameters.benchmark_warmup_seconds),
                "--tLen", str(parameters.transaction_length),
                "--wPer", str(parameters.write_percentage),
                "--closestReplica", "-1",
                "--mode", "mtapir",
                "--replScheme", str(parameters.repl_scheme),
                "--numServerThreads", str(parameters.num_server_threads),
                "--zipf", str(parameters.zipf_coefficient),
                "--ncpu", str(client_i),
                "--nhost", str(host_i),
                "--numClientThreads", str(parameters.num_threads_per_client),
                "--secondsFromEpoch", str(seconds),
            ]

            # As with the servers, we record the stdout and stderr of the
            # clients. See above for details.
            cmd.append(('> >(tee /mnt/log/client_{0}_{1}_out.txt) ' +
                        '2> >(tee /mnt/log/client_{0}_{1}_err.txt >&2)')
                       .format(client.hostname, client_i))

            # Record (and print) the command we run, so that we can re-run it
            # later while we debug.
            print('Running {} on {}.'.format(' '.join(cmd), client.hostname))
            bench_dir.write_string(
                'client_{}_{}_cmd.txt'.format(client.hostname, client_i),
                ' '.join(cmd) + '\n')

            client_tasks.append(client.run(cmd, quiet=True))
    parallel_client_tasks = Parallel(client_tasks, aggregate=True)
    parallel_client_tasks.start(wait=True)
    bench_dir.write_string('clients_done_time.txt', str(datetime.datetime.now()))

    # Copy stdout and stderr files over.
    print(boxed('Copying *_out.txt and *_err.txt.'))
    for host in list(servers.keys()) + clients:
        subprocess.call([
            'scp',
            '{}:/mnt/log/*_out.txt'.format(host.hostname),
            bench_dir.path
        ])
        subprocess.call([
            'scp',
            '{}:/mnt/log/*_err.txt'.format(host.hostname),
            bench_dir.path
        ])

    # Copy the client logs over.
    print(boxed('Copying *.log.'))
    for client in clients[:parameters.num_client_machines]:
        subprocess.call([
            'scp',
            '{}:/mnt/log/*.log'.format(client.hostname),
            bench_dir.path
        ])

    bench_dir.write_string('logs_copied_time.txt', str(datetime.datetime.now()))

    # Concatenate client logs.
    print(boxed('Concatenating and processing logs.'))
    subprocess.call([
        'cat {0}/*.log > {0}/client.log'.format(bench_dir.path)
    ], shell=True)

    # Process logs.
    try:
        results = process_client_logs(
            '{}/client.log'.format(bench_dir.path),
            warmup_sec=parameters.benchmark_warmup_seconds,
            duration_sec=parameters.benchmark_duration_seconds -
                         2*parameters.benchmark_warmup_seconds)
        bench_dir.write_dict('results.json', results._asdict())
    except ValueError:
        results = None

    bench_dir.write_string('logs_processed_time.txt', str(datetime.datetime.now()))

    # Kill the servers.
    print(boxed('Killing servers.'))
    # We can't use PyREM's stop function because we need sudo priviledges
    #parallel_server_tasks.stop()
    kill_tasks = []
    for (replica_index, server) in enumerate(sorted(list(servers.keys()), key=lambda h: h.hostname)[:2*parameters.f + 1]):
        cmd = [
            "sudo kill -INT `pgrep meerkat_server`",
        ]

        # Record (and print) the command we run, so that we can re-run it later
        # while we debug.
        print('Running {} on {}.'.format(' '.join(cmd), server.hostname))
        kill_tasks.append(server.run(cmd))
    parallel_kill_tasks = Parallel(kill_tasks, aggregate=True)
    parallel_kill_tasks.start(wait=True)

    bench_dir.write_string('servers_killed_time.txt', str(datetime.datetime.now()))

    return results

def run_suite(suite_dir, parameters_list, clients, servers):
    # Store the list of parameters.
    parameters_strings = '\n'.join(str(p) for p in parameters_list)
    suite_dir.write_string('parameters_list.txt', parameters_strings)

    # Incrementally write out benchmark results.
    with suite_dir.create_file('results.csv') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(ParametersAndResult._fields)

        # Run benchmarks.
        for (i, parameters) in enumerate(parameters_list, 1):
            with suite_dir.new_benchmark_directory() as bench_dir:
                print(boxed('Running experiment {} / {} in {}'
                            .format(i, len(parameters_list), bench_dir.path)))
                bench_dir.write_dict('parameters.json', parameters._asdict())
                bench_dir.write_string('parameters.txt', str(parameters))

                result = run_benchmark(bench_dir, clients, servers, parameters)
                if result:
                    csv_writer.writerow(ParametersAndResult(*(parameters + result)))
                    f.flush()

def main(args):
    # Set benchmark parameters.
    base_parameters = Parameters(
        config_file_directory=args.config_file_directory,
        f=0,
        key_file=args.key_file,
        num_keys=1000 * 1000,
        num_server_threads=1,
        repl_scheme='ir',
        server_binary=args.server_binary,
        client_binary=args.client_binary,
        benchmark_duration_seconds=60,
        benchmark_warmup_seconds=15,
        transaction_length=2,
        write_percentage=50,
        zipf_coefficient=0,
        num_client_machines=1,
        num_clients_per_machine=1,
        num_threads_per_client=1,
        suite_directory=args.suite_directory,
    )
    parameters_list = [
      base_parameters._replace(
          f=f,
          zipf_coefficient=zipf_coefficient,
          transaction_length=transaction_length,
          write_percentage=write_percentage,
          num_server_threads=num_server_threads,
          repl_scheme=repl_scheme,
          num_client_machines=num_client_machines,
          num_clients_per_machine=num_clients_per_machine,
          num_threads_per_client=1,
      )
      for transaction_length in [2]
      for write_percentage in [50]
      for zipf_coefficient in [0]
      for f in [1]
      #for num_server_threads in [1, 2, 4, 8, 12, 16, 20, 24, 28, 32]
      for num_server_threads in [40]
      for repl_scheme in ['ir']
      for (num_client_machines, num_clients_per_machine) in [(5, 40)]
      #for (num_client_machines, num_clients_per_machine) in [(3,20), (3,24), (4,21), (6,16), (6,18), (6,20),
      #(6,22), (6,24), (7,24)]
      #for (num_client_machines, num_clients_per_machine) in [(1,1), (1,12),
      #(1,24), (2,18), (2,24), (3,20), (3,24), (4,21), (6,16), (6,18), (6,20),
      #(6,22), (6,24), (7,24)]
      #for (num_client_machines, num_clients_per_machine, num_threads_per_client)
      # in [num_clients_to_triple(i) for i in compute_num_clients(num_server_threads)]
    ]
    # Run every experiment three times.
    #parameters_list = [q for p in parameters_list for q in [p] * 10]

    # Run the suite.
    suite_dir = benchmark.SuiteDirectory(args.suite_directory)
    suite_dir.write_dict('args.json', vars(args))
    run_suite(suite_dir, parameters_list, zookeeper_clients(), zookeeper_servers())

def parser():
    # https://stackoverflow.com/a/4028943/3187068
    home_dir = os.path.expanduser('~')

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--server_binary',
        type=str,
        required=True,
        help='The silo server binary.')
    parser.add_argument(
        '--client_binary',
        type=str,
        required=True,
        help='The silo client benchmark binary.')
    parser.add_argument(
        '--config_file_directory',
        type=str,
        default=os.path.join(home_dir, 'tapir_benchmarks'),
        help='The directory in which TAPIR shard files are stored.')
    parser.add_argument(
        '--key_file',
        type=str,
        default=os.path.join(home_dir, 'tapir_benchmarks/keys.txt'),
        help='The TAPIR keys file.')
    parser.add_argument(
        '--suite_directory',
        type=str,
        default=os.path.join(home_dir, 'tmp'),
        help='The directory into which benchmark results are written.')
    return parser


def parse_args():
    args = parser().parse_args()

    # Sanity check command line arguments.
    assert os.path.exists(args.config_file_directory)
    assert os.path.isfile(args.key_file)
    assert os.path.exists(args.suite_directory)

    return args

if __name__ == '__main__':
    main(parse_args())
