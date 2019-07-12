# Run like this: `python e6_plot.py e6.csv`.

from textwrap import wrap
from typing import Any, Dict, Iterable, List, NamedTuple, Tuple
import argparse
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

class LineData(NamedTuple):
    """Line data is data for a single line in a chart."""
    # The line's independent variable values.
    label: Dict[str, Any]

    # The line's mean values.
    mean: pd.Series

    # The std of the mean values.
    std: pd.Series

def sanitize(x: Any) -> Any:
    if isinstance(x, float):
        return '%.3f' % x
    else:
        return x

def sanitize_iterable(t: Iterable[Any]) -> Tuple:
    return tuple(sanitize(x) for x in t)

def plot(filename: str, ylabel: str, line_datas: List[LineData]) -> None:
    """Plot data.

    See https://stackoverflow.com/a/12958534/3187068.
    """
    matplotlib.rc('font', size=12)
    fig, ax = plt.subplots()

    # We want to color ir and lir experiments the same color, with one dashed
    # and one solid. To do this, we remember the color we used for a particular
    # line and re-use it across ir and lir lines.
    color_map: Dict[Any, str] = {}

    for line_data in line_datas:
        label = line_data.label
        mean = line_data.mean
        std = line_data.std

        sorted_label = list(sorted(label.items()))
        sorted_keys = [k for (k, v) in sorted_label]
        sorted_values = [v for (k, v) in sorted_label]
        no_repl_label = [(k, v) for (k, v) in sorted_label if k != 'repl_scheme']
        frozen = frozenset(no_repl_label)

        linestyle = ('dashed'
                     if 'repl_scheme' in label and label['repl_scheme'] == 'ir'
                     else 'solid')
        color = color_map.get(frozen, None)
        lines = ax.plot(
            mean,
            marker='o',
            color=color,
            linestyle=linestyle,
            label=sanitize_iterable(label.values()))
        assert len(lines) == 1, lines
        color = lines[0].get_color()
        color_map[frozen] = color
        ax.fill_between(
            std.index,
            mean - std,
            mean + std,
            color=color,
            alpha=0.25)

    title = f'{ylabel} for various values of {sorted_keys}'
    ax.set_title('\n'.join(wrap(title, 40)))
    ax.set_xlabel('number of server threads')
    ax.set_ylabel(ylabel)
    ax.grid()
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    fig.set_tight_layout(True)
    fig.savefig(filename)
    plt.close()

def main(data_filename: str) -> None:
    # Read in the data to a dataframe.
    df = pd.read_csv(data_filename)

    # Every experiment records a set of outputs for various values of
    # independent variables (e.g., throughput and latency for various values of
    # num_server_threads and zipf_coefficient). We create one plot for every
    # output. For each of these plots, one of the independent variables is the
    # x-axis. We create one line for every set of the remaining independent
    # variables.
    #
    # For example, if we record the throughput and latency for various values
    # of num_server_threads and zipf_coefficient, we can select
    # num_server_threads to be the x-axis. Then, we get two charts: one for
    # throughput and one for latency. On each chart, we get one line for every
    # value of zipf_coefficient.
    #
    # Here, we gather the set of independent variables, ignoring the x-axis one.
    ignore = {'zipf_coefficient'}
    outputs = {
        'num_transactions',
        'num_successful_transactions',
        'num_failed_transactions',
        'abort_rate',
        'throughput_all',
        'throughput_success',
        'throughput_failure',
        'average_latency_all',
        'median_latency_all',
        'average_latency_success',
        'median_latency_success',
        'average_latency_failure',
        'median_latency_failure',
    }
    varied_columns = [c for c in df
                      if df[c].nunique() > 1 and
                      c not in ignore | outputs]

    # Create the charts.
    for output in outputs:
        line_datas: List[LineData] = []
        if varied_columns:
            grouped = df.groupby(varied_columns)
            for (name, group) in grouped:
                if type(name) != list and type(name) != tuple:
                    name = [name]
                by_num_server_threads = group.groupby('zipf_coefficient')
                line_datas.append(LineData(
                    label={k: v for (k, v) in zip(varied_columns, name)},
                    mean=by_num_server_threads[output].mean(),
                    std=by_num_server_threads[output].std()))
        else:
            by_num_server_threads = df.groupby('zipf_coefficient')
            line_datas.append(LineData(
                label=dict(),
                mean=by_num_server_threads[output].mean(),
                std=by_num_server_threads[output].std()))

        filename = f'e6_{output}.pdf'
        plot(filename, output, line_datas)
        print(f'Wrote plot to {filename}.')

def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'data',
        type=str,
        help='Experiment data')
    return parser

if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    main(args.data)
