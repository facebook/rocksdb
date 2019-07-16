#!/usr/bin/env python3
import csv
import os
import random
import sys

import matplotlib.backends.backend_pdf
import matplotlib.pyplot as plt
import numpy as np


# Make sure a legend has the same color across all generated graphs.
def get_cmap(n, name="hsv"):
    """Returns a function that maps each index in 0, 1, ..., n-1 to a distinct
    RGB color; the keyword argument name must be a standard mpl colormap name."""
    return plt.cm.get_cmap(name, n)


color_index = 0
bar_color_maps = {}
colors = []
n_colors = 60
linear_colors = get_cmap(n_colors)
for i in range(n_colors):
    colors.append(linear_colors(i))
# Shuffle the colors so that adjacent bars in a graph are obvious to differentiate.
random.shuffle(colors)


def num_to_gb(n):
    one_gb = 1024 * 1024 * 1024
    if float(n) % one_gb == 0:
        return "{}".format(n / one_gb)
    # Keep two decimal points.
    return "{0:.2f}".format(float(n) / one_gb)


def plot_miss_ratio_graphs(csv_result_dir, output_result_dir):
    mrc_file_path = csv_result_dir + "/mrc"
    if not os.path.exists(mrc_file_path):
        return
    miss_ratios = {}
    print("Processing file {}".format(mrc_file_path))
    with open(mrc_file_path, "r") as csvfile:
        rows = csv.reader(csvfile, delimiter=",")
        is_header = False
        for row in rows:
            if not is_header:
                is_header = True
                continue
            cache_name = row[0]
            num_shard_bits = int(row[1])
            ghost_capacity = int(row[2])
            capacity = int(row[3])
            miss_ratio = float(row[4])
            config = "{}-{}-{}".format(cache_name, num_shard_bits, ghost_capacity)
            if config not in miss_ratios:
                miss_ratios[config] = {}
                miss_ratios[config]["x"] = []
                miss_ratios[config]["y"] = []
            miss_ratios[config]["x"].append(num_to_gb(capacity))
            miss_ratios[config]["y"].append(miss_ratio)
    fig = plt.figure()
    for config in miss_ratios:
        plt.plot(miss_ratios[config]["x"], miss_ratios[config]["y"], label=config)
    plt.xlabel("Cache capacity (GB)")
    plt.ylabel("Miss Ratio (%)")
    # plt.xscale('log', basex=2)
    plt.ylim(ymin=0)
    plt.title("RocksDB block cache miss ratios")
    plt.legend()
    fig.savefig(output_result_dir + "/mrc.pdf", bbox_inches="tight")


def sanitize(label):
    # matplotlib cannot plot legends that is prefixed with "_"
    # so we need to remove them here.
    index = 0
    for i in range(len(label)):
        if label[i] == "_":
            index += 1
        else:
            break
    data = label[index:]
    # The value of uint64_max in c++.
    if "18446744073709551615" in data:
        return "max"
    return data


# Read the csv file vertically, i.e., group the data by columns.
def read_data_for_plot_vertical(csvfile):
    x = []
    labels = []
    label_stats = {}
    csv_rows = csv.reader(csvfile, delimiter=",")
    data_rows = []
    for row in csv_rows:
        data_rows.append(row)
    # header
    for i in range(1, len(data_rows[0])):
        labels.append(sanitize(data_rows[0][i]))
        label_stats[i - 1] = []
    for i in range(1, len(data_rows)):
        for j in range(len(data_rows[i])):
            if j == 0:
                x.append(sanitize(data_rows[i][j]))
                continue
            label_stats[j - 1].append(float(data_rows[i][j]))
    return x, labels, label_stats


# Read the csv file horizontally, i.e., group the data by rows.
def read_data_for_plot_horizontal(csvfile):
    x = []
    labels = []
    label_stats = {}
    csv_rows = csv.reader(csvfile, delimiter=",")
    data_rows = []
    for row in csv_rows:
        data_rows.append(row)
    # header
    for i in range(1, len(data_rows)):
        labels.append(sanitize(data_rows[i][0]))
        label_stats[i - 1] = []
    for i in range(1, len(data_rows[0])):
        x.append(sanitize(data_rows[0][i]))
    for i in range(1, len(data_rows)):
        for j in range(len(data_rows[i])):
            if j == 0:
                # label
                continue
            label_stats[i - 1].append(float(data_rows[i][j]))
    return x, labels, label_stats


def read_data_for_plot(csvfile, vertical):
    if vertical:
        return read_data_for_plot_vertical(csvfile)
    return read_data_for_plot_horizontal(csvfile)


def plot_line_charts(
    csv_result_dir,
    output_result_dir,
    filename_suffix,
    pdf_name,
    xlabel,
    ylabel,
    title,
    vertical,
    legend,
):
    pdf = matplotlib.backends.backend_pdf.PdfPages(output_result_dir + "/" + pdf_name)
    for file in os.listdir(csv_result_dir):
        if not file.endswith(filename_suffix):
            continue
        print("Processing file {}".format(file))
        with open(csv_result_dir + "/" + file, "r") as csvfile:
            x, labels, label_stats = read_data_for_plot(csvfile, vertical)
            if len(x) == 0 or len(labels) == 0:
                continue
            # plot figure
            fig = plt.figure()
            for label_index in label_stats:
                plt.plot(
                    [int(x[i]) for i in range(len(x))],
                    label_stats[label_index],
                    label=labels[label_index],
                )

            # Translate time unit into x labels.
            if "_60" in file:
                plt.xlabel("{} (Minute)".format(xlabel))
            if "_3600" in file:
                plt.xlabel("{} (Hour)".format(xlabel))
            plt.ylabel(ylabel)
            plt.title("{} {}".format(title, file))
            if legend:
                plt.legend()
            pdf.savefig(fig)
    pdf.close()


def plot_stacked_bar_charts(
    csv_result_dir,
    output_result_dir,
    filename_suffix,
    pdf_name,
    xlabel,
    ylabel,
    title,
    vertical,
    x_prefix,
):
    global color_index, bar_color_maps, colors
    pdf = matplotlib.backends.backend_pdf.PdfPages(
        "{}/{}".format(output_result_dir, pdf_name)
    )
    for file in os.listdir(csv_result_dir):
        if not file.endswith(filename_suffix):
            continue
        with open(csv_result_dir + "/" + file, "r") as csvfile:
            print("Processing file {}/{}".format(csv_result_dir, file))
            x, labels, label_stats = read_data_for_plot(csvfile, vertical)
            if len(x) == 0 or len(label_stats) == 0:
                continue
            # Plot figure
            fig = plt.figure()
            ind = np.arange(len(x))  # the x locations for the groups
            width = 0.5  # the width of the bars: can also be len(x) sequence
            bars = []
            bottom_bars = []
            for _i in label_stats[0]:
                bottom_bars.append(0)
            for i in range(0, len(label_stats)):
                # Assign a unique color to this label.
                if labels[i] not in bar_color_maps:
                    bar_color_maps[labels[i]] = colors[color_index]
                    color_index += 1
                p = plt.bar(
                    ind,
                    label_stats[i],
                    width,
                    bottom=bottom_bars,
                    color=bar_color_maps[labels[i]],
                )
                bars.append(p[0])
                for j in range(len(label_stats[i])):
                    bottom_bars[j] += label_stats[i][j]
            plt.xlabel(xlabel)
            plt.ylabel(ylabel)
            plt.xticks(
                ind, [x_prefix + x[i] for i in range(len(x))], rotation=20, fontsize=8
            )
            plt.legend(bars, labels)
            plt.title("{} filename:{}".format(title, file))
            pdf.savefig(fig)
    pdf.close()


def plot_access_timeline(csv_result_dir, output_result_dir):
    plot_line_charts(
        csv_result_dir,
        output_result_dir,
        filename_suffix="access_timeline",
        pdf_name="access_time.pdf",
        xlabel="Time",
        ylabel="Throughput",
        title="Access timeline with group by label",
        vertical=False,
        legend=True,
    )


def plot_reuse_graphs(csv_result_dir, output_result_dir):
    plot_stacked_bar_charts(
        csv_result_dir,
        output_result_dir,
        filename_suffix="avg_reuse_interval_naccesses",
        pdf_name="avg_reuse_interval_naccesses.pdf",
        xlabel="",
        ylabel="Percentage of accesses",
        title="Average reuse interval",
        vertical=True,
        x_prefix="< ",
    )
    plot_stacked_bar_charts(
        csv_result_dir,
        output_result_dir,
        filename_suffix="avg_reuse_interval",
        pdf_name="avg_reuse_interval.pdf",
        xlabel="",
        ylabel="Percentage of blocks",
        title="Average reuse interval",
        vertical=True,
        x_prefix="< ",
    )
    plot_stacked_bar_charts(
        csv_result_dir,
        output_result_dir,
        filename_suffix="access_reuse_interval",
        pdf_name="reuse_interval.pdf",
        xlabel="Seconds",
        ylabel="Percentage of accesses",
        title="Reuse interval",
        vertical=True,
        x_prefix="< ",
    )
    plot_stacked_bar_charts(
        csv_result_dir,
        output_result_dir,
        filename_suffix="reuse_lifetime",
        pdf_name="reuse_lifetime.pdf",
        xlabel="Seconds",
        ylabel="Percentage of blocks",
        title="Reuse lifetime",
        vertical=True,
        x_prefix="< ",
    )
    plot_line_charts(
        csv_result_dir,
        output_result_dir,
        filename_suffix="reuse_blocks_timeline",
        pdf_name="reuse_blocks_timeline.pdf",
        xlabel="",
        ylabel="Percentage of blocks",
        title="Reuse blocks timeline",
        vertical=False,
        legend=False,
    )


def plot_percentage_access_summary(csv_result_dir, output_result_dir):
    plot_stacked_bar_charts(
        csv_result_dir,
        output_result_dir,
        filename_suffix="percentage_of_accesses_summary",
        pdf_name="percentage_access.pdf",
        xlabel="",
        ylabel="Percentage of accesses",
        title="",
        vertical=True,
        x_prefix="",
    )
    plot_stacked_bar_charts(
        csv_result_dir,
        output_result_dir,
        filename_suffix="percent_ref_keys",
        pdf_name="percent_ref_keys.pdf",
        xlabel="",
        ylabel="Percentage of blocks",
        title="",
        vertical=True,
        x_prefix="",
    )
    plot_stacked_bar_charts(
        csv_result_dir,
        output_result_dir,
        filename_suffix="percent_data_size_on_ref_keys",
        pdf_name="percent_data_size_on_ref_keys.pdf",
        xlabel="",
        ylabel="Percentage of blocks",
        title="",
        vertical=True,
        x_prefix="",
    )
    plot_stacked_bar_charts(
        csv_result_dir,
        output_result_dir,
        filename_suffix="percent_accesses_on_ref_keys",
        pdf_name="percent_accesses_on_ref_keys.pdf",
        xlabel="",
        ylabel="Percentage of blocks",
        title="",
        vertical=True,
        x_prefix="",
    )


def plot_access_count_summary(csv_result_dir, output_result_dir):
    plot_stacked_bar_charts(
        csv_result_dir,
        output_result_dir,
        filename_suffix="access_count_summary",
        pdf_name="access_count_summary.pdf",
        xlabel="Access count",
        ylabel="Percentage of blocks",
        title="",
        vertical=True,
        x_prefix="< ",
    )


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(
            "Must provide two arguments: 1) The directory that saves a list of "
            "directories which contain block cache trace analyzer result files "
            "2) the directory to save plotted graphs."
        )
        exit(1)
    csv_result_dir = sys.argv[1]
    output_result_dir = sys.argv[2]
    print(
        "Processing directory {} and save graphs to {}.".format(
            csv_result_dir, output_result_dir
        )
    )
    for csv_relative_dir in os.listdir(csv_result_dir):
        csv_abs_dir = csv_result_dir + "/" + csv_relative_dir
        result_dir = output_result_dir + "/" + csv_relative_dir
        if not os.path.isdir(csv_abs_dir):
            print("{} is not a directory".format(csv_abs_dir))
            continue
        print("Processing experiment dir: {}".format(csv_relative_dir))
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)
        plot_miss_ratio_graphs(csv_abs_dir, result_dir)
        plot_access_timeline(csv_abs_dir, result_dir)
        plot_reuse_graphs(csv_abs_dir, result_dir)
        plot_percentage_access_summary(csv_abs_dir, result_dir)
        plot_access_count_summary(csv_abs_dir, result_dir)
