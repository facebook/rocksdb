#!/usr/bin/env python3
import csv
import math
import os
import random
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.backends.backend_pdf
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns


# Make sure a legend has the same color across all generated graphs.
def get_cmap(n, name="hsv"):
    """Returns a function that maps each index in 0, 1, ..., n-1 to a distinct
    RGB color; the keyword argument name must be a standard mpl colormap name."""
    return plt.cm.get_cmap(name, n)


color_index = 0
bar_color_maps = {}
colors = []
n_colors = 360
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


def plot_miss_stats_graphs(
    csv_result_dir, output_result_dir, file_prefix, file_suffix, ylabel, pdf_file_name
):
    miss_ratios = {}
    for file in os.listdir(csv_result_dir):
        if not file.startswith(file_prefix):
            continue
        if not file.endswith(file_suffix):
            continue
        print("Processing file {}/{}".format(csv_result_dir, file))
        mrc_file_path = csv_result_dir + "/" + file
        with open(mrc_file_path, "r") as csvfile:
            rows = csv.reader(csvfile, delimiter=",")
            for row in rows:
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
                miss_ratios[config]["x"].append(capacity)
                miss_ratios[config]["y"].append(miss_ratio)
            fig = plt.figure()
            for config in miss_ratios:
                plt.plot(
                    miss_ratios[config]["x"], miss_ratios[config]["y"], label=config
                )
            plt.xlabel("Cache capacity")
            plt.ylabel(ylabel)
            plt.xscale("log", basex=2)
            plt.ylim(ymin=0)
            plt.title("{}".format(file))
            plt.legend()
            fig.savefig(
                output_result_dir + "/{}.pdf".format(pdf_file_name), bbox_inches="tight"
            )


def plot_miss_stats_diff_lru_graphs(
    csv_result_dir, output_result_dir, file_prefix, file_suffix, ylabel, pdf_file_name
):
    miss_ratios = {}
    for file in os.listdir(csv_result_dir):
        if not file.startswith(file_prefix):
            continue
        if not file.endswith(file_suffix):
            continue
        print("Processing file {}/{}".format(csv_result_dir, file))
        mrc_file_path = csv_result_dir + "/" + file
        with open(mrc_file_path, "r") as csvfile:
            rows = csv.reader(csvfile, delimiter=",")
            for row in rows:
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
                miss_ratios[config]["x"].append(capacity)
                miss_ratios[config]["y"].append(miss_ratio)
    if "lru-0-0" not in miss_ratios:
        return
    fig = plt.figure()
    for config in miss_ratios:
        diffs = [0] * len(miss_ratios["lru-0-0"]["x"])
        for i in range(len(miss_ratios["lru-0-0"]["x"])):
            for j in range(len(miss_ratios[config]["x"])):
                if miss_ratios["lru-0-0"]["x"][i] == miss_ratios[config]["x"][j]:
                    diffs[i] = (
                        miss_ratios[config]["y"][j] - miss_ratios["lru-0-0"]["y"][i]
                    )
                    break
        plt.plot(miss_ratios["lru-0-0"]["x"], diffs, label=config)
    plt.xlabel("Cache capacity")
    plt.ylabel(ylabel)
    plt.xscale("log", basex=2)
    plt.title("{}".format(file))
    plt.legend()
    fig.savefig(
        output_result_dir + "/{}.pdf".format(pdf_file_name), bbox_inches="tight"
    )


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
    filename_prefix,
    filename_suffix,
    pdf_name,
    xlabel,
    ylabel,
    title,
    vertical,
    legend,
):
    global color_index, bar_color_maps, colors
    pdf = matplotlib.backends.backend_pdf.PdfPages(output_result_dir + "/" + pdf_name)
    for file in os.listdir(csv_result_dir):
        if not file.endswith(filename_suffix):
            continue
        if not file.startswith(filename_prefix):
            continue
        print("Processing file {}/{}".format(csv_result_dir, file))
        with open(csv_result_dir + "/" + file, "r") as csvfile:
            x, labels, label_stats = read_data_for_plot(csvfile, vertical)
            if len(x) == 0 or len(labels) == 0:
                continue
            # plot figure
            fig = plt.figure()
            for label_index in label_stats:
                # Assign a unique color to this label.
                if labels[label_index] not in bar_color_maps:
                    bar_color_maps[labels[label_index]] = colors[color_index]
                    color_index += 1
                plt.plot(
                    [int(x[i]) for i in range(len(x) - 1)],
                    label_stats[label_index][:-1],
                    label=labels[label_index],
                    color=bar_color_maps[labels[label_index]],
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


def plot_heatmap(csv_result_dir, output_result_dir, filename_suffix, pdf_name, title):
    pdf = matplotlib.backends.backend_pdf.PdfPages(
        "{}/{}".format(output_result_dir, pdf_name)
    )
    for file in os.listdir(csv_result_dir):
        if not file.endswith(filename_suffix):
            continue
        csv_file_name = "{}/{}".format(csv_result_dir, file)
        print("Processing file {}/{}".format(csv_result_dir, file))
        corr_table = pd.read_csv(csv_file_name)
        corr_table = corr_table.pivot("label", "corr", "value")
        fig = plt.figure()
        sns.heatmap(corr_table, annot=True, linewidths=0.5, fmt=".2")
        plt.title("{} filename:{}".format(title, file))
        pdf.savefig(fig)
    pdf.close()


def plot_timeline(csv_result_dir, output_result_dir):
    plot_line_charts(
        csv_result_dir,
        output_result_dir,
        filename_prefix="",
        filename_suffix="access_timeline",
        pdf_name="access_time.pdf",
        xlabel="Time",
        ylabel="Throughput",
        title="Access timeline with group by label",
        vertical=False,
        legend=True,
    )


def convert_to_0_if_nan(n):
    if math.isnan(n):
        return 0.0
    return n


def plot_correlation(csv_result_dir, output_result_dir):
    # Processing the correlation input first.
    label_str_file = {}
    for file in os.listdir(csv_result_dir):
        if not file.endswith("correlation_input"):
            continue
        csv_file_name = "{}/{}".format(csv_result_dir, file)
        print("Processing file {}/{}".format(csv_result_dir, file))
        corr_table = pd.read_csv(csv_file_name)
        label_str = file.split("_")[0]
        label = file[len(label_str) + 1 :]
        label = label[: len(label) - len("_correlation_input")]

        output_file = "{}/{}_correlation_output".format(csv_result_dir, label_str)
        if output_file not in label_str_file:
            f = open("{}/{}_correlation_output".format(csv_result_dir, label_str), "w+")
            label_str_file[output_file] = f
            f.write("label,corr,value\n")
        f = label_str_file[output_file]
        f.write(
            "{},{},{}\n".format(
                label,
                "LA+A",
                convert_to_0_if_nan(
                    corr_table["num_accesses_since_last_access"].corr(
                        corr_table["num_accesses_till_next_access"], method="spearman"
                    )
                ),
            )
        )
        f.write(
            "{},{},{}\n".format(
                label,
                "PA+A",
                convert_to_0_if_nan(
                    corr_table["num_past_accesses"].corr(
                        corr_table["num_accesses_till_next_access"], method="spearman"
                    )
                ),
            )
        )
        f.write(
            "{},{},{}\n".format(
                label,
                "LT+A",
                convert_to_0_if_nan(
                    corr_table["elapsed_time_since_last_access"].corr(
                        corr_table["num_accesses_till_next_access"], method="spearman"
                    )
                ),
            )
        )
        f.write(
            "{},{},{}\n".format(
                label,
                "LA+T",
                convert_to_0_if_nan(
                    corr_table["num_accesses_since_last_access"].corr(
                        corr_table["elapsed_time_till_next_access"], method="spearman"
                    )
                ),
            )
        )
        f.write(
            "{},{},{}\n".format(
                label,
                "LT+T",
                convert_to_0_if_nan(
                    corr_table["elapsed_time_since_last_access"].corr(
                        corr_table["elapsed_time_till_next_access"], method="spearman"
                    )
                ),
            )
        )
        f.write(
            "{},{},{}\n".format(
                label,
                "PA+T",
                convert_to_0_if_nan(
                    corr_table["num_past_accesses"].corr(
                        corr_table["elapsed_time_till_next_access"], method="spearman"
                    )
                ),
            )
        )
    for label_str in label_str_file:
        label_str_file[label_str].close()

    plot_heatmap(
        csv_result_dir,
        output_result_dir,
        "correlation_output",
        "correlation.pdf",
        "Correlation",
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
        filename_prefix="",
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
    plot_line_charts(
        csv_result_dir,
        output_result_dir,
        filename_prefix="",
        filename_suffix="skewness",
        pdf_name="skew.pdf",
        xlabel="",
        ylabel="Percentage of accesses",
        title="Skewness",
        vertical=True,
        legend=False,
    )


def plot_miss_ratio_timeline(csv_result_dir, output_result_dir):
    plot_line_charts(
        csv_result_dir,
        output_result_dir,
        filename_prefix="",
        filename_suffix="3600_miss_ratio_timeline",
        pdf_name="miss_ratio_timeline.pdf",
        xlabel="Time",
        ylabel="Miss Ratio (%)",
        title="Miss ratio timeline",
        vertical=False,
        legend=True,
    )
    plot_line_charts(
        csv_result_dir,
        output_result_dir,
        filename_prefix="",
        filename_suffix="3600_miss_timeline",
        pdf_name="miss_timeline.pdf",
        xlabel="Time",
        ylabel="# of misses ",
        title="Miss timeline",
        vertical=False,
        legend=True,
    )
    plot_line_charts(
        csv_result_dir,
        output_result_dir,
        filename_prefix="",
        filename_suffix="3600_miss_timeline",
        pdf_name="miss_timeline.pdf",
        xlabel="Time",
        ylabel="# of misses ",
        title="Miss timeline",
        vertical=False,
        legend=True,
    )
    plot_line_charts(
        csv_result_dir,
        output_result_dir,
        filename_prefix="",
        filename_suffix="3600_policy_timeline",
        pdf_name="policy_timeline.pdf",
        xlabel="Time",
        ylabel="# of times a policy is selected ",
        title="Policy timeline",
        vertical=False,
        legend=True,
    )
    plot_line_charts(
        csv_result_dir,
        output_result_dir,
        filename_prefix="",
        filename_suffix="3600_policy_ratio_timeline",
        pdf_name="policy_ratio_timeline.pdf",
        xlabel="Time",
        ylabel="Percentage of times a policy is selected ",
        title="Policy timeline",
        vertical=False,
        legend=True,
    )


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(
            "Must provide two arguments: \n"
            "1) The directory that saves a list of "
            "directories which contain block cache trace analyzer result files. \n"
            "2) the directory to save plotted graphs. \n"
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
        plot_access_count_summary(csv_abs_dir, result_dir)
        plot_timeline(csv_abs_dir, result_dir)
        plot_miss_ratio_timeline(csv_result_dir, output_result_dir)
        plot_correlation(csv_abs_dir, result_dir)
        plot_reuse_graphs(csv_abs_dir, result_dir)
        plot_percentage_access_summary(csv_abs_dir, result_dir)
        plot_miss_stats_graphs(
            csv_abs_dir,
            result_dir,
            file_prefix="",
            file_suffix="mrc",
            ylabel="Miss ratio (%)",
            pdf_file_name="mrc",
        )
        plot_miss_stats_diff_lru_graphs(
            csv_abs_dir,
            result_dir,
            file_prefix="",
            file_suffix="mrc",
            ylabel="Miss ratio (%)",
            pdf_file_name="mrc_diff_lru",
        )
        # The following stats are only available in pysim.
        for time_unit in ["1", "60", "3600"]:
            plot_miss_stats_graphs(
                csv_abs_dir,
                result_dir,
                file_prefix="ml_{}_".format(time_unit),
                file_suffix="p95mb",
                ylabel="p95 number of byte miss per {} seconds".format(time_unit),
                pdf_file_name="p95mb_per{}_seconds".format(time_unit),
            )
            plot_miss_stats_graphs(
                csv_abs_dir,
                result_dir,
                file_prefix="ml_{}_".format(time_unit),
                file_suffix="avgmb",
                ylabel="Average number of byte miss per {} seconds".format(time_unit),
                pdf_file_name="avgmb_per{}_seconds".format(time_unit),
            )
            plot_miss_stats_diff_lru_graphs(
                csv_abs_dir,
                result_dir,
                file_prefix="ml_{}_".format(time_unit),
                file_suffix="p95mb",
                ylabel="p95 number of byte miss per {} seconds".format(time_unit),
                pdf_file_name="p95mb_per{}_seconds_diff_lru".format(time_unit),
            )
            plot_miss_stats_diff_lru_graphs(
                csv_abs_dir,
                result_dir,
                file_prefix="ml_{}_".format(time_unit),
                file_suffix="avgmb",
                ylabel="Average number of byte miss per {} seconds".format(time_unit),
                pdf_file_name="avgmb_per{}_seconds_diff_lru".format(time_unit),
            )
