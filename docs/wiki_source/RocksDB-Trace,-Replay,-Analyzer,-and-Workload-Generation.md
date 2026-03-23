# Query Tracing and Replaying

The trace_replay APIs allow the user to track down the query information to a trace file. In the current implementation, **Get**, **WriteBatch** (Put, Delete, Merge, SingleDelete, and DeleteRange), **Iterator** (Seek and SeekForPrev) and **MultiGet** are the queries that tracked by the trace-replay API. Key, query timestamp, value (if applied), column family ID*(s) form one trace record. For Iterator, we also track the lower and upper bound if they are specified in the Iterator. For MultiGet, we serialize the whole multi-get information (a vector or timestamp, keys, and column family IDs) in one record.

Since one lock is used to protect the tracing instance and there will be extra I/Os for the trace file, the performance of DB will be influenced. According to the current test on the MyRocks and ZippyDB shadow server, the performance hasn't been a concern in those shadows. The trace records from the same DB instance are written to a binary trace file. User can specify the path of trace file (e.g., store in different storage devices to reduce the I/O influence).

Currently, the trace file can be replayed via `Replayer` or by using the [db_bench](/facebook/rocksdb/wiki/Benchmarking-tools#db_bench). The queries records in the traces file can be replayed to the target DB instance according to the time stamps. It can replay the workload nearly the same as the workload being collected, which will provide a more production-like testing case. If you want to replay the workload in a precise way. Right before you start tracing, you can create a hard link of the DB directory to another location (snapshot), delete the manifest in the copied directory, make a real copy of the manifest to the copied directory. Now, you can safely copy the snapshot to the location you want to replay.

An simple example to use the tracing APIs to generate a trace file:


```C++
Options opt;
Env* env = rocksdb::Env::Default();
EnvOptions env_options(opt);
std::string trace_path = "/tmp/trace_test_example";
std::unique_ptr<TraceWriter> trace_writer;
DB* db = nullptr;
std::string db_name = "/tmp/rocksdb";

// Create the trace file writer
NewFileTraceWriter(env, env_options, trace_path, &trace_writer);
DB::Open(opt, db_name, &db);

// Start tracing
TraceOptions trace_opt;
db->StartTrace(trace_opt, std::move(trace_writer));

/* your call of RocksDB APIs */

// End tracing
db->EndTrace()
```

To automatically replay the trace via `Replayer`:

```C++
Status s;
DB* db;
std::vector<ColumnFamilyHandle*> handles;

/* Open db and get db handles */

// Create TraceReader
std::string trace_path = "/tmp/trace_test_example";
std::unique_ptr<TraceReader> trace_reader;
s = NewFileTraceReader(db->GetEnv(), EnvOptions(), trace_path, &trace_reader);

// Create Replayer
std::unique_ptr<Replayer> replayer;
s = db->NewDefaultReplayer(handles, std::move(trace_reader), &replayer);

// Prepare (read trace header)
s = replayer->Prepare();

// Automatically replay (4 threads, 2x spped)
s = replayer->Replay(ReplayOptions(4, 2.0), nullptr);

/* To restart replay, Prepare() first, then Replay() */
```

To manually replay the trace via `Replayer`:

```C++
/* Open db, create TraceReader and Replayer */

// Prepare (read trace header)
s = replayer->Prepare();

// Skip any unsupported traces
while (s.ok() || s.IsNotSupported()) {
  std::unique_ptr<TraceRecord> record;
  s = replayer->Next(&record);
  if (s.ok()) {
    // Execute the TraceRecord
    s = replayer->Execute(std::move(record), nullptr);
  }
}

// The current trace end is represented by Status::Incomplete()
assert(s.IsIncomplete());
```

Example of replaying traces and reporting execution results:
```C++
class SimpleResultHandler : public TraceRecordResult::Handler {
 public:
  SimpleResultHandler() {}
  ~SimpleResultHandler() override {}

  Status Handle(const StatusOnlyTraceExecutionResult& result) override {
    cnt_++;
    latency_ += result.GetLatency();
    std::cout << "Status: " << result.GetStatus().ToString() << std::endl;
  }

  Status Handle(const SingleValueTraceExecutionResult& result) override {
    cnt_++;
    latency_ += result.GetLatency();
    std::cout << "Status: " << result.GetStatus().ToString()
              << ", value: " << result.GetValue() << std::endl;
  }

  Status Handle(const MultiValuesTraceExecutionResult& result) override {
    cnt_++;
    latency_ += result.GetLatency();
    size_t size = result.GetMultiStatus().size();
    for (size_t i = 0; i < size; i++) {
      std::cout << i << ", status: " << result.GetMultiStatus()[i].ToString()
                << ", value: " << result.GetValues()[i] << std::endl;
    }
  }

  Status Handle(const IteratorTraceExecutionResult& result) override {
    cnt_++;
    latency_ += result.GetLatency();
    if (result.GetValid()) {
      std::cout << "Valid"
                << ", status: " << result.GetStatus().ToString()
                << ", key: " << result.GetKey().ToString()
                << ", value: " << result.GetValue().ToString() << std::endl;
    } else {
      std::cout << "Invalid"
                << ", status: " << result.GetStatus().ToString() << std::endl;
    }
  }

  uint64_t GetCount() const { return cnt_; }

  double GetAvgLatency() const { return 1.0 * latency_ / cnt_; }

  void Reset() {
    cnt_ = 0;
    latency_ = 0;
  }

 private:
  std::atomic<uint64_t> cnt_{0};
  std::atomic<uint64_t> latency_{0};
};

/* Open db, create TraceReader and Replayer */

// Prepare (read trace header)
s = replayer->Prepare();

SimpleResultHandler handler;

/* For auto-replay */
// Result callback for auto-replay
auto res_cb = [&handler](Status st, std::unique_ptr<TraceRecordResult>&& res) {
  // res == nullptr if !st.ok()
  if (res != nullptr) {
    res->Accept(&handler);
  }
};

s = replayer->Replay(ReplayOptions(4, 2.0), res_cb);

std::cout << "Total traces: " << handler.GetCount() << std::endl;
std::cout << "Average latency: " << handler.GetAvgLatency() << " us"
          << std::endl;

/* For manual-replay */
// handler.Reset();

s = replayer->Prepare();

// Skip any unsupported traces
while (s.ok() || s.IsNotSupported()) {
  std::unique_ptr<TraceRecord> record;
  s = replayer->Next(&record);
  if (s.ok()) {
    // Execute the TraceRecord
    std::unique_ptr<TraceRecordResult> res;
    s = replayer->Execute(std::move(record), &res);
    // res == nullptr if !s.ok()
    if (res != nullptr) {
      res->Accept(&handler);
    }
  }
}

std::cout << "Total traces: " << handler.GetCount() << std::endl;
std::cout << "Average latency: " << handler.GetAvgLatency() << " us"
          << std::endl;
```


To replay the trace via **db_bench**:


```sh
# You may also set --trace_replay_threads and --trace_replay_fast_forward
./db_bench --benchmarks=replay --trace_file=/tmp/trace_test_example --num_column_families=5
```





# Trace Analyzing, Visualizing, and Modeling

After the user finishes the tracing steps by using the trace_replay APIs, the user will get one binary trace file. In the trace file, Get, Seek, and SeekForPrev are tracked with separate trace record, while queries of Put, Merge, Delete, SingleDelete, and DeleteRange are packed into WriteBatches. One tool is needed to 1) interpret the trace into the human readable format for further analyzing, 2) provide rich and powerful in-memory processing options to analyze the trace and output the corresponding results, and 3) be easy to add new analyzing options and query types to the tool.


The RocksDB team developed the initial version of the tool: trace_analyzer. It provides the following analyzing options and output results.

Note that most of the generated analyzing results output files will be separated in different column families and different query types, which means, one query type in one column family will have its own output files. Usually, one specified output option will generate one output file.


## Analyze The Trace

The trace analyzer options

```
    -analyze_delete (Analyze the Delete query.) type: bool default: false
    -analyze_get (Analyze the Get query.) type: bool default: false
    -analyze_iterator ( Analyze the iterate query like Seek() and
      SeekForPrev().) type: bool default: false
    -analyze_merge (Analyze the Merge query.) type: bool default: false
    -analyze_multiget ( Analyze the MultiGet query. NOTE: for MultiGet, we
      analyze each KV-pair read in one MultiGet query. Therefore, the total
      queries and QPS are calculated based on the number of KV-pairs being
      accessed not the number of MultiGet.It can be improved in the future if
      needed) type: bool default: false
    -analyze_put (Analyze the Put query.) type: bool default: false
    -analyze_range_delete (Analyze the DeleteRange query.) type: bool
      default: false
    -analyze_single_delete (Analyze the SingleDelete query.) type: bool
      default: false
    -convert_to_human_readable_trace (Convert the binary trace file to a human
      readable txt file for further processing. This file will be extremely
      large (similar size as the original binary trace file). You can specify
      'no_key' to reduce the size, if key is not needed in the next step.
      File name: <prefix>_human_readable_trace.txt
      Format:[type_id cf_id value_size time_in_micorsec <key>].) type: bool
      default: false
    -key_space_dir (<the directory stores full key space files> 
      The key space files should be: <column family id>.txt) type: string
      default: ""
    -no_key ( Does not output the key to the result files to make smaller.)
      type: bool default: false
    -no_print (Do not print out any result) type: bool default: false
    -output_access_count_stats (Output the access count distribution statistics
      to file.
      File name: 
      <prefix>-<query_type>-<cf_id>-accessed_key_count_distribution.txt 
      Format:[access_count number_of_access_count]) type: bool default: false
    -output_dir (The directory to store the output files.) type: string
      default: ""
    -output_ignore_count (<threshold>, ignores the access count <= this value,
      it will shorter the output.) type: int32 default: 0
    -output_key_distribution (Print the key size distribution.) type: bool
      default: false
    -output_key_stats (Output the key access count statistics to file
      for accessed keys:
      file name: <prefix>-<query_type>-<cf_id>-accessed_key_stats.txt
      Format:[cf_id value_size access_keyid access_count]
      for the whole key space keys:
      File name: <prefix>-<query_type>-<cf_id>-whole_key_stats.txt
      Format:[whole_key_space_keyid access_count]) type: bool default: false
    -output_prefix (The prefix used for all the output files.) type: string
      default: "trace"
    -output_prefix_cut (The number of bytes as prefix to cut the keys.
      If it is enabled, it will generate the following:
      For accessed keys:
      File name: <prefix>-<query_type>-<cf_id>-accessed_key_prefix_cut.txt 
      Format:[acessed_keyid access_count_of_prefix number_of_keys_in_prefix
      average_key_access prefix_succ_ratio prefix]
      For whole key space keys:
      File name: <prefix>-<query_type>-<cf_id>-whole_key_prefix_cut.txt
      Format:[start_keyid_in_whole_keyspace prefix]
      if 'output_qps_stats' and 'top_k' are enabled, it will output:
      File name:
      <prefix>-<query_type>-<cf_id>-accessed_top_k_qps_prefix_cut.txt
      Format:[the_top_ith_qps_time QPS], [prefix qps_of_this_second].)
      type: int32 default: 0
    -output_qps_stats (Output the query per second(qps) statistics 
      For the overall qps, it will contain all qps of each query type. The time
      is started from the first trace record
      File name: <prefix>_qps_stats.txt
      Format: [qps_type_1 qps_type_2 ...... overall_qps]
      For each cf and query, it will have its own qps output.
      File name: <prefix>-<query_type>-<cf_id>_qps_stats.txt 
      Format:[query_count_in_this_second].) type: bool default: false
    -output_time_series (Output the access time in second of each key, such
      that we can have the time series data of the queries 
      File name: <prefix>-<query_type>-<cf_id>-time_series.txt
      Format:[type_id time_in_sec access_keyid].) type: bool default: false
    -output_value_distribution (Out put the value size distribution, only
      available for Put and Merge.
      File name:
      <prefix>-<query_type>-<cf_id>-accessed_value_size_distribution.txt
      Format:[Number_of_value_size_between x and x+value_interval is: <the
      count>]) type: bool default: false
    -print_correlation (intput format: [correlation pairs][.,.]
      Output the query correlations between the pairs of query types listed in
      the parameter, input should select the operations from:
      get, put, delete, single_delete, rangle_delete, merge. No space between
      the pairs separated by comma. Example: =[get,get]... It will print out
      the number of pairs of 'A after B' and the average time interval between
      the two query.) type: string default: ""
    -print_overall_stats ( Print the stats of the whole trace, like total
      requests, keys, and etc.) type: bool default: true
    -print_top_k_access (<top K of the variables to be printed> Print the top k
      accessed keys, top k accessed prefix and etc.) type: int32 default: 1
    -sample_ratio (If the trace size is extremely huge or user want to sample
      the trace when analyzing, sample ratio can be set (0, 1.0]) type: double
      default: 1
    -trace_path (The trace file path.) type: string default: ""
    -try_process_corrupted_trace (In default, trace_analyzer will exit if the
      trace file is corrupted due to the unexpected tracing cases. If this
      option is enabled, trace_analyzer will stop reading the trace file, and
      start analyzing the read-in data.) type: bool default: false
    -value_interval (To output the value distribution, we need to set the value
      intervals and make the statistic of the value size distribution in
      different intervals. The default is 8.) type: int32 default: 8
```

**One Example**

```sh
./trace_analyzer \
  -analyze_get \
  -output_access_count_stats \
  -output_dir=/data/trace/result \
  -output_key_stats \
  -output_qps_stats \
  -convert_to_human_readable_trace \
  -output_value_distribution \
  -output_key_distribution \
  -print_overall_stats \
  -print_top_k_access=3 \
  -output_prefix=test \
  -trace_path=/data/trace/trace
```

**Another Example**
```
./trace_analyzer -analyze_get -analyze_put -analyze_merge -analyze_delete \
-analyze_single_delete -analyze_iterator -analyze_multiget \
-output_access_count_stats -output_dir=./result -output_key_stats -output_qps_stats \
-output_value_distribution -output_key_distribution -output_time_series -print_overall_stats \
-print_top_k_access=3 -value_interval=1 -output_prefix=trace_test -trace_path=./trace_example
```


**Query Type Options**

User can specify which type queries that should be analyzed and use “-analyze_<type>”.

**Output Human Readable Traces**

The original binary trace stores the encoded data structures and content, to interpret the trace, the tool should use the RocksDB library. Thus, to simplify the further analyzing of the trace, user can specify


```
-convert_to_human_readable_trace
```


The original trace will be converted to a txt file, the content is “[type_id cf_id value_size time_in_micorsec <key>]”. If the key is not needed, user can specify “-no_key” to reduce the file size. This option is independent to all other option, once it is specified, the converted trace will be generated. If the original key is included, the txt file size might be similar or even larger than the original trace file size.

**Input and Output Options**

To analyze a trace file, user need to indicate the path to the trace file by


```
-trace_path=<path to the trace>
```


To store the output files,  user can specify a directory (make sure the directory exist before running the analyzer) to store these files


```
-output_dir=<the path to the output directory>
```


If user wants to analyze the accessed keys together with the existing keyspace. User needs to specify a directory that stores the keyspace files. The file should be in the name of “<column family id>.txt” and each line is one key. Usually, user can use the “./ldb scan” of the LDB tool to dump out all the existing keys. To specify the directory


```
-key_space_dir=<the path to the key space directory>
```


To collect the output files more easily, user can specify the “prefix” for all the output files


```
-output_prefix=<the prefix, like "trace1">
```


If user does not want to print out the general statistics to the screen, user can specify 


```
-no_print
```



**The Analyzing Options**

Currently, the trace_analyzer tool provides several different analyzing options to characterize the workload. Some of the results are directly printed out (options with prefix “-print”) others will output to the files (options with prefix “-output”). User can specify the combination of the options to analyze the trace. Note that, some of the analyzing options are very memory intensive (e.g., -output_time_series, -print_correlation, and -key_space_dir). If the memory is not enough, try to run them in different time.

The general information of the workloads like the total number of keys, the number of analyzed queries of each column family, the key and value size statistics (average and medium), the number of keys being accessed are printed to screen when this option is specified


```
-print_overall_stats
```



To get the total access count of each key and the size of the value, user can specify 


```
-output_key_stats
```


It will output the access count of each key to a file and the keys are sorted in lexicographical order. Each key will be assigned with an integer as the ID for further processing

In some workloads, the composition of the keys has some common part. For example, in MyRocks, the first X bytes of the key is the table index_num. We can use the first X bytes to cut the keys into different prefix range. By specifying the number of bytes to cut the key space, the trace_analyzer will generate a file. One record in the file represents a cut of prefix, the corresponding KeyID, and the prefix content are stored. There will be two separate files if the -key_space_dir is specified. One file is for the accessed keys, the other one is for the whole key space. Usually, the prefix cut file is used together with the accessed_key_stats.txt and whole_key_stats.txt respectively. 


```
-output_prefix_cut=<number of bytes as prefix>
```



If user wants to visualize the accesses of the keys in the tracing timeline, user can specify:


```
-output_time_series
```


Each access to one key will be stored as one record in the time series file. 


If the user is interested to know about the detailed QPS changing during the tracing time, user can specify:


```
-output_qps_stats
```


For each query type of one column family, one file with the query number per second will be generated. Also, one file with the QPS of each query type on all column families as well as the overall QPS are output to a separate file. The average QPS and peak QPS will be printed out.

Sometimes, user might be interested to know about the TOP statistics. user can specify


```
-print_top_k_access
```


The top K accessed keys, the access number will be printed. Also, if the prefix_cut option is specified, the top K accessed prefix with their total access count are printed. At the same time, the top K prefix with the highest average access is printed. 

If the user is interested to know about the value size distribution (only applicable for Put and Merge ) user can specify


```
-output_value_distribution
-value_interval
```


Since the value size varies a lot, User might just want to know how many values are in each value size range. User can specify the value_interval=x to generate the number of values between [0,x), [x,2x)......

The key size distribution is output to the file if user specify


```
-output_key_distribution
```



## Visualize the workload

After processing the trace with trace_analyzer, user will get a couple of output files. Some of the files can be used to visualize the workload such as the heatmap (the hotness or coldness of keys), time series graph (the overview of the key access in timeline), the QPS of the analyzed queries. 

Here, we use the open source plot tool GNUPLOT as an example to generate the graphs. More details about the GNUPLOT can be found here (http://gnuplot.info/). User can directly write the GNUPLOT command to draw the graph, or to make it simple, user can use the following shell script to generate the GNUPLOT source file (before using the script, make sure the file name and some of the content are replaced with the effective ones).

To draw the heat map of accessed keys (**note that, the following scripts need to be customized based on the file name, the arguments of your workloads, and the plotting style you want. They are just examples**)


```bash
#!/bin/bash

# The query type
ops="iterator"

# The column family ID
cf="9"

# The column family name if known, if not, replace it with some prefix
cf_name="rev:cf-assoc-deleter-id1-type"

form="accessed"

# The column number that will be plotted
use="4"

# The higher bound of Y-axis
y=2

# The higher bound of X-axis
x=29233
echo "set output '${cf_name}-${ops}-${form}-key_heatmap.png'" > plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set term png size 2000,500" >>plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set title 'CF: ${cf_name} ${form} Key Space Heat Map'">>plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set xlabel 'Key Sequence'">>plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set ylabel 'Key access count'">>plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set yrange [0:$y]">>plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set xrange [0:$x]">>plot-${cf_name}-${ops}-${form}-heatmap.gp

# If the preifx cut is avialable, it will draw the prefix cut
while read f1 f2
do
    echo "set arrow from $f1,0 to $f1,$y nohead lc rgb 'red'" >> plot-${cf_name}-${ops}-${form}-heatmap.gp
done < "trace.1532381594728669-${ops}-${cf}-${form}_key_prefix_cut.txt"
echo "plot 'trace.1532381594728669-${ops}-${cf}-${form}_key_stats.txt' using ${use} notitle w dots lt 2" >>plot-${cf_name}-${ops}-${form}-heatmap.gp
gnuplot plot-${cf_name}-${ops}-${form}-heatmap.gp
```

Or you can directly us GNUPLOT script to plot out the key access distribution. Here is one example (plot_get_heat_map.gp):

```
set term png size 2000,600 enhanced font 'Times New Roman, 40'
set output '24-0-get-accessed-key_heatmap.png'
set title 'Default' offset 0,-1
set ylabel 'Key access count'
set yrange [0:400]
set xrange [0:60000000]
plot 'trace.x-get-0-accessed_key_stats.txt' using 4 notitle w dots lt 2
```

To draw the time series map of accessed keys
```bash
#!/bin/bash

# The query type
ops="iterator"

# The higher bound of X-axis
x=29233

# The column family ID
cf="8"

# The column family name if known, if not, replace it with some prefix
cf_name="rev:cf-assoc-deleter-id1-type"

# The type of the output file
form="time_series"

# The column number that will be plotted
use="3:2"

# The total time of the tracing duration, in seconds
y=88000
echo "set output '${cf_name}-${ops}-${form}-key_heatmap.png'" > plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set term png size 3000,3000" >>plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set title 'CF: ${cf_name} time series'">>plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set xlabel 'Key Sequence'">>plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set ylabel 'Key access count'">>plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set yrange [0:$y]">>plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set xrange [0:$x]">>plot-${cf_name}-${ops}-${form}-heatmap.gp

# If the preifx cut is avialable, it will draw the prefix cut
while read f1 f2
do
    echo "set arrow from $f1,0 to $f1,$y nohead lc rgb 'red'" >> plot-${cf_name}-${ops}-${form}-heatmap.gp
done < "trace.1532381594728669-${ops}-${cf}-accessed_key_prefix_cut.txt"
echo "plot 'trace.1532381594728669-${ops}-${cf}-${form}.txt' using ${use} notitle w dots lt 2" >>plot-${cf_name}-${ops}-${form}-heatmap.gp
gnuplot plot-${cf_name}-${ops}-${form}-heatmap.gp
```

Or you can use the GNUPLOT script as follows (plot_time_series.gp):
```
set output 'new-trace_1-get-0-time_series.png'
set term png size 1000,800 enhanced font 'Times New Roman, 32'
set xlabel 'Key Sequence'
set ylabel 'Time(second)' offset 1,0,0
set yrange [0:89000]
set xrange [0:500000]
set format x '%.0tx10^%T'
set xtics 100000
set mxtics
plot 'trace_1-get-0-time_series.txt' using 3:2 notitle w dots lt 2
```

To plot out the QPS


```bash
#!/bin/bash

# The query type
ops="iterator"

# The higher bound of the QPS
y=5

# The column family ID
cf="9"

# The column family name if known, if not, replace it with some prefix
cf_name="rev:cf-assoc-deleter-id1-type"

# The type of the output file
form="qps_stats"

# The column number that will be plotted
use="1"

# The total time of the tracing duration, in seconds
x=88000
echo "set output '${cf_name}-${ops}-${form}-IO_per_second.png'" > plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set term png size 2000,1200" >>plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set title 'CF: ${cf_name} QPS Over Time'">>plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set xlabel 'Time in second'">>plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set ylabel 'QPS'">>plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set yrange [0:$y]">>plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "set xrange [0:$x]">>plot-${cf_name}-${ops}-${form}-heatmap.gp
echo "plot 'trace.1532381594728669-${ops}-${cf}-${form}.txt' using ${use} notitle with linespoints" >>plot-${cf_name}-${ops}-${form}-heatmap.gp
gnuplot plot-${cf_name}-${ops}-${form}-heatmap.gp
```

Or you can use the following GNUPLOT script (plot_qps_all.gp):
First, you can use the following python script to preprocess the Get, Put, Merge QPS raw file. Here is one example to preprocess the raw file:
```
import numpy as np
s1 = "trace_2-qps_stats.txt"
s2 = "./result_sample/"
s3 = s2+s1
l1 = []
l2 = []
l3 = []
l4 = []
l5 = []
l6 = []
f = open(s3,'r')
start=1539923212
sum = 0
result = list()
for line in open(s3):
    line = f.readline()
    r1,r2,r3,r4,r5,r6,r7,r8,r9 = [str(i) for i in line.split()]
    l1.append(int(r1)*30)
    l2.append(int(r2)*30)
    l3.append(int(r3)*30)
    l4.append(int(r4)*30)
    l5.append((int(r7)+int(r8))*30)
    l6.append(int(r9)*30)
f.close()
s4 = s2+"new-"+s1
fw = open(s4,'w')
for i in range(0,len(l1)):
    fw.write("%d %d %d %d %d %d %d\n"%(start, l1[i],l2[i],l3[i],l4[i],l5[i],l6[i]))
    start = start+1
print len(l1)
```
Then, you can use gnuplot to plot out the results:
```
set term png size 2000,1000 enhanced font 'Times New Roman, 40'
set output 'all_qps_100.png'
set ylabel 'Queries/sec'
set yrange [0:100]
plot 'new-trace_2-get-0-qps_stats.txt' title 'Get' with lines lw 3 lt 6,\
     'new-trace_2-put-0-qps_stats.txt' title 'Put' with lines lw 3 lt -1,\
     'new-trace_2-merge-0-qps_stats.txt' title 'Merge' with lines linestyle 3 lw 3 lt 5
```

Here is the GNUPLOT script for key and value size distribution plot (plot_get_key_size_distribution.gp):
First, you can use the following python script to pre-process the raw file:
```
import numpy as np
s1 = "trace_2-get-0-accessed_key_size_distribution.txt"
s2 = "./result_sample/"
s3 = s2+s1
l1 = []
l2 = []
f = open(s3,'r')
sum = 0
result = list()
for line in open(s3):
    line = f.readline()
    r1,r2= [str(i) for i in line.split()]
    l1.append(int(r1))
    l2.append(int(r2))
    sum = sum + int(r2)
f.close()
s4 = s2+"new-"+s1
fw = open(s4,'w')
cur = 0.0
count = 0
for i in range(0,200):
    if count < len(l1) and l1[count] == i:
        cur = float(l2[count]) / sum
        count = count+1
        fw.write("%d %.12f\n"%(i,cur))
    else:
        fw.write("%d %.12f\n"%(i,0))
print sum
```

Then, you can plot them out

```
set term png size 1000,600 enhanced font 'Times New Roman, 36'
set output 'key-size-distribution.png'
set xlabel 'Key size (bytes)'
set yrange [0:1.01]
set xrange [0:180]
set key right bottom
plot 'new-trace_2-get-0-accessed_key_size_distribution.txt' using 1:2 title 'Get' with linespoints lw 5 lt 6 pointtype 2 pointsize 3,\
 'new-trace_2-put-0-accessed_key_size_distribution.txt' using 1:2 title 'Put' with linespoints lw 5 lt -1 pointtype 7 pointsize 3,\
'new-trace_2-merge-0-accessed_key_size_distribution.txt' using 1:2 title 'Merge' with linespoints lw 5 lt 1 pointtype 1 pointsize 3
```


## Model the Workloads

We can use different tools, script, and models to fit the workload statistics. Typically, user can use the distributions of key access count and key-range access count to fit in the model. Also, QPS can be modeled. Here, we use Matlab as an example to fit the key access count, key-range access count, and QPS. Note that, before we model the key-range hotness, we need to decide how to cut the whole key-space into key-ranges. Our suggestion is that the key-range size can be similar to the number of KV-pairs in a SST file.

User can try different distributions to fit the model. In this example, we use two-term exponential model to fit the access distribution, and two-term sin() to fit the QPS

To fit the key access statistics to the model and get the statistics, user can run the following script:


```
% This script is used to fit the key access count distribution
% to the two-term exponential distirbution and get the parameters

% The input file with surfix: accessed_key_stats.txt
fileID = fopen('trace.1531329742187378-get-4-accessed_key_stats.txt');
txt = textscan(fileID,'%f %f %f %f');
fclose(fileID);

% Get the number of keys that has access count x
t2=sort(txt{4},'descend');

% The number of access count that is used to fit the data
% The value depends on the accuracy demond of your model fitting
% and the value of count should be always not greater than
% the size of t2
count=30000;

% Generate the access count x
x=1:1:count;
x=x';

% Adjust the matrix and uniformed
y=t2(1:count);
y=y/(sum(y));
figure;

% fitting the data to the exp2 model
f=fit(x,y,'exp2')

%plot out the original data and fitted line to compare
plot(f,x,y);
```


To fit the key access count distribution to the model, user can run the following script:


```
% This script is used to fit the key access count distribution
% to the two-term exponential distirbution and get the parameters

% The input file with surfix: key_count_distribution.txt
fileID = fopen('trace-get-9-accessed_key_count_distribution.txt');
input = textscan(fileID,'%s %f %s %f');
fclose(fileID);

% Get the number of keys that has access count x
t2=sort(input{4},'descend');

% The number of access count that is used to fit the data
% The value depends on the accuracy demond of your model fitting
% and the value of count should be always not greater than
% the size of t2
count=100;

% Generate the access count x
x=1:1:count;
x=x';
y=t2(1:count);

% Adjust the matrix and uniformed
y=y/(sum(y));
y=y(1:count);
x=x(1:count);

figure;
% fitting the data to the exp2 model
f=fit(x,y,'exp2')

%plot out the original data and fitted line to compare
plot(f,x,y);
```



To fit the key-range average access count to the model, user can run the following script:


```
% This script is used to fit the prefix average access count distribution
% to the two-term exponential distirbution and get the parameters

% The input file with surfix: accessed_key_prefix_cut.txt
fileID = fopen('trace-get-4-accessed_key_prefix_cut.txt');
txt = textscan(fileID,'%f %f %f %f %s');
fclose(fileID);

% The per key access (average) of each prefix, sorted
t2=sort(txt{4},'descend');

% The number of access count that is used to fit the data
% The value depends on the accuracy demond of your model fitting
% and the value of count should be always not greater than
% the size of t2
count=1000;

% Generate the access count x
x=1:1:count;
x=x';

% Adjust the matrix and uniformed
y=t2(0:count);
y=y/(sum(y));
x=x(1:count);

% fitting the data to the exp2 model
figure;
f=fit(x,y,'exp2')

%plot out the original data and fitted line to compare
plot(f,x,y);
```



To fit the QPS to the model, user can run the following script:


```
% This script is used to fit the qps of the one query in one of the column
% family to the sin'x' model. 'x' can be 1 to 10. With the higher value
% of the 'x', you can get more accurate fitting of the qps. However,
% the model will be more complex and some times will be overfitted.
% The suggestion is to use sin1 or sin2

% The input file shoud with surfix: qps_stats.txt
fileID = fopen('trace-get-4-io_stats.txt');
txt = textscan(fileID,'%f');
fclose(fileID);
t1=txt{1};

% The input is the queries per second. If you directly use the qps
% you may got a high value of noise. Here, 'n' is the number of qps
% that you want to combined to one average value, such that you can
% reduce it to queries per n*seconds.
n=10;
s1 = size(t1, 1);
M  = s1 - mod(s1, n);
t2  = reshape(t1(1:M), n, []);
y = transpose(sum(t2, 1) / n);

% Up to this point, you need to move the data down to the x-axis,
% the offset is the ave. So the model will be 
% s(x) =  a1*sin(b1*x+c1) + a2*sin(b2*x+c2) + ave
ave = mean(y);
y=y-ave;

% Adjust the matrix
count = size(y,1);
x=1:1:count;
x=x';

% Fit the model to 'sin2' in this example and draw the point and
% fitted line to compare
figure;
s = fit(x,y,'sin2')
plot(s,x,y);
```


Users can use the model for further analyzing or use it to generate the synthetic workload.

# Synthetic Workload Generation based on Models

In the previous section, users can use the fitting functions of Matlab to fit the traced workload to different models, such that we can use a set of parameters and functions to profile the workload. We focus on the 4 variables to profile the workload: 1) value size; 2) KV-pair access hotness; 3) QPS; 4) Iterator scan length. According to our current research, the value size and Iterator scan length follows the Generalized Pareto Distribution. The probability density function is: f(x) = (1/sigma) * (1+k * (x-theta)\sigma)^(-1-1/k). The KV-pair access follows power-law, in which about 80% of the KV-pair has an access count less than 4. We sort the keys based on the access count in descending order and fit them to the models. The two-term power model fit the KV-pair access distribution best. The probability density function is: f(x) = ax^b+c. The Sine function fits the QPS best. F(x) = A * sin(B * x + C) + D.

Here is one example of the parameters we get from the workload collected in Facebook social graph:

1) Value Size: sigma = 226.409, k = 0.923$, theta = 0
2) KV-pair Access: a = 0.001636, b = -0.7094 , and c = 3.217*10^-9
3) QPS: $A = 147.9, B = 8.3*10^-5, C = -1.734, D = 1064.2
4) Iterator scan length: sigma = 1.747, k = 0.0819, theta = 0

We developed a benchmark called “mixgraph” in db_bench, which can use the four set of parameters to generate the synthetic workload. The workload is statistically similar to the original one. Note that, only the workload that can be fit to the models used for the four variables can be used in the mixgraph. For example, if the value size follows the power distribution instead of Generalized Pareto Distribution, we cannot use the mixgraph to generate the workloads.

More details about the workloads, benchmarks, and models, please refer to the published paper at FAST2020 ([here](https://www.usenix.org/system/files/fast20-cao_zhichao.pdf))

To enable the “mixgraph” benchmark, user needs to specify:

```sh
./db_bench —benchmarks="mixgraph"
```


To set the parameters of the value size distribution (Generalized Pareto Distribution only), user needs to specify:

```
-value_k=<> -value_sigma=<> -value_theta=<>
```


To set the parameters of the KV-pair access distribution (power distribution only and C==0), user needs to specify:

```
-key_dist_a=<> -key_dist_b=<>
```

To set the parameters of key-range distributions which follows the two-term exponential distribution (f(x)=a*exp(b*x)+c*exp(d*x)), user needs to specify both the key-range hotness distribution parameters and the number of key-ranges (keyrange_num):

```
-keyrange_dist_a=<> -keyrange_dist_b=<> -keyrange_dist_c=<> -keyrange_dist_d=<> -keyrange_num=<>
```


To set the parameters of the QPS (Sine),  user needs to specify:

```
-sine_a=<> -sine_b=<> -sine_c=<> -sine_d=<> -sine_mix_rate_interval_milliseconds=<>
```

The mix rate is used to set the time interval that how lone we should correct the rate according to the distribution, the smaller it is, the better it will fit.

To set the parameters of the iterator scan length distribution (Generalized Pareto Distribution only), user needs to specify:

```
-iter_k=<> -iter_sigma=<> -iter_theta=<>
```


User need to specify the query ratio between the Get, Put, and Seek. Such that we can generate the mixed workload that can be similar to the social graph workload (so called mix graph). Make sure that the sum of the ratio is 1.

```
-mix_get_ratio=<> -mix_put_ratio=<> -mix_seek_ratio=<>
```


Finally, user need to specify how many queries they want to execute:

```
-reads=<>
```

and what's the total KV-pairs are in the current testing DB

```
-num=<>
```

The num together with the aforementioned distributions decided the queries.

Here is one example that can be directly used to generate the workload which simulate the queries of ZippyDB described in the published paper [link](https://www.usenix.org/system/files/fast20-cao_zhichao.pdf). The workloads has 0.42 billion queries and 50 million KV-pairs in total. We use 30 key-ranges. Note that, if user runs the benchmark following the 24 hours Sine period, it will take about 22-24 hours. In order to speedup the benchmarking, user can increase the sine_d to a larger value such as 45000 to increase the workload intensiveness and also reduce the sine_b accordingly.

```sh
./db_bench \
  --benchmarks="mixgraph" \
  -use_direct_io_for_flush_and_compaction=true \
  -use_direct_reads=true \
  -cache_size=268435456 \
  -keyrange_dist_a=14.18 \
  -keyrange_dist_b=-2.917 \
  -keyrange_dist_c=0.0164 \
  -keyrange_dist_d=-0.08082 \
  -keyrange_num=30 \
  -value_k=0.2615 \
  -value_sigma=25.45 \
  -iter_k=2.517 \
  -iter_sigma=14.236 \
  -mix_get_ratio=0.85 \
  -mix_put_ratio=0.14 \
  -mix_seek_ratio=0.01 \
  -sine_mix_rate_interval_milliseconds=5000 \
  -sine_a=1000 \
  -sine_b=0.000073 \
  -sine_d=4500 \
  --perf_level=2 \
  -reads=420000000 \
  -num=50000000 \
  -key_size=48
```







