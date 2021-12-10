
import argparse
import os
import random
import sys

# TODO
#   option for insert only
#   figure out reduction pct for overwrite
#   overwrite with skew, first N levels?
#   overwrite vs insert (yes/no max size, some/no loss on merge, yes/no major compaction, yes/no space-amp-goal)
#   read hotness options other than uniform for stcs
#   
#   stopping condition for prefix: size ratio or benefit?
#   does major compaction respect max_merge_width ?

def parse_options(argv):
  parser = argparse.ArgumentParser()
  parser.add_argument('--seed', type=int, default=1, help='Seed for RNG')
  parser.add_argument('--algorithm', choices=['stcs', 'prefix', 'tower'],
                      required=True, help='The algorithm to simulate')
  parser.add_argument('--workload', choices=['insert', 'overwrite'], default='overwrite', 
                      help='Workload is insert-only or overwrite-only')
  parser.add_argument('--init_size_gb', type=int, default=128,
                      help='Max size of database in GB')
  parser.add_argument('--memtable_mb', type=int, default=64,
                      help='Size of memtable in MB')
  parser.add_argument('--run_time', type=int, default=5000,
                      help='Run for this many memtable flushes')
  parser.add_argument('--debug', default=False, action='store_true')
  parser.add_argument('--keep_pct', type=int, default=90, choices=range(1,100),
                      help='Percentage of the compaction input that is kept because it is redundant. '\
                           'Must be 0 for workload=insert and in [1,100] for workload=overwrite.')
  parser.add_argument('--space_amp_goal', type=int, default=50,
                      help='Triggers compaction when sizeof(non-max runs) is >= (space_amp_goal/100 * sizeof(max run)). '\
                           'Ignored when 0, must be >= 0.')

  # Tuning options common to all algorithms
  parser.add_argument('--min_merge_width', type=int, default=4,
                      help='See min_threshold for STCS in ScyllaDB, min_merge_width for RocksDB')
  parser.add_argument('--max_merge_width', type=int, default=32,
                      help='See max_threshold for STCS in ScyllaDB, max_merge_width for RocksDB')

  # Options for STCS (ScyllaDB & Cassandra)
  parser.add_argument('--stcs_bucket_high', type=float, default=1.5,
                      help='See bucket_high for STCS in ScyllaDB')
  parser.add_argument('--stcs_bucket_low', type=float, default=0.5,
                      help='See bucket_low for STCS in ScyllaDB')
  parser.add_argument('--stcs_min_sstable_size_mb', type=int, default=80,
                      help='See min_sstable_size for STCS in ScyllaDB')

  # Options for prefix (HBase and RocksDB)
  parser.add_argument('--prefix_read_amp_trigger', type=int, default=9,
                      help='Trigger minor compaction when there are this many sorted runs')
  parser.add_argument('--prefix_size_ratio', type=float, default=1.5,
                      help='If max_merge_width has not been reached, stop adding sorted runs '\
                           'to the next minor compaction when the next run is more than this '\
                           'much larger than the sum of the sizes of the sorted runs already selected.')
  # TODO: support this
  parser.add_argument('--prefix_benefit', default=False, action='store_true',
                      help='When true ignore --prefix_size_ratio and use cost/benefit analysis to determine number of runs to merge')

  # Options for tower
  # TODO ???
  parser.add_argument('--tower_l0_trigger', type=int, default=8, 
                      help='Trigger minor compaction when the L0 has this many runs')
  parser.add_argument('--tower_size_ratio', type=float, default=1.5,
                      help='Similar to --prefix_size_ratio')

  args = parser.parse_args()
  return args

next_id = 0
def get_id():
  global next_id
  n = next_id
  next_id += 1
  return n

def make_run(size, min_ts, max_ts, avg_wr):
  r = { 'size':size, 'min_ts':min_ts, 'max_ts':max_ts, 'avg_wr':avg_wr, 'id':get_id() } 
  return r

def runs_to_str(runs, short=False, full=False):
  s = "[ "
  for r in runs:
    if short:
      s += " %.0f" % r['size']
    elif not full:
      s += " %.0f:%d" % (r['size'], r['id'])
    else:
      s += " %.1f:%d:%d:%d:%.1f" % (r['size'], r['id'], r['min_ts'], r['max_ts'], r['avg_wr'])
  s += " ]"
  return s

def sum_run_sizes(runs):
  sz = 0
  for r in runs:
    sz += r['size']
  return sz  

def update_stats(now, runs, interval_stats, global_stats):
  ingest = runs[0]['size']
  s = { 'runs':len(runs), 'ingest':ingest, 'size':sum_run_sizes(runs) }
  interval_stats.append(s)

def final_stats(runs, interval_stats, global_stats):
  ingest = global_stats['ingest']
  write_amp = (global_stats['compact_wr'] + ingest) / ingest

  sum_size = 0
  max_size = 0
  sum_runs = 0
  max_runs = 0

  for s in interval_stats:
    sum_size += s['size']
    if s['size'] > max_size: max_size = s['size']
    sum_runs += s['runs']
    if s['runs'] > max_runs: max_runs = s['runs']

  print('final: %s %.1f runs(max,avg), %.0f %.1f size(max,avg), %s ingest, %.1f write-amp, %d major, %d minor' % (
      max_runs, sum_runs/len(interval_stats), 
      max_size, sum_size/len(interval_stats), 
      ingest, write_amp, global_stats['major'], global_stats['minor']))

def  merge_runs(runs_to_merge, args, now, all_runs, global_stats, append, sort_by_size):
  min_ts = next_id
  max_ts = -1
  size = 0
  avg_wr = 0
  for r in runs_to_merge:
    if r['min_ts'] < min_ts: min_ts = r['min_ts']
    if r['max_ts'] > max_ts: max_ts = r['max_ts']
    if args.keep_pct != 100:
      size = size + (r['size'] * (args.keep_pct/100.0))
    else:
      size = size + r['size']
    # Compute weighted average of avg_wr from input runs
    avg_wr = avg_wr + r['size'] * r['avg_wr']

  if args.workload == 'overwrite' and size > args.max_size_gb * 1024:
    size = args.max_size_gb * 1024

  # avg_wr is a weighted average of the number of times bytes in this run have been written
  avg_wr = avg_wr / size
  # increment because compaction just copied these runs to a new run
  avg_wr += 1
  global_stats['compact_wr'] += size
  if args.debug: print('merge_runs: %s input, %s size' % (runs_to_str(runs_to_merge, short=True), size))

  new_run = make_run(size, min_ts, max_ts, avg_wr)

  to_remove = [r['id'] for r in runs_to_merge]

  for id in to_remove:
    found = False
    for x, r2 in enumerate(all_runs):
      if id == r2['id']:
        del all_runs[x]
        found = True
        break

    if not found: print('merge_runs: not found %d' % id)
    assert found

  # Insert new_run into runs and then order based on size
  if append:
    all_runs.append(new_run)
  else:
    all_runs.insert(0, new_run)

  if sort_by_size:
    all_runs.sort(key=lambda r: r['size'])

def stcs_make_buckets(args, now, runs, global_stats):
  sorted_runs = sorted(runs, key=lambda r: r['size'])

  # Put the sorted runs into buckets by size

  buckets = []
  buckets.append({ 'avg_sz':sorted_runs[0]['size'], 'runs':[ sorted_runs[0] ] })
  cur_idx = 0

  for next_r in sorted_runs[1:]:
    bucket_avg_sz = buckets[cur_idx]['avg_sz']
    run_sz = next_r['size']
    smallest_in_bucket = buckets[cur_idx]['runs'][0]['size']
    if args.debug:
      print('next run: %.0f:%d with sz(%.1f) and idx(%d) :: %s' % (next_r['size'], next_r['id'], run_sz, cur_idx, next_r))

    added = False
    if ((run_sz > (bucket_avg_sz * args.stcs_bucket_low) and run_sz < (bucket_avg_sz * args.stcs_bucket_high))
        or
        (run_sz < args.stcs_min_sstable_size_mb and bucket_avg_sz < args.stcs_min_sstable_size_mb)):

      bucket_sz = run_sz
      bucket_sz += sum_run_sizes(buckets[cur_idx]['runs'])
      new_avg_size = bucket_sz / (1 + len(buckets[cur_idx]['runs']))

      # This check is in ScyllaDB but not in Cassandra
      if (run_sz < args.stcs_min_sstable_size_mb or smallest_in_bucket > new_avg_size * args.stcs_bucket_low):
        buckets[cur_idx]['avg_sz'] = new_avg_size
        buckets[cur_idx]['runs'].append(next_r)
        if args.debug: print('... add to current: %s' % next_r)
        added = True

    if not added:
      cur_idx = cur_idx + 1
      buckets.append({ 'avg_sz':next_r['size'], 'runs':[ next_r ] })
      if args.debug: print('... add to new: %s' % next_r)

  if args.debug:
    for b in buckets:
      print('bucket: %.1f avg_size, %d runs %s' % (b['avg_sz'], len(b['runs']), runs_to_str(b['runs'])))

  return sorted_runs, buckets

def stcs_minor_compact(args, now, runs, global_stats):
  # Simulated getBuckets from Cassandra and ScyllaDB. But it is better to focus on the ScyllaDB code
  # as the Cassandra code is more complicated than it needs to be and has non-determinism from iterating
  # on the hash table.
  # https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/compaction/SizeTieredCompactionStrategy.java#L251
  # https://github.com/scylladb/scylla/blob/master/compaction/size_tiered_compaction_strategy.cc

  if args.debug: print('stcs_minor_compact')

  sorted_runs, buckets = stcs_make_buckets(args, now, runs, global_stats)

  # Then choose a bucket to compact

  possible_buckets = [b for b in buckets if len(b['runs']) >= args.min_merge_width]

  if not possible_buckets:
    if args.debug: print('stcs_compact: no candidate buckets')
    return False

  # TODO: options other than random selection of possible_buckets, see read hotness
  compact_bucket = random.choice(possible_buckets)
  if args.debug:
    print('Compact bucket: %.1f avg_size, %d runs %s' % (
        compact_bucket['avg_sz'], len(compact_bucket['runs']), runs_to_str(compact_bucket['runs'])))

  # Then trim the bucket if it has too many runs, ignore the coldest sorted runs from it
  # TODO: for now trim is random
  if len(compact_bucket['runs']) > args.max_merge_width:
    random.shuffle(compact_bucket['runs'])
    compact_bucket['runs'] = compact_bucket['runs'][0:args.max_merge_width]
    if args.debug:
      print('Compact bucket after max trim: %.1f avg_size, %d runs %s' % (
          compact_bucket['avg_sz'], len(compact_bucket['runs']), runs_to_str(compact_bucket['runs'])))

  merge_runs(compact_bucket['runs'], args, now, runs, global_stats, True, True)
  global_stats['minor'] += 1

  return True

def stcs_major_compact(args, now, runs, global_stats):
  if args.space_amp_goal == 0 or len(runs) < 2:
    return False

  sorted_runs, buckets = stcs_make_buckets(args, now, runs, global_stats)

  # if the largest bucket has > 1 run then merge them
  # else if sizeof(next-to-max bucket) is too large, merge it with largest sorted run
  # else do nothing

  if len(buckets[-1]['runs']) > 1:
    merge_runs(buckets[-1]['runs'], args, now, runs, global_stats, True, True)
    global_stats['major'] += 1
    if args.debug: print('major: 2+ runs in max bucket')
    return True
  elif len(buckets) >=2 and (
      sum_run_sizes(buckets[-2]['runs']) >=
      ((args.space_amp_goal / 100.0) * sum_run_sizes(buckets[-1]['runs']))):
    merge_runs(buckets[-1]['runs'] + buckets[-2]['runs'], args, now, runs, global_stats, True, True)
    global_stats['major'] += 1
    if args.debug: print('major: next-to-max bucket too large')
    return True
  else:
    return False

def stcs_compact(args, now, runs, global_stats):
  stcs_major_compact(args, now, runs, global_stats)
  stcs_minor_compact(args, now, runs, global_stats)

def prefix_minor_compact(args, now, runs, global_stats):
  if len(runs) < args.prefix_read_amp_trigger: return False
  assert len(runs) >= 2

  runs_to_merge = []
  size_sum = 0

  for x, r in enumerate(runs[:]):
    runs_to_merge = runs[:x]

    if x >= args.max_merge_width:
      # Stop when max_merge_width runs were found
      break;

    if x >= args.min_merge_width and (size_sum * args.prefix_size_ratio) < r['size']:
      # Stop adding runs min_merge_width were found and the current run is too big
      # relative to the sum of sizes of the previous runs.
      break;

    size_sum += r['size']

  assert runs_to_merge
  if args.debug: print('prefix_minor')
  merge_runs(runs_to_merge, args, now, runs, global_stats, False, False)
  global_stats['minor'] += 1
  return True

def prefix_major_compact(args, now, runs, global_stats):
  if args.space_amp_goal == 0 or len(runs) < 2:
    return False

  size_without_max = sum_run_sizes(runs[:-1])
  if size_without_max < ((args.space_amp_goal/100.0) * runs[-1]['size']):
    return False

  # Passing previous check means there might be too much space-amp, do a major compaction
  runs_to_merge = runs
  if len(runs_to_merge) > args.max_merge_width:
    runs_to_merge = runs[(len(runs) - args.max_merge_width):]
    assert len(runs_to_merge) == args.max_merge_width
 
  new_len = len(runs) - len(runs_to_merge) + 1 
  if args.debug: print('prefix_major')
  merge_runs(runs_to_merge, args, now, runs, global_stats, True, False)
  global_stats['major'] += 1
  assert new_len == len(runs)

  return True

def prefix_compact(args, now, runs, global_stats):
  if not prefix_major_compact(args, now, runs, global_stats):
    prefix_minor_compact(args, now, runs, global_stats)

def tower_minor_compact(args, now, runs, global_stats):
  assert len(runs) >= args.tower_l0_trigger

  runs_to_merge = []
  size_sum = 0

  if args.debug: print('tower_minor: %s' % runs_to_str(runs))

  for x, r in enumerate(runs[:]):
    runs_to_merge = runs[:x]

    # Always accept the first N, they are from memtable flushes
    if x < args.tower_l0_trigger:
      assert r['size'] <= args.memtable_mb

    elif (size_sum * args.tower_size_ratio) < r['size']:
      # Next run is too much larger than sum of sizes of previous runs
      break

    size_sum += r['size']

  assert runs_to_merge
  if args.debug: print('tower_minor')
  merge_runs(runs_to_merge, args, now, runs, global_stats, False, False)
  global_stats['minor'] += 1
  return True

def tower_major_compact(args, now, runs, global_stats):
  if args.space_amp_goal == 0 or len(runs) < 2:
    return False

  size_without_max = sum_run_sizes(runs[:-1])
  if size_without_max < ((args.space_amp_goal/100.0) * runs[-1]['size']):
    return False

  # Passing previous check means there might be too much space-amp, do a major compaction
  runs_to_merge = runs

  # TODO: does max_merge_width apply here?
  #if len(runs_to_merge) > args.max_merge_width:
  #  runs_to_merge = runs[(len(runs) - args.max_merge_width):]
  #  assert len(runs_to_merge) == args.max_merge_width

  new_len = len(runs) - len(runs_to_merge) + 1 
  if args.debug: print('tower_major')
  merge_runs(runs_to_merge, args, now, runs, global_stats, True, False)
  global_stats['major'] += 1
  assert new_len == len(runs)

  return True

def tower_compact(args, now, runs, global_stats):
  global_stats['tower_l0_size'] += 1

  if tower_major_compact(args, now, runs, global_stats):
    global_stats['tower_l0_size'] = 0
    return

  if global_stats['tower_l0_size'] >= args.tower_l0_trigger:
    tower_minor_compact(args, now, runs, global_stats)
    global_stats['tower_l0_size'] = 0
    return
 
def run_sim(args, compact_func):
  runs = []
  interval_stats = []
  global_stats = { 'ingest':0, 'compact_wr':0, 'minor':0, 'major':0, 'tower_l0_size':0 }

  # TODO: better estimate for avg_wr, maybe l2(size/fanout)??
  runs.insert(0, make_run(args.init_size_gb * 1024, 0, 0, 10))

  for x in range(args.run_time):
    runs.insert(0, make_run(args.memtable_mb, x, x, 1))
    global_stats['ingest'] += args.memtable_mb
    update_stats(x, runs, interval_stats, global_stats)
    iw = global_stats['ingest']
    cw = global_stats['compact_wr']
    print('full: %d at, %d i, %d c, %.1f wa : %s' % (
        x, iw, cw, (cw + iw) / (iw * 1.0), runs_to_str(runs, full=True)))
    print('short: %d at, %d i, %d c, %.1f wa : %s' % (
        x, iw, cw, (cw + iw) / (iw * 1.0), runs_to_str(runs, short=True)))
    compact_func(args, x, runs, global_stats)
 
  final_stats(runs, interval_stats, global_stats)

def stcs_sim(args):
  run_sim(args, stcs_compact)

def prefix_sim(args):
  run_sim(args, prefix_compact)

def tower_sim(args):
  run_sim(args, tower_compact)

def main(argv):
  args = parse_options(argv)
  random.seed(args.seed)

  if args.prefix_benefit:
    print('--tower_benefit not supported yet')
    sys.exit(-1)

  if args.workload == 'insert':
    if args.keep_pct != 100:
      print('keep_pct was %d and must be 100 for --workload=insert' % args.keep_pct)
      sys.exit(-1)
    if args.space_amp_goal != 0:
      printf('space_amp_goal must be 0 for --workload=insert')
      sys.exit(-1)
  elif args.workload == 'overwrite':
    args.max_size_gb = args.init_size_gb
  else:
    print('--workload=%s not supported' % args.workload)
    sys.exit(-1)

  if args.algorithm == 'stcs':
    stcs_sim(args)
  elif args.algorithm == 'prefix':
    prefix_sim(args)
  elif args.algorithm == 'tower':
    tower_sim(args)
  else:
    print('Value for algorithm(%s) not supported' % args.algorithm)
    sys.exit(-1)

if __name__ == '__main__':
  sys.exit(main(sys.argv))

