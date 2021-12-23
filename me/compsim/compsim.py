
import argparse
import math
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
  parser.add_argument('--algorithm', choices=['stcs', 'prefix', 'tower', 'greedy'],
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
  parser.add_argument('--keep_pct', type=int, default=100, choices=range(1,100),
                      help='Percentage of the compaction input that is kept because it is redundant. '\
                           'Must be 0 for workload=insert and in [1,100] for workload=overwrite.')
  parser.add_argument('--space_amp_goal', type=int, default=51,
                      help='Triggers compaction when sizeof(non-max runs) is >= (space_amp_goal/100 * sizeof(max run)). '\
                           'Ignored when 0, must be >= 0.')

  # Options for STCS (ScyllaDB & Cassandra)
  parser.add_argument('--stcs_bucket_high', type=float, default=1.5,
                      help='See bucket_high for STCS in ScyllaDB')
  parser.add_argument('--stcs_bucket_low', type=float, default=0.5,
                      help='See bucket_low for STCS in ScyllaDB')
  parser.add_argument('--stcs_min_sstable_size_mb', type=int, default=80,
                      help='See min_sstable_size for STCS in ScyllaDB')
  parser.add_argument('--stcs_min_threshold', type=int, default=4)
  parser.add_argument('--stcs_max_threshold', type=int, default=16)

  # Options for prefix (HBase and RocksDB)
  parser.add_argument('--prefix_read_amp_trigger', type=int, default=9,
                      help='Trigger minor compaction when there are this many sorted runs')
  parser.add_argument('--prefix_size_ratio', type=float, default=1.5,
                      help='If max_merge_width has not been reached, stop adding sorted runs '\
                           'to the next minor compaction when the next run is more than this '\
                           'much larger than the sum of the sizes of the sorted runs already selected.')
  parser.add_argument('--prefix_v2', default=False, action='store_true',
                      help='Prefix with a few tweaks')
  parser.add_argument('--prefix_min_merge_width', type=int, default=4)
  parser.add_argument('--prefix_max_merge_width', type=int, default=16)

  # Options for tower
  parser.add_argument('--tower_l0_trigger', type=int, default=8, 
                      help='Trigger minor compaction when the L0 has this many runs')
  parser.add_argument('--tower_size_ratio', type=float, default=1.5,
                      help='Similar to --prefix_size_ratio')
  parser.add_argument('--tower_min_merge_width', type=int, default=4)
  parser.add_argument('--tower_max_merge_width', type=int, default=16)

  # Options for greedy
  parser.add_argument('--greedy_read_amp_trigger', type=int, default=8, 
                      help='Trigger minor compaction when there are this many sorted runs')
  parser.add_argument('--greedy_max_merge_width', type=int, default=16)

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

def final_stats(runs, interval_stats, global_stats, args, read_amp_config, name):
  ingest = global_stats['ingest']
  write_amp = (global_stats['compact_wr'] + ingest) / ingest

  init_size_mb = args.init_size_gb * 1024
  sum_size = 0
  max_size = 0
  sum_runs = 0
  max_runs = 0
  
  for s in interval_stats:
    sum_size += s['size']
    if s['size'] > max_size: max_size = s['size']
    sum_runs += s['runs']
    if s['runs'] > max_runs: max_runs = s['runs']

  print('final: %d %.1f runs(max,avg), %.2f %.2f space-amp(max,avg), %.1f write-amp, %d sa_major, %d ra_major, %d minor, %d ra, %d sag, %s' % (
      max_runs, sum_runs / len(interval_stats), 
      max_size / init_size_mb, (sum_size / len(interval_stats)) / init_size_mb,
      write_amp, global_stats['sa_major'], global_stats['ra_major'], global_stats['minor'],
      read_amp_config, args.space_amp_goal, name))

def  merge_runs(runs_to_merge, args, now, all_runs, global_stats, sort_by_size):
  assert args.workload  == 'overwrite'

  min_ts = next_id
  max_ts = -1
  size = 0
  avg_wr = 0

  for r in runs_to_merge:
    if r['min_ts'] < min_ts: min_ts = r['min_ts']
    if r['max_ts'] > max_ts: max_ts = r['max_ts']

    # Sizeof output run should be between (max, sum) where max is sizeof largest input run
    # and sum is the sum of the sizes of the input run.
    if args.keep_pct != 100:
      print('keep_pct != 100 not supported yet')
      sys.exit(-1)
      # TODO: figure this out. This code is buggy, if the input has sizes (64, 576) and keep_pct=90
      #       then the output also has size 576.
      # size = size + (r['size'] * (args.keep_pct/100.0))
    else:
      size = size + r['size']

    # Compute weighted average of avg_wr from input runs
    avg_wr = avg_wr + r['size'] * r['avg_wr']
    if args.debug: print('merge_runs: size = %.1f' % size)

  if args.workload == 'overwrite':
    if len(runs_to_merge) == len(all_runs):
      # This is a major compaction
      size = args.max_size_gb * 1024
    elif size > args.max_size_gb * 1024:
      size = args.max_size_gb * 1024

  # avg_wr is a weighted average of the number of times bytes in this run have been written
  avg_wr = avg_wr / size
  # increment because compaction just copied these runs to a new run
  avg_wr += 1
  global_stats['compact_wr'] += size

  # Find place in all_runs at which merge_runs begins
  insert_at = -1
  if len(runs_to_merge) < len(all_runs):
    for x, r in enumerate(all_runs):
      insert_at = x
      if r['id'] == runs_to_merge[0]['id']:
        break
    assert insert_at != -1

  new_run = make_run(size, min_ts, max_ts, avg_wr)

  # Remove runs in merge_runs from all_runs
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

  if args.debug:
    print('merge_runs: %s input, %.1f size, %d ins_at' % (
          runs_to_str(runs_to_merge, short=True), size, insert_at))

  # Insert new_run into all_runs
  if insert_at != -1:
    all_runs.insert(insert_at, new_run)
  else:
    all_runs.append(new_run)

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

  possible_buckets = [b for b in buckets if len(b['runs']) >= args.stcs_min_threshold]

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
  if len(compact_bucket['runs']) > args.stcs_max_threshold:
    random.shuffle(compact_bucket['runs'])
    compact_bucket['runs'] = compact_bucket['runs'][0:args.stcs_max_threshold]
    if args.debug:
      print('Compact bucket after max trim: %.1f avg_size, %d runs %s' % (
          compact_bucket['avg_sz'], len(compact_bucket['runs']), runs_to_str(compact_bucket['runs'])))

  merge_runs(compact_bucket['runs'], args, now, runs, global_stats, True)
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
    merge_runs(buckets[-1]['runs'], args, now, runs, global_stats, True)
    global_stats['sa_major'] += 1
    if args.debug: print('major: 2+ runs in max bucket')
    return True
  elif len(buckets) >=2 and (
      sum_run_sizes(buckets[-2]['runs']) >=
      ((args.space_amp_goal / 100.0) * sum_run_sizes(buckets[-1]['runs']))):
    merge_runs(buckets[-1]['runs'] + buckets[-2]['runs'], args, now, runs, global_stats, True)
    global_stats['sa_major'] += 1
    if args.debug: print('major: next-to-max bucket too large')
    return True
  else:
    return False

def stcs_compact(args, now, runs, global_stats):
  stcs_major_compact(args, now, runs, global_stats)
  stcs_minor_compact(args, now, runs, global_stats)

def prefix_compact_too_many_runs(args, now, runs, use_size_ratio, global_stats):
  if len(runs) < args.prefix_read_amp_trigger: return False
  assert len(runs) >= args.prefix_min_merge_width

  max_merge_width = args.prefix_max_merge_width
  orig_max_merge_width = max_merge_width
  min_merge_width = args.prefix_min_merge_width
 
  if not use_size_ratio:
    # This is the 2nd pass that "forces" a minor compaction using a few sorted runs

    if not args.prefix_v2:
      max_merge_width = len(runs) - args.prefix_read_amp_trigger + 1

      if len(runs) == args.prefix_read_amp_trigger:
        # See ">" here, maybe should be ">="
        # https://github.com/facebook/rocksdb/blob/806d8916da26dfc82ed361a56dde790132f0d6a6/db/compaction/compaction_picker_universal.cc#L438
        # Also see https://github.com/facebook/rocksdb/issues/9275
        if args.debug: print('prefix_ctmr: false with not use_size_ratio and len equal')
        return False

      if max_merge_width < min_merge_width:
        # See https://github.com/facebook/rocksdb/issues/9275
        if args.debug: print('prefix_ctmr: false with max_mw < min_mw')
        return False

    else:
      max_merge_width = len(runs) - args.prefix_read_amp_trigger + 1
      assert max_merge_width >= 1
      if max_merge_width == 1: max_merge_width = 2
      if max_merge_width < min_merge_width: min_merge_width = max_merge_width

  if max_merge_width < 2:
    print('uh oh: %d maxmw, %d nruns, %d ra_trigger\n' % (max_merge_width, len(runs), args.prefix_read_amp_trigger))
    sys.exit(-1)

  if args.debug:
    print('prefix_compact_too_many_runs: %s use_size_ratio, (%d, %d) merge_width, %d nruns' %
           (use_size_ratio, min_merge_width, max_merge_width, len(runs)))

  for startx in range(len(runs) - min_merge_width + 1):
    # Try to form the compaction input starting at runs[startx]

    lastx = -1
    size_sum = runs[startx]['size']

    extended = False

    # x starts at 0 and when x == 0 the number of sorted runs in the candidate sequence of runs is 2
    # because iteration started at startx+1 and startx is also in the candidate sequence, thus the
    # use of "x+2" below

    for x, r in enumerate(runs[(startx+1):]):

      next_run_too_big = (size_sum * args.prefix_size_ratio) < r['size']

      if (extended or use_size_ratio) and next_run_too_big:
        # Stop adding runs when the next one is too big relative to the sum of sizes of the previous runs
        if args.debug: print('too_many: stop 1 before %d' % x)
        break;

      size_sum += r['size']
      lastx = x

      if (x+2) >= max_merge_width:
        if not use_size_ratio and args.prefix_v2 and (x+2) < orig_max_merge_width and not next_run_too_big:
          # There might be a good run that just doesn't satisfy min_merge_width
          if args.debug: print('prefix_v2: continue with %d lastx, %.1f size' % (lastx, r['size']))
          extended = True
        else:
          # Stop when max_merge_width runs were found
          # Above, x starts at 0, which is relative to (startx+1)
          if args.debug: print('too_many: stop 2 at %d' % x)
          break;

    if args.debug: print('too_many: lastx = %d' % lastx)

    # Again, x starts at 0
    if lastx != -1: lastx += startx + 1

    if args.debug: print('prefix_ctmr: %d startx, %d lastx' % (startx, lastx))

    if lastx != -1 and (lastx - startx + 1) >= min_merge_width:
      runs_to_merge = runs[startx:(lastx+1)]
      if args.debug: print('prefix_minor')
      if len(runs_to_merge) < len(runs):
        global_stats['minor'] += 1
      else:
        global_stats['ra_major'] += 1
      assert len(runs_to_merge) <= orig_max_merge_width
      merge_runs(runs_to_merge, args, now, runs, global_stats, False)
      return True

  if args.debug: print('prefix_ctmr: false at end')
  return False

def prefix_compact_space(args, now, runs, global_stats):
  if not args.prefix_v2 and len(runs) < args.prefix_read_amp_trigger:
    return False

  if args.space_amp_goal == 0 or len(runs) < 2:
    return False

  size_without_max = sum_run_sizes(runs[:-1])
  if size_without_max < ((args.space_amp_goal/100.0) * runs[-1]['size']):
    return False

  # There is too much space-amp, do a major compaction
  runs_to_merge = runs
  if len(runs_to_merge) > args.prefix_max_merge_width:
    runs_to_merge = runs[(len(runs) - args.prefix_max_merge_width):]
    assert len(runs_to_merge) == args.prefix_max_merge_width
 
  new_len = len(runs) - len(runs_to_merge) + 1 
  if args.debug: print('prefix_major')
  merge_runs(runs_to_merge, args, now, runs, global_stats, False)
  global_stats['sa_major'] += 1
  assert new_len == len(runs)

  return True

def prefix_compact(args, now, runs, global_stats):
  if prefix_compact_space(args, now, runs, global_stats):
    return
  elif prefix_compact_too_many_runs(args, now, runs, True, global_stats):
    return
  elif prefix_compact_too_many_runs(args, now, runs, False, global_stats):
    return

def greedy_compact_too_many_runs(args, now, runs, global_stats):
  if len(runs) < args.greedy_read_amp_trigger: return False

  init_size_mb = args.init_size_gb * 1024
  best = (0, 0, -1)

  # TODO: for now don't allow major compaction here, thus the "-2" and "-1" for
  # the stopping condition of the next two loops

  for startx in range(len(runs) - 2):
    for stopx in range(startx+1, len(runs) - 1):
      if (stopx - startx + 1) > args.greedy_max_merge_width:
        break
      if args.debug: print('greedy_ra: try range (%d, %d)' % (startx, stopx))

      sum_sizes = sum_run_sizes(runs[startx:(stopx+1)])
      m_after = math.log2(init_size_mb / sum_sizes)

      m_before = 0
      for r in runs[startx:(stopx+1)]:
        m_before += (r['size'] / sum_sizes) * math.log2(init_size_mb / r['size'])

      # benefit = m_before / m_after
      benefit = m_before - m_after
      if args.debug: print('greedy_ra: range(%d, %d) b(%.3f), before(%.3f), after(%.3f)' % (startx, stopx, benefit, m_before, m_after))

      if benefit > best[2]:
        best = (startx, stopx, benefit)
        if args.debug: print('greedy_ra: best range (%d, %d) benefit %.3f' % (best[0], best[1], best[2]))

  assert best[1] != 0
 
  runs_to_merge = runs[ best[0] : (best[1] + 1) ]
  if len(runs_to_merge) < len(runs):
    global_stats['minor'] += 1
  else:
    global_stats['ra_major'] += 1
  merge_runs(runs_to_merge, args, now, runs, global_stats, False)
  return True

def greedy_compact_space(args, now, runs, global_stats):
  if args.space_amp_goal == 0 or len(runs) < 2:
    return False

  size_without_max = sum_run_sizes(runs[:-1])
  if size_without_max < ((args.space_amp_goal/100.0) * runs[-1]['size']):
    return False

  # There is too much space-amp, do a major compaction
  runs_to_merge = runs
  if len(runs_to_merge) > args.greedy_max_merge_width:
    runs_to_merge = runs[(len(runs) - args.greedy_max_merge_width):]
    assert len(runs_to_merge) == args.greedy_max_merge_width
 
  new_len = len(runs) - len(runs_to_merge) + 1 
  if args.debug: print('greedy_major')
  merge_runs(runs_to_merge, args, now, runs, global_stats, False)
  global_stats['sa_major'] += 1
  assert new_len == len(runs)

  return True

def greedy_compact(args, now, runs, global_stats):
  if greedy_compact_space(args, now, runs, global_stats):
    return
  elif greedy_compact_too_many_runs(args, now, runs, global_stats):
    return

def tower_minor_compact(args, now, runs, global_stats):
  assert len(runs) >= args.tower_l0_trigger

  runs_to_merge = []
  size_sum = 0

  if args.debug: print('tower_minor: %s' % runs_to_str(runs))

  lastx = -1
  for x, r in enumerate(runs[:]):

    if x < args.tower_l0_trigger:
      # Always accept the first N, they are from memtable flushes
      assert r['size'] <= args.memtable_mb

    elif (size_sum * args.tower_size_ratio) < r['size']:
      # Next run is too much larger than sum of sizes of previous runs
      break

    lastx = x
    size_sum += r['size']

  if args.debug: print('tower_minor')
  assert lastx != -1
  runs_to_merge = runs[:(lastx+1)]

  if len(runs_to_merge) < len(runs):
    global_stats['minor'] += 1
  else:
    global_stats['ra_major'] += 1
  merge_runs(runs_to_merge, args, now, runs, global_stats, False)
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
  #if len(runs_to_merge) > args.tower_max_merge_width:
  #  runs_to_merge = runs[(len(runs) - args.tower_max_merge_width):]
  #  assert len(runs_to_merge) == args.tower_max_merge_width

  new_len = len(runs) - len(runs_to_merge) + 1 
  if args.debug: print('tower_major')
  merge_runs(runs_to_merge, args, now, runs, global_stats, False)
  global_stats['sa_major'] += 1
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
 
def run_sim(args, compact_func, read_amp_config, name):
  runs = []
  interval_stats = []
  global_stats = { 'ingest':0, 'compact_wr':0, 'minor':0, 'sa_major':0, 'ra_major':0, 'tower_l0_size':0 }

  # TODO: better estimate for avg_wr, maybe l2(size/fanout)??
  runs.insert(0, make_run(args.init_size_gb * 1024, 0, 0, 10))

  for x in range(args.run_time):
    runs.insert(0, make_run(args.memtable_mb, x, x, 1))
    global_stats['ingest'] += args.memtable_mb
    update_stats(x, runs, interval_stats, global_stats)
    iw = global_stats['ingest']
    cw = global_stats['compact_wr']
    #print('full: %d at, %d i, %d c, %.1f wa : %s' % (
    #    x, iw, cw, (cw + iw) / (iw * 1.0), runs_to_str(runs, full=True)))
    print('short: %d at, %d i, %d c, %.1f wa : %s' % (
        x, iw, cw, (cw + iw) / (iw * 1.0), runs_to_str(runs, short=True)))
    compact_func(args, x, runs, global_stats)
 
  final_stats(runs, interval_stats, global_stats, args, read_amp_config, name)

def stcs_sim(args):
  run_sim(args, stcs_compact, args.stcs_min_threshold, 'stcs')

def prefix_sim(args):
  name = 'prefix'
  if args.prefix_v2: name = 'prefix_v2'
  run_sim(args, prefix_compact, args.prefix_read_amp_trigger, name)

def tower_sim(args):
  run_sim(args, tower_compact, args.tower_l0_trigger, 'tower')

def greedy_sim(args):
  run_sim(args, greedy_compact, args.greedy_read_amp_trigger, 'greedy')

def main(argv):
  args = parse_options(argv)
  random.seed(args.seed)

  if args.workload == 'insert':
    print('--workload=insert not supported yet')
    sys.exit(-1)
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
  elif args.algorithm == 'greedy':
    greedy_sim(args)
  else:
    print('Value for algorithm(%s) not supported' % args.algorithm)
    sys.exit(-1)

if __name__ == '__main__':
  sys.exit(main(sys.argv))

