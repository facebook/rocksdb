//  Copyright (c) 2017-present, Toshiba Memory America, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// scaf #define IITIMING  // set to log timing breakdown for the iterator
#include <inttypes.h>
#include "db/value_log_iterator.h"
#include "rocksdb/status.h"
#include "util/string_util.h"

namespace rocksdb {
// Iterator class to go through the files for Active Recycling, in order
//
// This does essentially the same thing as the TwoLevelIterator, but without a LevelFilesBrief, going forward only, and with the facility for
// determining when an input file is exhausted.  Rather than put switches
// into all the other iterator types, we just do everything here, in a single level.
  RecyclingIterator::RecyclingIterator(Compaction *compaction_,  // pointer to all compaction data including files
    const EnvOptions& env_options_  //  pointer to environmant data
  ) :
    compaction(compaction_),
    file_index((size_t)-1),  // init pointing before first file
    file_iterator(nullptr),  // init no iterator for file
    env_options(env_options_)
  {
    // These read options are copied from MakeInputIterator in version_set.cc
    read_options.verify_checksums = true;
    read_options.fill_cache = false;
    read_options.total_order_seek = true;
  }

  // Go to the next key.  The current key must be valid, or else we must be at the beginning
  void RecyclingIterator::Next() {
    // If there is a current iterator, see if it has another kv
    if(file_iterator==nullptr || (file_iterator->Next(),!file_iterator->Valid())) {
      // the current iterator, if any, has expired.  Advance to the next one
      do {   // loop in case there is a file with no kvs
        // If there is no next file, we are through
        if(++file_index >= compaction->inputs()->size())break;  // exit if no more files
        // Create the iterator for the next (or first) file
        file_iterator.reset(compaction->column_family_data()->table_cache()->NewIterator(
          read_options, env_options,
          compaction->column_family_data()->internal_comparator() /* not used */,
          *(*compaction->inputs())[file_index][0], nullptr /* range_del_agg */,
          nullptr /* prefix extractor */, nullptr /* don't need reference to table */,
	  nullptr /* no file_read_hist */, TableReaderCaller::kUncategorized /* caller */, nullptr /* arena */,
	  false /* skip_filters */, (*compaction->inputs())[file_index][0]->level, nullptr /* smallest_compaction_key */,
	  nullptr /* largest_compaction_key */));
        // start at the beginning of the next file
        file_iterator->SeekToFirst();             
      } while(!file_iterator->Valid());
    }
  }

// Make sure there are at least reservation spaces in vec.  We don't just use reserve directly,
// because it reserves the minimum requested, which leads to quadratic performance
static void reserveatleast(std::vector<NoInitChar>& vec, size_t reservation) {
  if(vec.size()+reservation <= vec.capacity()) return;  // return fast if no need to expand
  size_t newcap = vec.capacity() & -0x40;   // keep capacity a nice boundary
  if(newcap<1024)newcap=1024;   // start with a minimum above 0 so it can grow by multiplication
  while(newcap < vec.size()+reservation)newcap = (size_t)(newcap*1.4) & -0x40;  // grow in large jumps until big enough
  vec.reserve(newcap);
}

// Append the addend to end of charvec, by reserving enough space and then copying in the data
static void appendtovector(std::vector<NoInitChar> &charvec, const Slice &addend) {
  reserveatleast(charvec,addend.size());  // make sure there is room for the new data
  char *bufend = (char *)charvec.data()+charvec.size();   // address of place to put new data
  charvec.resize(charvec.size()+addend.size());  // advance end pointer past end of new data
  memcpy(bufend,addend.data(),addend.size());  // move in the new data
}


// Code for building the indirect-value files.
// This iterator lies between the compaction-iterator and the builder loop.
// We read all the values, save the key/values, write indirects, and then
// return the indirect kvs one by one to the builder.
  IndirectIterator::IndirectIterator(  // this constructor is used for compaction
   CompactionIterator* c_iter,   // the input iterator that feeds us kvs.
   ColumnFamilyData* cfd,  // the column family we are working on
   const Compaction *compaction,   // various info for this compaction
   Slice *end,  // end+1 key in range, if given
   bool use_indirects,  // if false, just pass c_iter through
   RecyclingIterator *recyciter,  // null if not Active Recycling; then, points to the iterator
   int job_id,  // job id for logmsgs
   bool paranoid_file_checks  // set if we should verify references as created
 ) :
  c_iter_(c_iter),
  pcfd(cfd),
  end_(end),
  use_indirects_(use_indirects),
  current_vlog(cfd->vlog()),
  recyciter_(recyciter),
  job_id_(job_id),
  paranoid_file_checks_(paranoid_file_checks),
  compaction_ringno(compaction->ringno()),
  compaction_output_level(compaction->output_level()),
  compaction_mutable_cf_options(compaction->mutable_cf_options()),
  compaction_lastfileno(compaction->lastfileno()),
  compaction_max_compaction_bytes(compaction->max_compaction_bytes()),
  compaction_inputs_size((const_cast<Compaction *>(compaction))->inputs()->size()),
  compaction_max_output_file_size(compaction_output_level==0?0:compaction->max_output_file_size()),  // no size limit on SST files written to L0
  compaction_grandparents(&compaction->grandparents()),
  compaction_comparator(&compaction->column_family_data()->internal_comparator()),
  compaction_writebuffersize(0)
  { IndirectIteratorDo(); }

 IndirectIterator::IndirectIterator(  // this constructor used by flush
    CompactionIterator* c_iter,   // the input iterator that feeds us kvs
    ColumnFamilyData* cfd,  // VLog for this CF, if any
    bool use_indirects,   // if false, do not do any indirect processing, just pass through c_iter_
    const MutableCFOptions& mutable_cf_options,  // options in use
    int job_id,
    bool paranoid_file_checks
 ) :
  c_iter_(c_iter),
  pcfd(cfd),
  end_(nullptr),
  use_indirects_(use_indirects),
  current_vlog(cfd==nullptr?nullptr:cfd->vlog()),
  recyciter_(nullptr),
  job_id_(job_id),
  paranoid_file_checks_(paranoid_file_checks),
  compaction_ringno(0),  // if we use a ring, it's 0
  compaction_output_level(0),  // flush always goes to level 0
  compaction_mutable_cf_options(&mutable_cf_options),
  compaction_lastfileno(0),  // not used
  compaction_max_compaction_bytes(0),  // used only for grandparents
  compaction_inputs_size(0),  // used only for AR.  0 is a flag indicating flush
  compaction_max_output_file_size(0),  // flush always write a single file
  compaction_grandparents(nullptr),
  compaction_comparator(nullptr),
  compaction_writebuffersize(mutable_cf_options.write_buffer_size)
  { IndirectIteratorDo(); }



void IndirectIterator::IndirectIteratorDo() {
  // init stats we will keep
  remappeddatalen = 0;  // number of bytes that were read & rewritten to a new VLog position
  bytesintocompression = 0;  // number of bytes split off to go to VLog
  nfileswritten = 0;  // number of files created

  // If indirects are disabled, we have nothing to do.  We will just be returning values from c_iter_.
  if(!use_indirects_)return;
#ifdef IITIMING
  const uint64_t start_micros = current_vlog->immdbopts_->env->NowMicros();
  std::vector<uint64_t> iitimevec(10,0);
#endif
  // For Active Recycling, use the ringno chosen earlier; otherwise use the ringno for the output level
  if(recyciter_!=nullptr)outputringno = compaction_ringno;   // AR
  else{

    outputringno = current_vlog->VLogRingNoForLevelOutput(compaction_output_level);  // get the ring number we will write output to
#if DEBLEVEL&4
printf("Creating iterator for level=%d, earliest_passthrough=",compaction->output_level());
#endif
  }
  if(outputringno+1==0){use_indirects_ = false; return;}  // if no output ring, skip looking for indirects
  outputring = current_vlog->VLogRingFromNo((int)outputringno);  // the ring we will write values to

  // Calculate the remapping threshold.  References earlier than this will be remapped.
  //
  // It is not necessary to lock the ring to calculate the remapping - any valid value is OK - but we do need to do an
  // atomic read to make sure we get a recent one
  //
  // We have to calculate a threshold for our output ring.  A reference to any other ring will automatically be passed through
  // For active Recycling, we stop just a few files above the last remapped file, because any remapping that does not
  // result in an empty file just ADDS to fragmentation.  For normal compaction, we copy a fixed fraction of the ring
  VLogRingRefFileno head = current_vlog->rings_[outputringno]->atomics.fd_ring_head_fileno.load(std::memory_order_acquire);  // last file with data
  VLogRingRefFileno tail = current_vlog->rings_[outputringno]->atomics.fd_ring_tail_fileno.load(std::memory_order_acquire);  // first file with live refs
  if(head>tail)head-=tail; else head=0;  // change 'head' to head-tail.  It would be possible for tail to pass head, on an
         // empty ring.  What happens then doesn't matter, but to keep things polite we check for it rather than overflowing the unsigned subtraction.
  earliest_passthrough = (VLogRingRefFileno)(tail + 
                                                  0.01 * ((recyciter_!=nullptr) ? compaction_mutable_cf_options->fraction_remapped_during_active_recycling
                                                                               : compaction_mutable_cf_options->fraction_remapped_during_compaction)[outputringno]
                                                  * head);  // calc file# before which we remap.  fraction is expressed as percentage
        // scaf bug this creates one passthrough for all rings during AR, whilst they might have different thresholds
  // For Active Recycling, since the aim is to free old files, we make sure we remap everything in the files we are going to delete.  But we should remap a little
  // more than that: the same SSTs that write to the oldest VLog files probably wrote to a sequence of VLog files, and it would be a shame to recycle them only to have to
  // come and do it again to pick up the other half of the files.  So, How much more?  Good question.  We would like to know how many VLog files were written by the
  // compaction that created them.  Alas, there is no way to know that, or even to make a good guess.  So, we hope that the user has responsibly sized the files so that
  // there aren't many more VLog files than SSTs, and we pick a number that is more than the number of files we expect in a compaction.  It is OK to err on the high side,
  // because once an SST has been compacted it probably won't be revisited for a while, and thus there will be a large gap between the batch of oldest files referred to
  // in an SST and the second-oldest batch.
  if(recyciter_!=nullptr)earliest_passthrough = std::max(earliest_passthrough,compaction_lastfileno+30);   // AR   scaf constant, which is max # VLog files per compaction, conservatively high
#if DEBLEVEL&4
  for(int i=0;i<earliest_passthrough.size();++i)printf("%lld ",earliest_passthrough[i]);
printf("\n");
#endif
  addedfrag.clear(); addedfrag.resize(current_vlog->rings_.size());   // init the fragmentation count to 0 for each ring

  // Get the compression information to use for this file
  compressiontype = compaction_mutable_cf_options->ring_compression_style[outputringno];

  // Calculate the total size of keys+values that we will allow in a single compaction block.
  double maxcompbytes;  // max size of compaction block
  if(compaction_inputs_size){  // if compaction...
    // This is enough to hold all the SSTs in max_compaction_bytes, plus 1 VLog file per each of those SSTs.  This is high for non-L0 compactions, which
    // need only enough storage for references that are being copied; but the limit is needed mainly for initial manual compaction of the whole database, which comes from L0 and has to have all the (compressed) data.
    // We will fill up to 2 compaction blocks before we start writing VLogs and SSTs
    // File size of -1 means 'unlimited', and we use a guess.
    double sstsizemult = compaction_mutable_cf_options->vlogfile_max_size[outputringno]>0 && compaction_mutable_cf_options->max_file_size[compaction_output_level]>0 ?
       (double)compaction_mutable_cf_options->vlogfile_max_size[outputringno] / (double)compaction_mutable_cf_options->max_file_size[compaction_output_level] :  // normal value
       5;  // if filesize unlimited, make the batch pretty big
    // For some reason, if compaction->max_compaction_bytes() is HIGH_VALUE gcc overflows and ends up with compactionblocksize set to 0.  We try to avoid that case
    maxcompbytes = (double)std::min(maxcompactionblocksize,compaction_max_compaction_bytes);  // size allowed for one compaction
    maxcompbytes = compactionblocksizefudge * maxcompbytes * (1 + sstsizemult);  // expanded to make multiple blocks very unlikely
  }else{
    // for Flush, we just want to be big enough to hold the write buffers
    maxcompbytes = compactionblocksizefudge * (double)compaction_writebuffersize;  // perhaps should multiply by min_write_buffer_number_to_merge, but how to access it?
  }
  // apply the size limits to the blocksize
  compactionblocksize = std::min(maxcompactionblocksize,(size_t)maxcompbytes);
  // If something was specified funny, make sure the compaction block is big enough to allow progress
  compactionblocksize = std::max(mincompactionblocksize,compactionblocksize);

  // Get the length in bytes of the smallest value that we will make indirect for the output level we are writing into
  minindirectlen = (size_t)compaction_mutable_cf_options->min_indirect_val_size[outputringno];

  // Get the initial allocation for the disk-data buffer for a block.  This is related to the size of the files going into the compaction: for most compactions, it is the remapped references
  // from the SSTs (remapped values are expanded to full size just before being written).  We allow for 30% of the indirect values to be remapped.
  // to disk).  We want the initial value to be fairly close to avoid repeated copying of very large buffers.
  // If this is a flush, we need save only enough room for 1 write buffer (plus a smidgen of overhead).
  // If it is a first compaction into the ring (implying we didn't write to VLog on flush), we have to allow enough for all the compacted L0 blocks
  // Otherwise it's a recompaction and we 

  initdiskallo = (size_t) (
    (compaction_inputs_size==0) ? 1.1 * compaction_mutable_cf_options->write_buffer_size  // flush
    :  (compaction_output_level<=current_vlog->starting_level_for_ring((int)outputringno))?  // uncompacted inputs  (scaf intra-L0 when start=1 is too big)
       compaction_mutable_cf_options->write_buffer_size*compaction_mutable_cf_options->level0_file_num_compaction_trigger  // allow for the min # compactible write buffers
     : 0.3*compaction_mutable_cf_options->target_file_size_base*(compaction_mutable_cf_options->max_bytes_for_level_multiplier+2)
    );   // into other levels, allow for as many SSTs as a normal compaction will have
  // Limit the max size, for example if the user has a huge l0-files number.  If more is needed, the buffer will be entended
  initdiskallo=std::min(maxinitdiskallo,initdiskallo);

  // For AR, create the list of number of records in each input file.
  filecumreccnts.clear(); if(recyciter_!=nullptr)filecumreccnts.resize(compaction_inputs_size);

  // Init stats for the compaction: number of bytes written to disk
  diskdatalen = 0;

  ReadAndResolveInputBlock();  // if there is an error this sets status_

  // If there is no error, perform Next which functions as a SeekToFirst.  If there is an error, we must leave valid_=0 to cause immediate failure in compaction
  if(status_.ok()){
    // The info related to references must not hiccup at batch boundaries.  Compaction moves to a new batch (via Next) before we have taken the ref0 for the previous file.
    // We initialize here, and always prevringfno holds the ring/file output by the previous Next(), and ref0 has the references as of BEFORE prevringfno.  ref0 is reset to high-value only
    // during ref0() when it is output
    prevringfno = RingFno{0,rocksdb::high_value};  // init to no previous key
    ref0_ = std::vector<uint64_t>(pcfd->vlog()->nrings(),rocksdb::high_value);  // initialize to no refs to each ring

    Next();   // first Next() gets the first key; subsequent ones return later keys
  }
}

// Read all the kvs from c_iter and save them.  We start at the first kv
// We create:
// diskdata - the compressed, CRCd, values to write to disk.  Stored in key order.
// diskrecl - the length of each value in diskdata
// keys - the keys read from c_iter read as a Slice but converted to string
// passthroughdata - values from c_iter that should be passed through (Slice)
// valueclass - bit 0 means 'value is a passthrough'; bit 1 means 'value is being converted from direct to indirect', bit 2='Error'

// We have one set of these vectors for each compaction block.  This is rather inelegant, compared to just having 2-element vector of vectors.  But it avoids indexing the vectors.
// Since virtually no compactions need the extra blocks, we accept the inelegance
void IndirectIterator::ReadAndResolveInputBlock() {
  // The Slices are references to pinned tables.  We copy them into our buffers.
  // They are immediately passed to Builder which must make a copy of the data.
// obsolete   std::string indirectbuffer;   // temp area where we read indirect values that need to be remapped
  CompressionOptions opts;  // scaf need initial dict
  const CompressionContext context(compressiontype);  // scaf need initial dict
  const CompressionInfo compression_info(opts, context, CompressionDict::GetEmptyDict(),
                                         compressiontype, 0 /* _sample_for_compression */);
  std::vector<VLogRingRefFileOffset> outputrcdend; outputrcdend.reserve(compactionblockinitkeyalloc); // each entry here is the running total of the bytecounts that will be sent to the SST from each kv
  std::vector<NoInitChar> diskdata;  diskdata.reserve(initdiskallo); // where we accumulate the data to write

  // Initialize the vectors we will write the data to, and make an initial reserve to reduce reallocation.  We increment them exponentially, and start with a rather large allocation to avoid extensions normally
  keys.clear(); keys.reserve(compactionblockinitkeyalloc*compactionblockavgkeylen);
  keylens.clear(); keylens.reserve(compactionblockinitkeyalloc);
  passthroughdata.clear(); passthroughdata.reserve(compactionblockinitkeyalloc*VLogRingRef::sstrefsize);
  passthroughrecl.clear(); passthroughrecl.reserve(compactionblockinitkeyalloc);
  diskfileref.clear(); diskfileref.reserve(compactionblockinitkeyalloc);
  valueclass.clear(); valueclass.reserve(compactionblockinitkeyalloc);
  diskrecl.clear(); diskrecl.reserve(compactionblockinitkeyalloc);

  size_t totalsstlen=0;  // total length so far that will be written to the SST
  size_t bytesresvindiskdata=0;  // total length that we will write to disk.  diskdata contains a mixture of references and actual data

  // Loop over each input kv, till we run out or the blocks are filled
  while((inputnotempty = (c_iter_->Valid() && 
         !(recyciter_==nullptr && end_ != nullptr && pcfd->user_comparator()->Compare(c_iter_->user_key(), *end_) >= 0)))) {  // When the key is too high, don't process it
#ifdef IITIMING
    iitimevec[0] += current_vlog->immdbopts_->env->NowMicros() - start_micros;  // point 0 - top of loop
#endif

    // check to see if the compaction batch is full.  If so, switch to the other one; if both are full, exit loop
    if(totalsstlen+bytesresvindiskdata > compactionblocksize  && recyciter_==nullptr){  // never break up an AR.  But they shouldn't get big anyway
      // main compaction block is full.  If the overflow is not empty, we have to stop and process the overflow
      ROCKS_LOG_WARN(current_vlog->immdbopts_->info_log,
        "JOB [%d] IndirectIterator: compaction batch full with %" PRIu64 " values",job_id_,diskrecl.size());
      if(valueclass2.size()!=0)break;  // if 2 full blocks, stop
      ROCKS_LOG_WARN(current_vlog->immdbopts_->info_log,
        "JOB [%d] IndirectIterator: empty batch found, switching over to it",job_id_);
      // Here when the first block fills.  The second block is empty, so we just swap the current block into the overflow, which will reset the current to empty
      outputrcdend2.swap(outputrcdend); diskdata2.swap(diskdata); keys2.swap(keys); keylens2.swap(keylens); passthroughdata2.swap(passthroughdata);
        passthroughrecl2.swap(passthroughrecl); diskfileref2.swap(diskfileref); valueclass2.swap(valueclass); diskrecl2.swap(diskrecl);
      // Reserve space in the main block to reduce allocation overhead.
      keys.reserve(compactionblockinitkeyalloc*compactionblockavgkeylen); keylens.reserve(compactionblockinitkeyalloc); passthroughdata.reserve(compactionblockinitkeyalloc*VLogRingRef::sstrefsize);
        passthroughrecl.reserve(compactionblockinitkeyalloc); diskfileref.reserve(compactionblockinitkeyalloc); valueclass.reserve(compactionblockinitkeyalloc); diskrecl.reserve(compactionblockinitkeyalloc);
      // account for the disk data written, before it disappears
      diskdatalen += bytesresvindiskdata;
      // Reset output pointers to 0
      totalsstlen=0; bytesresvindiskdata=0;
    }

    // process this kv.  It is valid and the key is not past the specified ending key
    char vclass;   // disposition of this value
    size_t sstvaluelen;  // length of the value that will be written to the sst for this kv

    // If there is error status, save it.  We save only errors
    if(c_iter_->status().ok())vclass = vNone;
    else {
      vclass = vHasError;   // indicate error on this key
      inputerrorstatus.push_back(c_iter_->status());  // save the full error
    }

    // Classify the value, as (2) a value to be passed through, not indirect (3) a direct value that needs to be converted to indirect;
    // (1) an indirect reference that needs to be remapped; (4) an indirect value that is passed through
    //
    // For case 2, the value is copied to passthroughdata
    // For case 3, the value is compressed and CRCd and written to diskdata
    // For case 1, the value is put into diskdata and not modified; it will be replaced by the remapped data when the output buffer is built
    // For case 4, the value (16 bytes) is passed through, copied to passthroughdata
    const Slice &key = c_iter_->key();  // read the key
    // Because the compaction_iterator builds all its return keys in the same buffer, we have to move the key
    // into an area that won't get overwritten.  To avoid lots of memory allocation we jam all the keys into one vector,
    // and keep a vector of lengths
 
// obsolete       keys.append(key.data(),key.size());   // save the key...
    appendtovector(keys,key);  // collect new data into the written-to-disk area
    keylens.push_back(keys.size());    // ... and its cumulative length
    Slice &val = (Slice &) c_iter_->value();  // read the value
    sstvaluelen = val.size();  // if value passes through, its length will go to the SST
    if(IsTypeDirect(c_iter_->ikey().type) && recyciter_==nullptr && val.size() > minindirectlen ) {  // remap Direct type if length is big enough - but never if AR
      // direct value that should be indirected
#ifdef IITIMING
      iitimevec[1] += current_vlog->immdbopts_->env->NowMicros() - start_micros;  // point 1 - starting value handling
#endif
      bytesintocompression += val.size();  // count length into compression
      std::string compresseddata;  // place the compressed string will go
      // Compress the data.  This will never fail; if there is an error, we just get uncompressed data
      CompressionType ctype = CompressForVLog(std::string(val.data(),val.size()),compressiontype,compression_info,&compresseddata);
#ifdef IITIMING
      iitimevec[2] += current_vlog->immdbopts_->env->NowMicros() - start_micros;  // point 2 - after compression
#endif
      // Move the compression type and the compressed data into the output area, jammed together
      size_t ctypeindex = diskdata.size();  // offset to the new record
      reserveatleast(diskdata,1+4+compresseddata.size());  // make sure there's room for header+CRC
      diskdata.push_back((char)ctype);
      appendtovector(diskdata,compresseddata);  // collect new data into the written-to-disk area
#ifdef IITIMING
      iitimevec[3] += current_vlog->immdbopts_->env->NowMicros() - start_micros;  // point 3 - after copy
#endif
      // CRC the type/data and move the CRC to the output record
      uint32_t crcint = ~crc32c::Value((char *)diskdata.data()+ctypeindex,diskdata.size()-ctypeindex);  // take CRC, including the added 0s; complement to remove ffff format
      for(int iii = 4;iii>0;--iii)diskdata.push_back((char)0); // insert space for the CRC
      // Append the CRC to the type/data, giving final record format of type/data/CRC.  Write as an int32, possibly unaligned.  By using the processor's natural format we allow CRC instructions to work
// UBSAN complains      *(uint32_t*)((char *)diskdata.data()+diskdata.size()-4) = crcint;
      // To silence UBSAN, use memcpy.  But the CRC code itself is riddled with unaligned accesses
      memcpy((char *)diskdata.data()+diskdata.size()-4,&crcint,4);   // preserve native processor order so that CRC will work on the combined
#ifdef IITIMING
      iitimevec[4] += current_vlog->immdbopts_->env->NowMicros() - start_micros;  // point 4 - after CRC
#endif
      vclass += vIndirectFirstMap;  // indicate the conversion.  We always have a value in diskrecl when we set this vclass
      // We have built the compressed/CRCd record.  save its length
      diskrecl.push_back(bytesresvindiskdata += diskdata.size()-ctypeindex);   // write running sum of record lengths, i. e. current total allocated size after we add this record
      sstvaluelen = VLogRingRef::sstrefsize;  // what we write to the SST will be a reference
    } else if(IsTypeIndirect(c_iter_->ikey().type)) {  // is indirect ref?
      // value is indirect; does it need to be remapped?
      assert(val.size()==VLogRingRef::sstrefsize);  // should be a reference
      // If the reference is ill-formed, create an error if there wasn't one already on the key
      if(val.size()!=VLogRingRef::sstrefsize){
        ROCKS_LOG_ERROR(current_vlog->immdbopts_->info_log,
          "During compaction: indirect reference has length that is not %d",VLogRingRef::sstrefsize);
        if(vclass<vHasError){   // Don't create an error for this key if it carries one already
          inputerrorstatus.push_back(Status::Corruption("indirect reference is ill-formed."));
          vclass += vHasError;  // indicate that this key now carries error status...
          val = Slice();   // expunge the errant reference.  This will be treated as a passthrough below
          sstvaluelen = 0;  // the value now has 0 length.  Non-erroneous indirects keep their length from the file
        }
      } else {
        // Valid indirect reference.  See if it needs to be remapped: too old, or not in our output ring
        VLogRingRef ref(val.data());   // analyze the reference
        assert(ref.Ringno()<addedfrag.size());  // should be a reference
        if(ref.Ringno()>=addedfrag.size()) {  // If ring does not exist for this CF, that's an error
          ROCKS_LOG_ERROR(current_vlog->immdbopts_->info_log,
            "During compaction: reference is to ring %d, but there are only %" PRIu64 " rings",ref.Ringno(),addedfrag.size());
          if(vclass<vHasError){   // Don't create an error for this key if it carries one already
            inputerrorstatus.push_back(Status::Corruption("indirect reference is ill-formed."));
            vclass += vHasError;  // indicate that this key now carries error status...
            val = Slice();   // expunge the errant reference.  This will be treated as a passthrough below
            sstvaluelen = 0;  // the value now has 0 length.  Non-erroneous indirects keep their length from the file
          }
        } else if(ref.Ringno()!=outputringno || ref.Fileno()<earliest_passthrough) {  // file number is too low to pass through
          // indirect value being remapped.  Reserve enough space for the data in diskrecl, but move only the reference to diskdata.
          // When we go to write out the data we will copy it from the VLog.  That way we don't have to buffer up lots of copied values,
          // which is important if the compaction is very large, as can happen with Active Recycling
          vclass += vIndirectRemapped;  // indicate remapping     We always have a value in diskrecl when we set this vclass
          appendtovector(diskdata,val);  // write the reference to the disk-data area
          remappeddatalen += ref.Len();  // add to count of remapped bytes

          // move in the record length of the reference
          diskrecl.push_back(bytesresvindiskdata += ref.Len());   // write running sum of record lengths, i. e. current total size of diskdata after remapped references are expanded
        } else {
          // indirect value, passed through (normal case).  Mark it as a passthrough, and install the file number in the
          // reference for the ring so it can contribute to the earliest-ref for this file
          vclass += vPassthroughIndirect;  // indirect data passes through...
          diskfileref.push_back(RingFno{ref.Ringno(),ref.Fileno()});   // ... and we save the ring/file of the reference
          // As described below, we must copy the data that is going to be passed through
// obsolete            passthroughdata.append(val.data(),val.size());    // copy the data
          appendtovector(passthroughdata,val);  // copy the reference as data to be passed back to compaction but not written to disk
          passthroughrecl.push_back(val.size());  // save its length too
          // of all the data referenced in the VLog, this is the only data that DOES NOT turn into fragmentation.  Anything else - deletions or remapping -
          // does produce fragmentation.  We subtract the passthrough data from the frag count.  Later, we will add in the total number of bytes referenced to get
          // the total fragmentation added.
          addedfrag[ref.Ringno()] -= ref.Len();
        }
      }
    }
    if(!(vclass&~vHasError)) {
      // not classified above; must be passthrough, and not indirect
      vclass += vPassthroughDirect;  // indicate passthrough
      // Regrettably we have to make a copy of the passthrough data.  Even though the original data is pinned in SSTs,
      // anything returned from a merge uses buffers in the compaction_iterator that are overwritten after each merge.
      // Since most passthrough data is short (otherwise why use indirects?), this is probably not a big problem; the
      // solution would be to keep all merge results valid for the life of the compaction_iterator.
      appendtovector(passthroughdata,val);  // copy the data
// obsolete        passthroughdata.append(val.data(),val.size());    // copy the data
      passthroughrecl.push_back(val.size());
    }
    // save the type of record for use in the replay
    valueclass.push_back(vclass);

    // save the total length of the kv that will be written to the SST, so we can plan file sizes
// scaf emulate differential key encoding to figure this length
    outputrcdend.push_back(totalsstlen += key.size()+VarintLength(key.size())+sstvaluelen+VarintLength(sstvaluelen));

    // Associate the valid kv with the file it comes from.  We store the current record # into the file# slot it came from, so
    // that when it's all over each slot contains the record# of the last record (except for empty files which contain 0 as the ending record#)
    if(recyciter_!=nullptr)filecumreccnts[recyciter_->fileindex()] = valueclass.size();
#ifdef IITIMING
    iitimevec[5] += current_vlog->immdbopts_->env->NowMicros() - start_micros;  // point 5 - all loop processing except Next
#endif

    // We have processed one key from the compaction iterator - get the next one
    // This is the last thing we do in processng a kv.  If the key is out of bounds, we don't use it AND we don't call Next().
    // Next() is what accounts for the VLog space used by an indirect reference that contributes to a compaction.
    c_iter_->Next();
#ifdef IITIMING
    iitimevec[6] += current_vlog->immdbopts_->env->NowMicros() - start_micros;  // point 6 - after Next
#endif
  }

  // All values have been read from c_iter for the current block
  // inputnotempty is set if there are more input keys to process

  // If the data overflowed a compaction block, both compaction blocks will have some data, and the next data to process will be in the overflow (i. e. the 2 suffix).
  // We will have to swap the blocks to bring the next-to-process back to the main block.  In addition, if there are no more keys after the ones we have seen, we
  // append the overflow to the main so that we can process them both together and avoid having a runt block
  if(valueclass2.size()!=0){
     ROCKS_LOG_WARN(current_vlog->immdbopts_->info_log,
       "JOB [%d] IndirectIterator: processing stacked compaction batch with %" PRIu64 " values",job_id_,diskrecl2.size());
    // swap overflow back to main
    outputrcdend2.swap(outputrcdend); diskdata2.swap(diskdata); keys2.swap(keys); keylens2.swap(keylens); passthroughdata2.swap(passthroughdata);
      passthroughrecl2.swap(passthroughrecl); diskfileref2.swap(diskfileref); valueclass2.swap(valueclass); diskrecl2.swap(diskrecl);
    // if there are no more keys, combine the main and overflow
    if(!inputnotempty){
     ROCKS_LOG_WARN(current_vlog->immdbopts_->info_log,
       "JOB [%d] IndirectIterator: combining second compaction batch with %" PRIu64 " values",job_id_,diskrecl2.size());
      // for the values that are running totals, add the main ending value to all the values in the overflow.  Only if the main is not empty & thus contains a valid ending value
      if(outputrcdend.size()){for(auto& atom : outputrcdend2)atom += outputrcdend.back();}
      if(diskrecl.size()){for(auto& atom : diskrecl2)atom += diskrecl.back();}
      if(keylens.size()){for(auto& atom : keylens2)atom += keylens.back();}
      // append the overflow to the main.
      outputrcdend.insert(outputrcdend.end(), outputrcdend2.begin(), outputrcdend2.end()); outputrcdend2.clear();
      diskdata.insert(diskdata.end(), diskdata2.begin(), diskdata2.end()); diskdata2.clear();
      keys.insert(keys.end(), keys2.begin(), keys2.end()); keys2.clear();
      keylens.insert(keylens.end(), keylens2.begin(), keylens2.end()); keylens2.clear();
      passthroughdata.insert(passthroughdata.end(), passthroughdata2.begin(), passthroughdata2.end()); passthroughdata2.clear();
      passthroughrecl.insert(passthroughrecl.end(), passthroughrecl2.begin(), passthroughrecl2.end()); passthroughrecl2.clear();
      diskfileref.insert(diskfileref.end(), diskfileref2.begin(), diskfileref2.end()); diskfileref2.clear();
      valueclass.insert(valueclass.end(), valueclass2.begin(), valueclass2.end()); valueclass2.clear();
      diskrecl.insert(diskrecl.end(), diskrecl2.begin(), diskrecl2.end()); diskrecl2.clear();
    }
  }

  // After we have read all the keys, see how many bytes were read from each ring.  They will become fragmentation, to the extent they were not passed through
  if(!inputnotempty){   // after every key has been read, we can ask the iterator for its totals
    std::vector<int64_t> refsread(std::vector<int64_t>(addedfrag.size()));  // for each ring, the # bytes referred to
    c_iter_->RingBytesRefd(refsread);  // read bytesread from the iterator
    for(uint32_t i=0;i<addedfrag.size();++i)addedfrag[i] += refsread[i];  //  every byte will add to fragmentation, if not passed through
  }

  // TODO: It might be worthwhile to sort the kvs by key.  This would be needed only during Active Recycling, since they are
  // automatically sorted during compaction.  Perhaps we could merge by level.

#ifdef IITIMING
  iitimevec[7] += current_vlog->immdbopts_->env->NowMicros() - start_micros;  // point 7 - before write to VLog
#endif
  // Allocate space in the Value Log and write the values out, and save the information for assigning references
  VLogRingRefFileOffset initfrag;  // fragmentation created with initial writing of files
  outputring->VLogRingWrite(current_vlog,diskdata,diskrecl,valueclass,compaction_mutable_cf_options->vlogfile_max_size[outputringno],job_id_,firstdiskref,fileendoffsets,outputerrorstatus,initfrag);
  addedfrag[outputringno] += initfrag;  // add end-of-file fragmentation to fragmentation total
  // Now, before any SSTs have been released, switch over the first time from 'waiting for SST/VLog info' to 'normal operation'
  current_vlog->SetInitComplete();
  nextdiskref = firstdiskref;    // remember where we start, and initialize the running pointer to the disk data
#ifdef IITIMING
  iitimevec[8] += current_vlog->immdbopts_->env->NowMicros() - start_micros;  // point 8 - after write to VLog
#endif
  // add the new batch into the file-related stats we keep
  nfileswritten += fileendoffsets.size();
// obsolete   outputring->UpdateDeadband(fileendoffsets.size(),compaction_mutable_cf_options->active_recycling_size_trigger[outputringno]);
  if(nfileswritten) {  // start file# of 0 means delete, so don't add an entry if there are no files added
    // for huge compactions, coalesce adjacent runs.  Not necessary, supposedly
    if(createdfilelist.size() && createdfilelist.back()==firstdiskref.Fileno()-1) {
      createdfilelist.pop_back();  // new extends previous; remove previous end
    }else{
      createdfilelist.push_back(firstdiskref.Fileno());  // output new start
    }
    createdfilelist.push_back(firstdiskref.Fileno()+fileendoffsets.size()-1);   // append new end
  }

  // save what we need to return to stats
  diskdatalen += bytesresvindiskdata+initfrag;  // save # bytes written for stats report.  This is the actual file length on disk, including amounts added in rounding
// obsolete printf("diskdatalen=%" PRIu64 ", initfrag=%" PRIu64 ", refsread[0]=%" PRIu64 ", addedfrag[0]=%" PRIu64 "\n",diskdatalen,initfrag,refsread[0],addedfrag[0]);  // scaf
#if DEBLEVEL&4
printf("%" PRIu64 " keys read, with %" PRIu64 " passthroughs\n",keylens.size(),passthroughrecl.size());
#endif

#ifdef IITIMING
  ROCKS_LOG_INFO(
      current_vlog->immdbopts_->info_log, "[%s] Total IndirectIterator time %5.2f (comp=%5.2f, move=%5.2f, CRC=%5.2f, Next()=%5.2f, VLogWr=%5.2f, other=%5.2f).",
      current_vlog->cfd_->GetName().c_str(),
      iitimevec[8]*1e-6, (iitimevec[2]-iitimevec[1])*1e-6, (iitimevec[3]-iitimevec[2])*1e-6, (iitimevec[4]-iitimevec[3])*1e-6, (iitimevec[6]-iitimevec[5])*1e-6, (iitimevec[8]-iitimevec[7])*1e-6,
      (iitimevec[5]-iitimevec[4]+iitimevec[1]-iitimevec[0])*1e-6
      );
#endif

  if(outputerrorstatus.empty()) {
    // No error reading keys and writing to disk.
    if(recyciter_==nullptr) {      // If this is NOT Active Recycling, allocate the kvs to SSTs so as to keep files sizes equal.
      if(compaction_max_output_file_size!=0) {   // this is 0 for flush
        // here for compaction.  break the records into files
        BreakRecordsIntoFiles(filecumreccnts, outputrcdend, compaction_max_output_file_size,
          compaction_grandparents, &keys, &keylens, compaction_comparator,
          compaction_max_compaction_bytes);  // calculate filecumreccnts, including use of grandparent info
        // If there are too many data files per SST, the files are too small
        if(fileendoffsets.size()>2.0*filecumreccnts.size()){
          ROCKS_LOG_WARN(
              current_vlog->immdbopts_->info_log, "[%s]  You have %5.1f VLog files per SST.  Consider increasing vlogfile_max_size.",
              current_vlog->cfd_->GetName().c_str(),
              (double)fileendoffsets.size()/(double)filecumreccnts.size());
        }
      } else {
        // here for flush.  The records all go into a single file
        fileendoffsets.push_back(outputrcdend.size());  // allocate all the records to the sole output file
      }
    }
    // now filecumreccnts has the length in kvs of each eventual output file.  For AR, we mimic the input; for compaction, we create new files
#ifdef IITIMING
    ROCKS_LOG_INFO(
        current_vlog->immdbopts_->info_log, "[%s]  Now writing %" PRIu64 " SST files, %" PRIu64 " bytes, max filesize=%" PRIu64,
        current_vlog->cfd_->GetName().c_str(),
        filecumreccnts.size(), outputrcdend.size()?outputrcdend.back():0, compaction->max_output_file_size());
#endif

    // set up to read first key
    // set up the variables for the first key
    keyno_ = 0;  // we start on the first key
    passx_ = 0;  // initialize data pointers to start of region
    diskx_ = 0;
    nextpassthroughref = 0;  // init offset of first passthrough record
    filex_ = 0;  // indicate that we are creating references to the first file in filelengths
    statusx_ = 0;  // reset input error pointer to first error
    ostatusx_ = 0;  // reset output error pointer to first error
    passthroughrefx_ = 0;  // reset pointer to passthrough file/ring
    outputfileno = 0;  // init the output pointer for the next OverrideClose.  Next is called AFTER OverrideClose

  } else {
    // If there is an error(s) writing to the VLog, we don't have any way to give them all.  Until such an
    // interface is created, we will just give initial error status, which will abort the compaction
    // The error log was written when the inital error was found
    status_ = Status::Corruption("error writing to VLog");
  }
}


  // set up key_ etc. with the data for the next valid key, whose index in our tables is keyno_
  // We copy all these into temp variables, because the user is allowed to call key() and value() repeatedly and
  // we don't want to repeat any work
  //
  // keyno_ is the index of the key we are about to return, if valid
  // passx_ is the index of the next passthrough record
  // nextpassthroughref is the index of the next passthrough byte to return
  // diskx_ is the index of the next disk data offset (into diskrecl_)
  // nextdiskref_ has the file info for the next disk block
  // diskrecl_ has running total (i. e. record-end points) of each record written en bloc
  // We update all these variables to be ready for the next record
  void IndirectIterator::Next() {
    VLogRingRefFileOffset currendlen;  // the cumulative length of disk records up to the current one

    // If this table type doesn't support indirects, revert to the standard compaction iterator
    if(!use_indirects_){ c_iter_->Next(); return; }
    // Here indirects are supported.
  
    // Include the previous key's file/ring in the current result (because the compaction job calls Valid() then OverrideClose() then Next() then ref0() for
    // the current output file).  We do this even if the current key is invalid, because it is the previous key that we have to
    // include in the current file
    if(ref0_[prevringfno.ringno]>prevringfno.fileno)
      ref0_[prevringfno.ringno]=prevringfno.fileno;  // if current > new, switch to new

    // If there are no keys remaining in the current block, go get a new block.
    if(keyno_ >= valueclass.size() && inputnotempty)ReadAndResolveInputBlock();

    // NOTE: if ReadAndResolve was called, it has reset all the pointers to point to the start of the new batch, and has reset the data vectors.
    // It also set nextdiskref for the first record of the new block

    //  If there was an error writing files, or we have returned all the keys, set this one as invalid
    valid_ = status_.ok() && keyno_ < valueclass.size();
    if(valid_) {
      // There is another key to return.  Retrieve the key info and parse it

      // If there are errors about, we need to make sure we attach the errors to the correct keys.  (This is not used currently since we just abort with error status on any write error)
      // First we see if there was an input error for the key we are working on
      int vclass = valueclass[keyno_];   // extract key type
      if(vclass<vHasError)status_ = Status();  // if no error on input, init status to good
      else {
        // There was an input error when this key was read.  Set the return status based on the that error.
        // We will go ahead and process the key normally, in case the error was not fatal
        status_ = inputerrorstatus[statusx_++];  // recover input error status, advance to next error
        vclass -= vHasError;  // remove error flag, leaving the value type
      }

      size_t keyx = keyno_ ? keylens[keyno_-1] : 0;  // starting position of previous key
      ParseInternalKey(Slice((char *)keys.data()+keyx,keylens[keyno_] - keyx),&ikey_);  // Fill in the parsed result area

      prevringfno = RingFno{0,rocksdb::high_value};  // set to no indirect ref here
      // Create its info based on its class
      switch(vclass) {
      case vPassthroughIndirect:
        // Indirect passthrough.  We need to retrieve the reference file# and apply it
	// copy indirect ref, advance to next indirect ref
        prevringfno = diskfileref[passthroughrefx_++]; // fallthrough
      case vPassthroughDirect:
       // passthrough, either kind.  Fill in the slice from the buffered data, and advance pointers to next record
        value_.install((char *)passthroughdata.data() + nextpassthroughref,passthroughrecl[passx_++]);  // nextpassthroughref is current data offset
        nextpassthroughref += value_.size_;  // advance data offset for next time
        break;
      case vIndirectFirstMap:
        // first mapping: change the value type to indicate indirect
	// must be one or the other
        ikey_.type = (ikey_.type==kTypeValue) ? kTypeIndirectValue : kTypeIndirectMerge; // fallthrough
      case vIndirectRemapped:
        // Data taken from disk, either to be written the first time or to be rewritten for remapping
        // nextdiskref contains the next record to return
        // Fill in the slice with the current disk reference; then advance the reference to the next record
        nextdiskref.IndirectSlice(value_);  // convert nextdiskref to string form in its workarea, and point value_ to the workarea
#if 0  // use if we have to run down an errant value reference
        if(paranoid_file_checks_){
          // scaf see if the reference is OK before we send it to the SST
          std::string result;
          Status paranoidstat = current_vlog->VLogGet(value_,result);
          if(!paranoidstat.ok()){
            ROCKS_LOG_ERROR(current_vlog->immdbopts_->info_log,
              "During compaction: error checking references before handing them to compaction.  keyno_=%" PRIu64 "; ref file=%" PRIu64 ", len=%" PRIu64 ", offset=%" PRIu64 "",keyno_,nextdiskref.Fileno(),nextdiskref.Len(),nextdiskref.Offset());
            paranoid_file_checks_=false;  // to save log space, stop looking after 1 error
          }
        }
#endif
        // Save the file/ring of the record we are returning
        prevringfno = RingFno{nextdiskref.Ringno(),nextdiskref.Fileno()};
        // Advance to the next record - or the next file, getting the new file/offset/length ready in nextdiskref
        // If there is no next indirect value, don't set up for it
        currendlen = diskrecl[diskx_++];  // save end offset of current record, advance to next record
        if(diskx_<diskrecl.size()) {  // if there is going to be a next record...
          nextdiskref.SetOffset(nextdiskref.Offset()+nextdiskref.Len());   // offset of current + len of current = offset of next
          nextdiskref.SetLen(diskrecl[diskx_]-currendlen);    // endpos of next - endpos of current = length of next
          // Subtlety: if the length of the trailing values is 0, we could advance the file pointer past the last file.
          // To ensure that doesn't happen, we advance the output file only when the length of the value is nonzero.
          // It is still possible that a zero-length reference will have a filenumber past the ones we have allocated, so
          // we have to make sure that zero-length indirects (which shouldn't exist!) are not read
          if(nextdiskref.Len()){   // advance to next file only on nonempty value
            // Here, for nonempty current values only, we check to see whether there was an I/O error writing the
            // file.  We haven't advanced to the next file yet.  A negative value in the fileendoffset indicates an error.
            VLogRingRefFileOffset endofst = fileendoffsets[filex_];
            if(endofst<0){
              // error on the output file.  Return the full error information.  If there was an input error AND an output
              // error, keep the input error, since the output error will probably be reported later
              if(status_.ok())status_ = outputerrorstatus[ostatusx_];  // use error for the current file
              endofst = -endofst;  // restore endofst to positive so it can test correctly below
            }
            if(nextdiskref.Offset()==endofst) {   // if start of next rcd = endpoint of current file
              // The next reference is in the next output file.  Advance to the start of the next output file
              //if(fileendoffsets[filex_]<0)++ostatusx_;  // if we are leaving a file with error, point to the next error (if any)
              // Note: fileendoffsets[filex_]<0 is always false because fileendoffsets[x] always returns an unsigned integer.
              nextdiskref.SetFileno(nextdiskref.Fileno()+1);  // next file...
              nextdiskref.SetOffset(0);  // at the beginning...
              ++filex_;   // ... and advance to look at the ending position of the NEXT output file
            }
          }
        }
        break;
      }

      // Now that we know we have the value type right, create the total key to return to the user
      npikey.clear();  // clear the old key
      AppendInternalKey(&npikey, ikey_);
      key_.install(npikey.data(),npikey.size());  // Install data & size into the object buffer, to avoid returning stack variable

      // Advance to next position for next time
      ++keyno_;   // keyno_ always has the key-number to use for the next call to Next()
    }  // obsolete else status_ = Status();  // if key not valid, give good status
  }

}   // namespace rocksdb


	
