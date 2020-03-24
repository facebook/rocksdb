//  Copyright (c) 2017-present, Toshiba Memory America, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <atomic>
#include <memory>
#include <algorithm>
#include <inttypes.h>
#include <math.h>
#include "db/value_log.h"
#include "db/column_family.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "options/cf_options.h"
#include "rocksdb/status.h"
#include "util/compression.h"
#include<chrono>
#include<thread>

namespace rocksdb {
#if DELAYPROB
static Random rnd(301);
// Throw in occasional long delays to help find timing problems
extern void ProbDelay() {
    if(DELAYPROB > (rnd.Next() % 100))std::this_thread::sleep_for(DELAYTIME);  // give the compactor time to run
}
#endif

// compress a data block according to the options
// If there is an error, it just returns the input data
extern CompressionType CompressForVLog(const std::string& raw,
                    CompressionType type,
		    const CompressionInfo& compression_info,
                    std::string* compressed_output) {
  bool compressok;  // true if compression succeeded
  // Will return compressed block contents if (1) the compression method is
  // supported in this platform and (2) the compression rate is "good enough".
  switch (type) {
    case kSnappyCompression:
      compressok = Snappy_Compress(compression_info, raw.data(), raw.size(),
                          compressed_output);
      break;  // fall back to no compression.
    case kZlibCompression:
      compressok = Zlib_Compress(compression_info,kVLogCompressionVersionFormat,
              raw.data(), raw.size(), compressed_output);
      break;  // fall back to no compression.
    case kBZip2Compression:
      compressok = BZip2_Compress(compression_info,kVLogCompressionVersionFormat,
              raw.data(), raw.size(), compressed_output);
      break;  // fall back to no compression.
    case kLZ4Compression:
      compressok = LZ4_Compress(
              compression_info,kVLogCompressionVersionFormat,
              raw.data(), raw.size(), compressed_output); 
      break;  // fall back to no compression.
    case kLZ4HCCompression:
      compressok = LZ4HC_Compress(
              compression_info,kVLogCompressionVersionFormat,
              raw.data(), raw.size(), compressed_output);
      break;     // fall back to no compression.
    case kXpressCompression:
      compressok = XPRESS_Compress(raw.data(), raw.size(),
          compressed_output);
      break;
    case kZSTD:
    case kZSTDNotFinalCompression:
      compressok = ZSTD_Compress(compression_info, raw.data(), raw.size(),
                        compressed_output);
      break;     // fall back to no compression.
    default: compressok = false; break;  // Do not recognize this compression type
  }
  // Here if compressok is set, the data has been compressed into compressed_output.

  if(!compressok){
  // Compression method is not supported, or not good compression ratio, so just
  // fall back to uncompressed form.
    (*compressed_output).assign(raw);  // copy the input to the output
    return (CompressionType) kNoCompression;
  }
  return compression_info.type();
}


// given the running-sum of record-lengths rcdend, and the maximum filesize, break the input into chunks of approximately equal size
// result is given in filecumreccnts which is the list of record counts per file
// explicit result is the size of the largest allocation encountered
extern VLogRingRefFileOffset BreakRecordsIntoFiles(
  std::vector<size_t>& filecumreccnts,  // (result) running total of input records assigned to a file
  std::vector<VLogRingRefFileOffset>& rcdend,  // running total of input record lengths
  int64_t maxfsize,   // max size of an individual output file
    // the rest of the input arguments are used only if we are limiting the size of an output file based on grandparent size
  const std::vector<FileMetaData*> *grandparents,  // (optional) grandparent files that overlap with the key-range
  const std::vector<NoInitChar> *keys,  // (optional) the keys associated with the input records, run together
  const std::vector<size_t> *keylens, // (optional) the cumulative lengths of the keys
  const InternalKeyComparator *icmp,  // (optional) comparison function for this CF
  uint64_t maxcompsize  // (optional)  the maximum size of a compaction, including the file being created here and any grandchildren that overlap with it
  ) {
  VLogRingRefFileOffset maxfilesizecreated = 0;   // will accumulate the largest file size
  filecumreccnts.clear(); filecumreccnts.reserve(32); // clear the result area; reserve more space than we figure to need
  // split the input into file-sized pieces
  // Loop till we have processed all the inputs
  int64_t prevbytes = 0;  // total bytes written to previous files
  size_t grandparentx = 0;  // index of the next grandparent file.  We advance after we are sure the current file can no longer contribute
  for(int64_t rcdx = 0; rcdx<(int64_t)rcdend.size();++rcdx){   // rcdx is the index of the next input record to process
    int64_t firstrcdinfile = rcdx;  // remember the starting rcd# for this file
    // Calculate the output filesize, erring on the side of larger files.  In other words, we round down the number
    // of files needed, and get the target filesize from that.  For each calculation we use the number of bytes remaining
    // in the input, so that if owing to breakage a file gets too long, we will keep trying to prorate the remaining bytes
    // into the proper number of files.  We may end up reducing the number of files created if there is enough breakage.
    double bytesleft = (double)(rcdend.back()-prevbytes);  // #unprocessed bytes, which is total - (total processed up to previous record).
    // Because zero-length files are not allowed, we need to check to make sure there is data to put into a file
    // (consider the case of lengths 5 5 5 5 1000000 0 0 0 0  which we would like to split into two files of size 500005).  If there
    // is no data left, stop with the files we have - the zero-length records will add on to the end of the last file
    if(bytesleft==0.0)break;   // stop to avoid 0-length file
    // Round mostly down, but not so far down as to make the file too large (mostly needed for testcases that check 1 or 2 files).  We constrain the length to
    // between 1.0 and 1.25 times the nominal file size.  bytesleft/nfiles is in [1.00,1.25)*maxfile so nfiles is in [bytesleft/1.25*maxfile,bytesleft/maxfile).
    // Round the low value up, the high value down, and take the larger of the results
    double targetfilect = std::max(std::ceil(bytesleft/((double)maxfsize*1.25)),std::floor(bytesleft/((double)maxfsize)));  // number of files expected, rounded down
    int64_t targetfileend = prevbytes + (int64_t)(bytesleft/targetfilect);  // min endpoint for this file (min number of bytes plus number of bytes previously output)
    // Allocate records to this file until the minimum fileend is reached.  Use binary search in case there are a lot of small files (questionable decision, since
    // the files would have been added one by one)
    int64_t left = rcdx-1;  // init brackets for search
    // the loop variable rcdx will hold the result of the search, fulfilling its role as pointer to the end of the written block
    rcdx=rcdend.size()-1;
    while(rcdx!=(left+1)) {  // binary search
      // at top of loop rcdend[left]<targetfileend and rcdend[rcdx]>=targetfileend.  This remains invariant.  Note that left may be before the search region, or even -1
      // Loop terminates when rcdx-left=1; at that point rcdx points to the ending record, i. e. the first record that contains enough data
      // Calculate the middle record position, which is known not to equal an endpoint (since rcdx-left>1)
      size_t mid = left + ((rcdx-left)>>1);   // index of middle value
      // update one or the other input pointer
      if(rcdend[mid]<targetfileend)left=mid; else rcdx=mid;
    }
    // rcdend[rcdx] has the file length.  There is always at least one record per file.  If we are limiting the grandparent size, do that now
    if(grandparents!=nullptr && (*grandparents).size()) {
      // We are using the grandparents to limit the total size of this file PLUS its overlapping parents.
      // Because the number of grandparents averages the fanout, we will just scan forward through the grandparents, accumulating files
      // that overlap the current file.  The normal case is that we process grandparent files till we get past the last key in the output file;
      // in that case we output all the keys in the output file. 
      // get first key of putative output file
      size_t key0x = firstrcdinfile ? (*keylens)[firstrcdinfile-1] : 0;  // start of first internal key in rcd
      Slice key0((char *)(*keys).data()+key0x, (*keylens)[firstrcdinfile]-key0x);  // first key in new output file.
      // skip over grandparent files that are entirely before the current output file
      while(grandparentx<(*grandparents).size() && icmp->user_comparator()->Compare((*grandparents)[grandparentx]->largest.user_key(),ExtractUserKey(key0))<0)++grandparentx;  // skip if grandparent all before output
      // We have collected as many keys as we can, but we must also ensure that when this file is eventually compacted, it doesn't overlap so many grandparent files
      // that it creates too large a compaction.  We code this on the assumption that this limit is rarely triggered.
      // We go through the grandparents, accumulating the length of the grandparents that overlap the putative output record.  If all of them can be included in the
      // compaction along with the keys in the output record, we can keep all the keys. (normal case)
      // If adding the next grandparent would exceed the compaction size limit, we will have to cut back on keys.  There are two possibilities:
      // (1) we may cut output keys enough to make space for the grandparent to fit; (2) we may cut keys back enough so that the file no longer overlaps
      // this grandparent.  In case 2 we can immediately use the new output file; in case 1 we have to keep processing to make sure that the NEXT grandparent doesn't cause trouble.
      // initialize the total compaction size to the size of the output file
      // accumulate size of grandparents that overlap the putative output file.  We know the current grandparent file, if it exists, ends after the first key in the putative output
      size_t ratified_grandparent_size = 0;  // size of grandparents we know we can safely overlap
      size_t newgrandparentsize; // will hold next grandparent size
      for(;grandparentx<(*grandparents).size();ratified_grandparent_size+=newgrandparentsize, ++grandparentx) {
        // Here rcdx is the key that we hope will be the last in the output file
        // ratified_grandparent_size is the total size of all grandparents that we have processed & accepted
        size_t currentfilesize = rcdend[rcdx]-prevbytes;  // length of the records in the putative output
        size_t keynx = rcdx ? (*keylens)[rcdx-1] : 0;  // start of first internal key in rcd
        Slice keyn((char *)(*keys).data()+keynx, (*keylens)[rcdx]-keynx);  // last key in new output file.
        Slice nextstartkey = (*grandparents)[grandparentx]->smallest.user_key();  //  starting key of next grandparent file.  Slice OK because nothing to be freed
        // if the next grandparent starts after the last key in the putative output, we have processed all overlaps and can quit
        if(icmp->user_comparator()->Compare(nextstartkey,ExtractUserKey(keyn))>0)break;  // stop if grandparent all after output.  This is the normal exit
        // here the files overlap.  See how big this new grandparent is.  Because we buffer all the kvs for a compaction,
        // it is proper for us to limit the size AFTER decompression; so we use the total key+value size
        newgrandparentsize = ((*grandparents)[grandparentx]->raw_key_size && (*grandparents)[grandparentx]->raw_value_size) ?  // if size is 0, it means we couldn't initialize the true size
            (*grandparents)[grandparentx]->raw_key_size+(*grandparents)[grandparentx]->raw_value_size    // if neither 0, use initialized size
            : (uint64_t)(1.2*maxfsize);   // if size is 0, assume a biggish file.  1.2 is a handwave
        // if the new grandparent doesn't make the compaction too big, continue looking at the next one
        if(currentfilesize+ratified_grandparent_size+newgrandparentsize <= maxcompsize)continue;
        // Here the compaction has become too big.  We will NOT be able to include the new grandparent and all the keys.  That means we must discard keys from the end of the putative
        // output until it no longer overlaps with the file we couldn't include, or the keys are short enough that the new file will fit.  We will use a binary search to do this, because there may be a lot of keys.
        // As above we start leftvalue at its lowest possible value, rcdx at its highest possible value (which equals its starting value)
        int64_t leftvalue = firstrcdinfile-1;  // init brackets for search.  put left side before the search region to allow rcdx to get as low as it needs
        while(rcdx!=(leftvalue+1)) {  // binary search.
          // at top of loop (*keys)[leftvalue]<target key and (*keys)[rcdx]>=target key.  This remains invariant.  Note that leftvalue may be before the search region, or even -1
          // Loop terminates when rcdx-leftvalue=1; at that point rcdx points to the ending+1 record, i. e. the first record that contains a key too high
          // Calculate the middle record position, which is known not to equal an endpoint (since rcdx-leftvalue>1)
          size_t mid = leftvalue + ((rcdx-leftvalue)>>1);   // index of middle value
          // update one or the other input pointer.  First, collect the information.  The key for mid:
          size_t keymidx = mid ? (*keylens)[mid-1] : 0;  // start of middle internal key
          Slice keymid((char *)(*keys).data()+keymidx, (*keylens)[mid]-keymidx);  // last key in new output file.
          // The size of the keys from the file, if mid becomes the last record:
          currentfilesize = rcdend[mid]-prevbytes;  // length of the records in the putative output
          bool midisbeforegrandparent = icmp->user_comparator()->Compare(nextstartkey,ExtractUserKey(keymid))>0;  // do we have to include the grandparent if we use mid?
          // See if the filesize of the file ending at mid is acceptable
          if(currentfilesize+ratified_grandparent_size+(midisbeforegrandparent?0:newgrandparentsize) <= maxcompsize)leftvalue=mid; else rcdx=mid;
        }
        // We have now backed leftvalue up to the point where the keys up to leftvalue, plus the grandparents up to the current one, will fit in a compaction.
        // If the key at leftvalue is before the current grandparent, we are finished.  But if not, we have to continue the loop to look at the NEXT grandparent,
        // to make sure it either fits or doesn't overlap.  We set rcdx to the new putative ending index.
        // One special case: if NO kv will fit (maybe the kvs are huge), we have to take just 1 key and move on.
        if(leftvalue<firstrcdinfile){rcdx=firstrcdinfile; break;}  // special case: 1-kv file
        rcdx = leftvalue;   // otherwise, leftvalue is the highest key that fits: keep it
        // fetch the ending key in the file
        keynx = rcdx ? (*keylens)[rcdx-1] : 0;  // start of first internal key in rcd
        keyn = Slice((char *)(*keys).data()+keynx, (*keylens)[rcdx]-keynx);  // last key in new output file.
        if(icmp->user_comparator()->Compare(nextstartkey,ExtractUserKey(keyn))>0)break;  // case 2: no overlap with grandparent, we're done.  Leave grandparentx pointing to the current grandparent for next file
        // otherwise, the putative file ending at rcdx overlaps the current grandparent, but that grandparent will fit into the compaction.  Ratify the
        // current grandparent and keep looking, make sure that the next grandparent doesn't cause trouble
      }
    }
    // rcdx is the index of the last record in the new file
    filecumreccnts.push_back(rcdx+1);  // push cumulative # records in output file
    maxfilesizecreated=std::max(maxfilesizecreated,rcdend[rcdx]-prevbytes);   // keep track of the largest file created
    prevbytes = rcdend[rcdx];  // update total # bytes written
  }
  return maxfilesizecreated;
}

// MergeContext support, which has no home file
// Support for passing the VLog address via the MergeContext
MergeContext::MergeContext(ColumnFamilyData *cfd) : vlog(cfd->vlog()) {}
std::shared_ptr<VLog> MergeContext::GetVlog() { return vlog; }
void MergeContext::SetCfd(ColumnFamilyData *cfd) { vlog=cfd->vlog(); }

// Convert a filename, which is known to be a valid vlg filename, to the ring and filenumber in this VLog
static ParsedFnameRing VlogFnametoRingFname(std::string pathfname) {
    // Isolate the filename from the path as required by the subroutine
    std::string fname = pathfname.substr(pathfname.find_last_of("/\\")+1);  // get the part AFTER the last '/' or '\'; if not found, substr(0) which is the whole name

    // Extract the file number (which includes the ring) from the filename
    uint64_t number;  // return value, giving file number
    FileType type;  // return value, giving type of file
    ParseFileName(fname, &number, &type);  // get file number (can't fail)

    // Parse the file number into file number/ring
    return ParsedFnameRing(number);
}

// merge edits in sec into *this
void VLogRingRestartInfo::Coalesce(const VLogRingRestartInfo& sec,  // the stats to merge in
  bool outputdeletion  // true if a deletion in the input should be passed through to the result.  For final status we elide the deletion
)
{
  // merge the byte counts
  size += sec.size;  frag += sec.frag; fragfrac = (double)frag/std::max((double)size,1.0);   // avoid ZDIV
  // set up indexes to use to scan through inputs
  size_t thisx = 0, secx = 0;  // scan pointer within inputs
  // create output area
  std::vector<VLogRingRefFileno> outarea;  // we will build result here
  // set the delete-up-to point, taken from the merged inputs
  VLogRingRefFileno delto = 0;   // valid file numbers start at 1.  We delete every file up to and including delto
  if(valid_files.size()!=0 && valid_files[0]==0){thisx = 2; delto = valid_files[1];}
  if(sec.valid_files.size()!=0 && sec.valid_files[0]==0){ secx = 2; if(sec.valid_files[1]>delto)delto = sec.valid_files[1]; }
  // if there is a delete-to point, and our output can accept it, put it out
  if(outputdeletion && delto!=0){outarea.push_back(0); outarea.push_back(delto);}   // leading (0,maxdel) means delete all <= maxdel
  // loop till we have output all the intervals
  while(1) {
    // initialize the interval we will build: start with the smaller input.  If there are no inputs, we are through
    VLogRingRefFileno intlo = high_value, inthi;
    if(thisx<valid_files.size())intlo = valid_files[thisx];
    if(secx<sec.valid_files.size()){if(sec.valid_files[secx]<intlo)intlo = sec.valid_files[secx];}
    if(intlo==high_value)break;  // if there are no more intervals, stop looking
    inthi = intlo;   // the interval starts small, but it will be extended
    // keep processing inputs until we can't add on to the current interval
    while(1) {
      // We can consume an input interval if its front overlaps or extends the end of the current interval, i. e. if it is <= inthi+1.
      // If we accept an input interval, we adjust the end of the current interval up.  The beginning of the current interval is fixed
      if(thisx<valid_files.size() && valid_files[thisx]<=(inthi+1)){if(valid_files[thisx+1]>inthi)inthi = valid_files[thisx+1]; thisx += 2; continue;}
      if(secx<sec.valid_files.size() && sec.valid_files[secx]<=(inthi+1)){if(sec.valid_files[secx+1]>inthi)inthi = sec.valid_files[secx+1]; secx += 2; continue;}
      break;  // if nothing extended the interval, quit
    };
    // we have an interval.  Write it out, after removing any parts that are to be deleted
    if(inthi>delto) {   // if the upper end doesn't make it past the deletion point, write nothing
      if(intlo<(delto+1))intlo = delto+1;   // clamp the low end to above the deletion point
      outarea.push_back(intlo); outarea.push_back(inthi);  // write out the interval
    }
  }
  // we have written out all the intervals.  Replace the input intervals and we're done
  valid_files.swap(outarea);
#ifndef NDEBUG
  if(!outputdeletion){
    TEST_SYNC_POINT_CALLBACK("VLogRingRestartInfo::Coalesce",
                             this);
  }
#endif
}

// fold the VLog edits in sec into pri.  Each is vector with one set of stats per ring
void Coalesce(std::vector<VLogRingRestartInfo>& pri,  // the left input and result
  const std::vector<VLogRingRestartInfo>& sec,  // right input
  bool outputdeletion  // true if a deletion in the input should be passed through to the result.  For final status we elide the deletion
)
{
  // if the right argument has no rings, leave the left unchanged
  if(sec.size()==0)return;
  // if the left argument has no rings, resize it to the size of the right
  if(pri.size()==0)pri.resize(sec.size());
  assert(pri.size()==sec.size());  // rings should have same size
  // process each ring
  for(uint32_t i = 0; i<pri.size(); ++i)pri[i].Coalesce(sec[i],outputdeletion);
}


//
//
// ******************************************** VlogRing ***********************************************
// VLogRing keeps track of VLogRingFiles for a specified set of levels in the database.  Files are entered into
// the ring during compaction.  The file persists until there are no more SSTs that refer to it.  During compaction,
// the oldest values are picked up & copied to new files, which removes references to the oldest files.
//
// We use the VLogRing to retrieve the value of an indirect reference, given the ring number/file number/offset/length
// of the reference.  We also use the VLogRing to see which SSTs have references to the oldest files.
//

// A VLogRing is a set of sequentially-numbered files, with a common prefix and extension .vlg, that contain
// values for a column family.  Each value is pointed to by an SST, which uses a VLogRingRef for the purpose.
// During compaction, bursts of values are written to the VLogRing, lumped into files of approximately equal size.
//
// Each SST remembers the oldest VLogRingRef that it contains.  Whenever an SST is destroyed, we check to
// see if there are VLogRing files that are no longer referred to by any SST, and delete any such that are found.
//
// Each column family has (possibly many) VLogRing.  VLogRingRef entries in an SST implicitly refer to the VLog of the column family.
// Constructor.  Find all the VLogRing files and open them.  Create the VLogRingQueue for the files.
// Delete files that are not marked as valid in the manifest
VLogRing::VLogRing(
  int ringno,   // ring number of this ring within the CF
  ColumnFamilyData *cfd,  // info for the CF for this ring
  std::vector<std::string> filenames,  // the filenames that might be vlog files for this ring
  std::vector<VLogRingRefFileLen> filesizes,   // corresponding file sizes
  const ImmutableDBOptions *immdbopts,   // The current Env
  const EnvOptions& file_options  // options to use for all VLog files
)   : 
    ringno_(ringno),
    cfd_(cfd),
    immdbopts_(immdbopts),
    envopts_(file_options),
// obsolete     deletion_deadband(1000),   // init to big value
    initialstatus(Status{})
{
#if DEBLEVEL&1
printf("VLogRing cfd_=%p\n",cfd_);
#endif
  // Copy the direct-IO status from the CF options to the ENV options for this ring
  envopts_.use_direct_reads = cfd_->GetLatestCFOptions().vlog_direct_IO;
  envopts_.use_direct_writes = cfd_->GetLatestCFOptions().vlog_direct_IO;
  // The CF has the ring stats, including the current earliest and latest files.  Extract them.
  // The CF is guaranteed to have stats for the ring, but the file info may be empty
  VLogRingRefFileno earliest_ref, latest_ref;  // first and last valid file numbers
  if(cfd_->vloginfo()[ringno_].valid_files.size()==0){
    // No files in the manifest.  set file numbers back to beginning.
    earliest_ref = 1; latest_ref = 0;
  } else {
    // There are files.  Get first and last
    earliest_ref = cfd_->vloginfo()[ringno_].valid_files[0];
    latest_ref = cfd_->vloginfo()[ringno_].valid_files.back();
    assert(earliest_ref!=0);  // the CF data should never start with a deletion
  }

  // Allocate the rings for files and references.
  VLogRingRefFileno power2 = RingSizeNeeded(earliest_ref,latest_ref);
  while(power2-->0)fd_ring[0].emplace_back();  // mustn't use resize() - if requires copy semantics, incompatible with unique_ptr

  // Log out the files that we expect in the database
  std::string allfnos; allfnos.reserve(8 * cfd_->vloginfo()[ringno_].valid_files.size());
  for(size_t i=0;  i<cfd_->vloginfo()[ringno_].valid_files.size();i+=2){
    allfnos.append(" "); allfnos.append(std::to_string(cfd_->vloginfo()[ringno_].valid_files[i]));  // write start value
    allfnos.append("-"); allfnos.append(std::to_string(cfd_->vloginfo()[ringno_].valid_files[i+1]));  // write end value
  }
  ROCKS_LOG_INFO(immdbopts_->info_log,"Ring #%d expected initial files:%s\n",ringno_,allfnos.c_str());
  // For each file in this ring, open it (if its number is in the manifest) or delete it (if not)
  for(uint32_t i=0;i<filenames.size();++i) {
    ParsedFnameRing fnring = VlogFnametoRingFname(filenames[i]);

    if(fnring.ringno_==ringno) {  // If this file is for our ring...
      if(cfd_->vloginfo()[ringno_].ContainsFileno(fnring.fileno_)) {
        // the file is valid according to the manifest.  Open it
        std::unique_ptr<RandomAccessFile> fileptr;  // pointer to the opened file
        Status s = immdbopts_->env->NewRandomAccessFile(filenames[i], &fileptr,envopts_);  // open the file if it exists
          // move the file reference into the ring, and publish it to all threads.  These files start as deletable since we don't try to delete until references have been installed
        fd_ring[0][Ringx(fd_ring[0],fnring.fileno_)]=std::move(VLogRingFile(fileptr,filesizes[i],0));
#if DEBLEVEL&2
printf("Opening file %s\n",filenames[i].c_str());
#endif
        if(!s.ok()) {
          fd_ring[0][Ringx(fd_ring[0],fnring.fileno_)].filepointer=nullptr;  // if error, ensure pointer is null
          ROCKS_LOG_ERROR(immdbopts_->info_log,
              "Error opening VLog file %s: %s",filenames[i].c_str(),s.getState());
        }

        // if error opening file, we can't do anything useful, so leave file unopened, which will give an error if a value in it is referenced
      } else {
#if DEBLEVEL&2
printf("Deleting file %s\n",filenames[i].c_str());
#endif
        // The file is not referenced.  We must have crashed while deleting it, or before it was referenced.  Delete it now
        if(immdbopts_->env->DeleteFile(filenames[i]).ok()){ // scaf should be envopts_
          ROCKS_LOG_WARN(immdbopts_->info_log,
                        "Unreferenced VLog file %s deleted",filenames[i].c_str());
        }else{
          ROCKS_LOG_WARN(immdbopts_->info_log,
                        "Error deleting unreferenced VLog file %s",filenames[i].c_str());
        }
      }
    }
  }

  // Set up the pointers for the ring, indicating validity
  // The conversion from file# to slot is fixed given the size of the ring, so the first
  // file does not necessarily go into position 0
  // The tail pointers point to the oldest ref
  atomics.fd_ring_tail_fileno.store(earliest_ref,std::memory_order_release);  // oldest file in the ring
  // The head pointers point to the last entry.  But, we always start by
  // creating a new file.  The oldest file may have some junk at the end, which will hang around until
  // the ring recycles.
  atomics.fd_ring_head_fileno.store(latest_ref,std::memory_order_release);
  // Init ring usecount to 0, meaning 'not in use', and set write lock to 'available'
  atomics.usecount.store(0,std::memory_order_release);
  atomics.writelock.store(0,std::memory_order_release);
  // Since nothing starts out relying on the previous buffer, we can safely clear it at any time
  atomics.fd_ring_prevbuffer_clear_fileno.store(0,std::memory_order_release);

  // Start us up using array 0
  atomics.currentarrayx.store(0,std::memory_order_release);
}

// We call here after the initial SSTs for the ring have been installed.  We verify that no referenced files are missing.
// If there were snapshots, it is possible that when a snapshot closed, a file it was holding got deleted, but that file may remain
// in the file-list for the CF until a later deletion happens.  If the database is closed at that point, the early file will
// be missing but unreferenced.  So, we look here for files that are missing, but we ignore any that are earlier than the first reference
Status VLogRing::VerifyFilesPresent() {
  Status s;
  // Audit to make sure that all files that were supposed to be opened were actually opened
  size_t nmissingfiles=0, firstmissingfile=0, lastmissingfile=0;  // keep track of what's lacking
  int fileisreqd = 0;  // we don't start noticing errors until there is a reference to the file.  The files must be in order coming in from vloginfo
  for(size_t i=0; i<cfd_->vloginfo()[ringno_].valid_files.size();i+=2){ // for each start/end pair
    for(size_t j=cfd_->vloginfo()[ringno_].valid_files[i];j<=cfd_->vloginfo()[ringno_].valid_files[i+1];++j){  // for each file in the interval
      if(fd_ring[0][Ringx(fd_ring[0],j)].filepointer!=nullptr)fileisreqd = 1;  // once a file is found, later files should be also
      else{  // if file was not opened
        if(fd_ring[0][Ringx(fd_ring[0],j)].queue!=nullptr)fileisreqd = 1;  // once a file is referenced, it and later files are required
        if(fileisreqd){   // file is missing, but we need it.  That's going to be fatal.
          lastmissingfile=j;
          if(nmissingfiles==0)firstmissingfile=j;   // remember it, if it's the first
          ++nmissingfiles;   // and keep track of how many there were
        }else{
          // File was missing, but before any references.  We'll just step over it.  When the next deletion happens, the file numbers will be repaired
          atomics.fd_ring_tail_fileno.store(j+1,std::memory_order_release);  // skip over  the empty file, so we don't try to delete it
          // It is possible that this will empty the ring, if the new tail is in front of the head.  That's OK.  We DO NOT reset the tail/head to 1/0, even
          // though that is the normal 'empty' state, in case there are other undeleted files about that we might later collide with
        }
      }
    }
  }
  // Message if there were unopened files
  if(nmissingfiles){
    ROCKS_LOG_ERROR(immdbopts_->info_log,
         "%" PRIu64 " Not all VLog files could be opened.  The first one missing was #%" PRIu64 ", last was #%" PRIu64 "\n",nmissingfiles,firstmissingfile,lastmissingfile);
    s = Status::Corruption("VLog file cannot be opened");
  }

  return s;

}

// Delete the file.  Close it as random-access, then delete it
void VLogRingFileDeletion::DeleteFile(VLogRing& v,   // the current ring
  const ImmutableDBOptions *immdbopts,   // The current Env
  EnvOptions& /*file_options*/  // options to use for all VLog files
) {
  filepointer = nullptr;  // This closes the file if it was open
  std::string filename = VLogFileName(v.cfd_->ioptions()->cf_paths,
    VLogRingRef(v.ringno_,(int)fileno).FileNumber(), (uint32_t)v.cfd_->ioptions()->cf_paths.size()-1, v.cfd_->GetName());
#if DEBLEVEL&2
printf("Deleting file: %s\n",filename.c_str());
#endif
  if(!immdbopts->env->DeleteFile(filename).ok()){
    ROCKS_LOG_WARN(immdbopts->info_log,
                  "Error trying to delete VLog file %s",filename.c_str());
  }
}

// See how many files need to be deleted.  Result is a vector of them, with the ring entries for the old files cleared.
// The tail pointer is moved over the deleted files.
// This whole routine must run under the lock for the ring.
// On entry, we assume that we have just come back from reserving space for the deleted files, and that the tailpointer must be checked again
void VLogRing::CollectDeletions(
  VLogRingRefFileno tailfile,  // up-to-date tail file number.  There must be no files before it.
  VLogRingRefFileno headfile,  // up-to-date head file number
  std::vector<VLogRingFileDeletion>& deleted_files  // result: the files that are ready to be deleted.  The capacity must have been set and will not be exceeded in this routine
) {
  // If the tailpointer has moved since we decided to try to delete, that means that some other thread slipped in when we released the lock,
  // and has freed some files.  We don't need to free any, then
  if(tailfile!=atomics.fd_ring_tail_fileno.load(std::memory_order_acquire)) return;
#if DELAYPROB
ProbDelay();
#endif 
  // Fetch the up-to-date value for the current ring
  uint64_t arrayx = atomics.currentarrayx.load(std::memory_order_acquire);
#if DELAYPROB
ProbDelay();
#endif 
  // In case the ring was resized, recalculate the slot number to use. 
  size_t slotx = Ringx(fd_ring[arrayx],tailfile);   // point to first file to consider deleting
  if(fd_ring[arrayx][slotx].refcount_deletion)return;  // return fast if the first file does not need to be deleted
  while(1) {  // loop till tailfile is right
    if(deleted_files.size()==deleted_files.capacity())break;  // if we have no space for another deletion, stop
    deleted_files.emplace_back(tailfile,fd_ring[arrayx][slotx]);  // mark file for deletion.  This also resets the ring slot to empty
    if(++slotx==fd_ring[arrayx].size())slotx=0;  // Step to next slot; don't use Ringx() lest it perform a slow divide
    ++tailfile;  // Advance file number to match slot pointer
    if(tailfile+deletion_deadband>=headfile)break;   // Tail must stop at one past head, and that only if ring is empty; but we enforce the deadband
    if(fd_ring[arrayx][slotx].refcount_deletion)break;  // if the current slot has references, it will become the tail
  }
  // If we delete past the queued pointer, move the queued pointer
  if(tailfile>atomics.fd_ring_queued_fileno.load())atomics.fd_ring_queued_fileno.store(tailfile,std::memory_order_release);
#if DELAYPROB
ProbDelay();
#endif 
  // Now tailfile is the lowest file# that has a nonempty queue, unless there are no files, in which case it is one past the head
  // Save this value in the ring.  We do this AFTER we have made the changes to the ring, so that Get() will see them in that order
  atomics.fd_ring_tail_fileno.store(tailfile,std::memory_order_release);
#if DELAYPROB
ProbDelay();
#endif 
#if DEBLEVEL&8
printf("Tail pointer set; pointers=%lld %lld\n",atomics.fd_ring_tail_fileno.load(),atomics.fd_ring_head_fileno.load());
#endif

}

// Delete the files collected by CollectDeletions.  We must have released the lock
void VLogRing::ApplyDeletions(std::vector<VLogRingFileDeletion>& deleted_files) {
  std::string dfstring; dfstring.reserve(8 * deleted_files.size());
  for(size_t i = 0;i<deleted_files.size();++i) {
#if DEBLEVEL&1
  printf("Deleting: %llx\n",deleted_files[i].fileno);
#endif
    dfstring.append(" "); dfstring.append(std::to_string(deleted_files[i].fileno)); 
    deleted_files[i].DeleteFile(*this,immdbopts_,envopts_);
  }
  // Log deletion
  if( deleted_files.size()){
    ROCKS_LOG_INFO(
        immdbopts_->info_log, "Deleting %" PRIu64 " VLog files: %s\n",
        deleted_files.size(), dfstring.c_str());
  }
}



// Resize the ring if necessary, updating currentarrayx if it is resized
// If the ring is resized, or if we found that no change was needed, return the new value of currentarrayx
//   (NOTE that in either case tailfile and headfile are NOT modified)
// If someone else took the lock when we laid it down, return 2 to indicate that fact (the resize must be retried)
// Requires holding the ring at start, and guarantees holding it at the end (though it may have been released)
uint64_t VLogRing::ResizeRingIfNecessary(VLogRingRefFileno tailfile, VLogRingRefFileno headfile, uint64_t currentarray,  // current ring pointers
  size_t numaddedfiles  // number of files we plan to add
){
  // If our new files will fit in the current ring, return fast
  if((headfile+numaddedfiles)<(tailfile+fd_ring[currentarray].size()))return currentarray;
  // We have to resize the ring.  See how big the new ring needs to be (a power-of-2 multiple of the current ring)
  VLogRingRefFileno allosize = RingSizeNeeded(tailfile,headfile+numaddedfiles);  // # slots to allocate all active files
  // Allocate a vector for the new ring.  We have to relinquish the lock while we do this lest the request for memory
  // block while we are holding the ring.  Therefore we cannot simply change the reservation in the existing previous ring,
  // because another thread may be trying to resize also, possibly to a different size.
  ReleaseLock();
    std::vector<VLogRingFile> newring;  // allocate ring
    newring.reserve(allosize);   // reserve all the space we need, so it will never be resized
    for(size_t i=0;i<allosize;++i)newring.emplace_back();  // fill ring with harmless empty entries.  Do this outside of lock, to reduce lock time
#if DELAYPROB
ProbDelay();
#endif 
  AcquireLock();
  // If the ring pointers changed while we relinquished the ring, return with a value that will cause us to retry
  if(tailfile!=atomics.fd_ring_tail_fileno.load(std::memory_order_acquire) || headfile!=atomics.fd_ring_head_fileno.load(std::memory_order_acquire)
      || currentarray!=atomics.currentarrayx.load(std::memory_order_acquire))return 2;  // 2 means 'retry'
  // We are back in control.
  // Copy the old ring's data to the new ring.  This violates the 'no-copy' rule for unique_ptrs, as we temporarily
  // create a second copy of the data.  We will release the original before we exit.
  // NO EXCEPTIONS STARTING HERE
  for(size_t i=tailfile;i<=headfile;++i){  // copy the entire valid part of the ring from the old positions to the new ones
    newring[Ringx(newring,i)]=std::move(VLogRingFile(fd_ring[currentarray][Ringx(fd_ring[currentarray],i)],0));
  }
  // swap the new ring into the ping-pong buffer.  The old one, which is probably but not necessarily of 0 length,
  // will be destroyed when this function exits.  If it is not empty it will contain all nullptrs, because it is always a 'previous buffer' that
  // was cleared after it was swapped out
  uint64_t newcurrent = 1-currentarray;  // index of the new current buffer
  newring.swap(fd_ring[newcurrent]);
  // Set the point at which we deem we can reclaim the storage for the previous array.
  // We use a value of half a ring from where it is now; this will be rounded up since we sample only occasionally
  atomics.fd_ring_prevbuffer_clear_fileno.store(headfile+(allosize>>1),std::memory_order_release);
#if DELAYPROB
ProbDelay();
#endif 
  // switch buffers: set the new value of currentarrayx, thereby publishing the new ring to the system
  atomics.currentarrayx.store(newcurrent,std::memory_order_release);
#if DELAYPROB
ProbDelay();
#endif 
  // Go through the newly-previous ring, releasing all the pointers.  This restores the single ownership of the underlying resource in
  // the new ring
  for(size_t i=0;i<fd_ring[currentarray].size();++i)fd_ring[currentarray][i].filepointer.release();
  // EXCEPTIONS OK NOW
  size_t newsize = fd_ring[newcurrent].size();  // remember what we are going to print before we release the lock
  ReleaseLock();
    ROCKS_LOG_INFO(immdbopts_->info_log,"VLogRing buffer has been resized, new buffer is %lu, length=%" PRIu64 ", tailfile=%" PRIu64 ", headfile=%" PRIu64 "\n",newcurrent,newsize,tailfile,headfile);
  AcquireLock();

  return newcurrent;  // let operation proceed
  // newring, which now contains the obsolete third-from-last buffer, will be destroyed
}


// Write accumulated bytes to the VLogRing.  First allocate the bytes to files, being
// careful not to split a record, then write them all out.  Create new files as needed, and leave them
// open.
// The result is a VLogRingRef for the first (of possibly several sequential) file, and a vector indicating the
// number of bytes written to each file
// We housekeep the end-of-VLogRing information
// We use release-acquire ordering for the VLogRing file and offset to avoid needing a Mutex in the reader
// If the circular buffer gets full we have to relocate it, so we use release-acquire
void VLogRing::VLogRingWrite(
std::shared_ptr<VLog> current_vlog,  // The vlog this ring belongs to
std::vector<NoInitChar>& bytes,   // The bytes to be written, jammed together
std::vector<VLogRingRefFileOffset>& rcdend,  // The running length on disk of all records up to and including this one
std::vector<char>& valueclass,  // record type of all records - the ones with diskdata and the others too
int64_t maxfilesize,   // recommended maximum VLogFile size - may be exceeded up to 25%
int job_id,  // job id for logmsgs
VLogRingRef& firstdataref,   // result: reference to the first value written
std::vector<VLogRingRefFileOffset>& fileendoffsets,   // result: ending offset of the data written to each file.  The file numbers written are sequential
          // following the one in firstdataref.  The starting offset in the first file is in firstdataref; it is 0 for the others
std::vector<Status>& resultstatus,   // result: place to save error status.  For any file that got an error in writing or reopening,
          // we add the error status to resultstatus and change the sign of the file's entry in fileendoffsets.  (no entry in fileendoffsets
          // can be 0).
VLogRingRefFileOffset& initfrag  // result: total fragmentation in the created files (bytes at the end that don't hold values)
)
{
  // In this implementation, we write only complete files, rather than adding on to the last one
  // written by the previous call.  That way we don't have to worry about who is going to open the last file, and
  // whether it gets opened in time for the next batch to be added on.

  // We use spin locks rather than a mutex because we don't keep the lock for long and we never do anything
  // that might block while we hold the lock.  We use CAS to acquire the spinlock, and we code with the assumption that the spinlock
  // is always available, but takes ~50 cycles to complete the read

  resultstatus.clear();  // init no errors returned
  fileendoffsets.clear();  // init no files written
  initfrag = 0;  // init no bytes wasted at end of files
  // If there is nothing to write, abort early.  We must, because 0-length files are not allowed when memory-mapping is turned on
  // This also avoids errors if there are no references
  if(!bytes.size()){
    firstdataref.FillVLogRingRef(0,0,0,0);  // We don't need a starting ref since there are no records, but garbage interferes with our auditing
    return;   // fast exit if no data
  }

  std::vector<size_t> filecumreccnts;  // this will hold the # records in each file

  VLogRingRefFileOffset maxallosize = BreakRecordsIntoFiles(filecumreccnts, rcdend, maxfilesize,   nullptr,nullptr,nullptr,nullptr,0);  // calculate filecumreccnts.  SST grandparent info not used
    // remember the size of the largest allocation
  fileendoffsets.reserve(filecumreccnts.size());  //clear output area and give it the correct size

  // Log start of writing 
  ROCKS_LOG_INFO(
      current_vlog->immdbopts_->info_log, "[%s] [JOB %d] writing %" PRIu64 " VLog files, %" PRIu64 " bytes, max filesize=%" PRIu64,
      current_vlog->cfd_->GetName().c_str(), job_id,
      filecumreccnts.size(), rcdend.size()?rcdend.back():0, maxfilesize);


  std::vector<VLogRingFileDeletion> deleted_files;  // files put here will be deleted after we release the lock
  AcquireLock();
    VLogRingRefFileno headfile, tailfile; uint64_t currentarray;

    // Check for resizing the ring.  When we come out of this loop the value of headfile, tailfile, currentarray will
    // be up to date, and we will still have the lock (though it may have been given up for a while)
    do{
      tailfile = atomics.fd_ring_tail_fileno.load(std::memory_order_acquire);
#if DELAYPROB
ProbDelay();
#endif 
      headfile = atomics.fd_ring_head_fileno.load(std::memory_order_acquire);
#if DELAYPROB
ProbDelay();
#endif 
      currentarray = atomics.currentarrayx.load(std::memory_order_acquire);
#if DELAYPROB
ProbDelay();
#endif 
    }while(2==(currentarray = ResizeRingIfNecessary(tailfile,headfile,currentarray,filecumreccnts.size())));  // return of 2 means 'retry required', otherwise new currentarray

    // Allocate file#s for this write.
    VLogRingRefFileno fileno_for_writing = headfile+1;

    // Mark the first slot as nondeletable.  This will prevent anyone from looking at it until it has had references installed.  The file pointer is invalid now so we don't
    // want anyone to try to delete the file prematurely.  This will protect the entire block.
    fd_ring[currentarray][Ringx(fd_ring[currentarray],fileno_for_writing)].refcount_deletion=-1;  // nondeletable indicator
    fd_ring[currentarray][Ringx(fd_ring[currentarray],fileno_for_writing)].refcount_manifest=-1;  // nondeletable indicator
  
    // Move the head pointer in the ring.  This reserves the files we are skipping over for us to use.  Does not have to be atomic with the read, since we have acquired writelock
    atomics.fd_ring_head_fileno.store(headfile+filecumreccnts.size(),std::memory_order_release);
#if DELAYPROB
ProbDelay();
#endif 
    // See if the tail pointer was being held up, for whatever reason
    if(tailfile+deletion_deadband<headfile && fd_ring[currentarray][Ringx(fd_ring[currentarray],tailfile)].refcount_deletion==0) {  // if tail>=head, the ring is empty & refcounts are invalid
      // The file at the tail pointer was deletable, which means the tail pointer was being held up either by proximity to the headpointer or
      // because a deletion operation hit its size limit.  We need to delete files, starting at the tailpointer.
      // This is a very rare case but we have to handle it because otherwise the tail pointer could get stuck
      //
      // We need to make sure we don't perform any large memory allocations that might block while we are holding a lock on the ring, so we reserve space for the
      // deletions - while NOT holding the lock - and then count the number of deletions
      ReleaseLock(); deleted_files.reserve(max_simultaneous_deletions); AcquireLock(); // reserve space to save deletions, outside of lock
      // Go get list of files to delete, updating the tail pointer if there are any.  CollectDeletions will reestablish tailptr, arrayx, slotx
      CollectDeletions(tailfile,headfile+filecumreccnts.size(),deleted_files);  // find deletions
    }

  ReleaseLock();

#if DEBLEVEL&8
printf("Head pointer set; pointers=%lld %lld\n",atomics.fd_ring_tail_fileno.load(),atomics.fd_ring_head_fileno.load());
#endif

  // We had to release the lock before we actually deleted the files, if any.  The files to delete are in deleted_files.  Delete them now
  ApplyDeletions(deleted_files);

  // Now create the output files.

  // We do not hold a lock during file I/O, so we must save anything that we will need to install into the rings
  std::vector<std::string> pathnames;   // the names of the files we create
  std::vector<std::unique_ptr<RandomAccessFile>> filepointers;  // pointers to the files, as random-access

// obsolete  size_t startofnextfile = 0;  // starting position of the next file.  Starts at beginning, advances by file-length

#if DEBLEVEL&2
printf("Writing %" PRIu64 " sequential files, %" PRIu64 " values, %" PRIu64 " bytes\n",filecumreccnts.size(),rcdend.size(), bytes.size());
#endif

  // Loop for each file: create it, reopen as random-access

  // filecumreccnts contains the cumulative record-counts for the files, from which we can recover the number of records in each file.
  // The data for the records is in diskdata, but it is a mixture of compressed data for new values and references to remapped values.
  // We traverse the records, copying them to filedata, reading from disk to resolve references.  We use the valueclass vector to
  // tell which records require reading.  Many of the valueclass entries are for other key types; we skip them.

  // In case we are using direct I/O, we align the file buffer to a sector boundary.  It's not worth checking; even if it were, it's inconvenient, because
  // the routines that tell you whether direct I/O is active require a file to run against.  So we just always align, using our own conservative estimate of the required alignment
  std::vector<NoInitChar> filebuffer(((maxallosize-1)|(directioalign-1))+directioalign);  // this is where we build the records to disk, big enough to round up to sector size and sector boundary
  size_t runningvaluex=0;   // index of next valueclass entry to examine
  size_t runningbytesx=0;   // index of next byte of bytes to be used
  size_t runningrcdx=0;  // index of next rcdend entry to be used
  int64_t filebufferalignoffset = (directioalign-1)&-(int64_t)filebuffer.data();  // offset to sector-aligned portion of filebuffer

  std::vector<VLogRingRefFileOffset> fileendoffsetspadded; fileendoffsetspadded.reserve(filecumreccnts.size());
  for(uint32_t i=0;i<filecumreccnts.size();++i) {   // Loop to create each file
    Status iostatus;  // where we save status from the file I/O
    int remappingerror = 0;  // set if we get an error remapping a value
// obsolete    size_t lenofthisfile = rcdend[filecumreccnts[i]-1] - startofnextfile;  // total len up through new file, minus starting position

    // Create filename for the file to write
// scaf can't we get by with just one pathname?
    pathnames.push_back(VLogFileName(cfd_->ioptions()->cf_paths,
      VLogRingRef(ringno_,(int)fileno_for_writing+i).FileNumber(), (uint32_t)cfd_->ioptions()->cf_paths.size()-1, cfd_->GetName()));

    // Buffer up the data for the file.  We copy each file's data to the output area; if the data is being moved from the
    // VLog for defrag, read it from the VLog into the buffer.  By delaying till now we avoid having to keep all the recycled data in memory.
    size_t rcdendx = filecumreccnts[i];  // index of last+1 record in the current file
    size_t filebufferx = filebufferalignoffset;  // write pointer into the file buffer.  Starts at sector alignment
    int64_t padlen = 0;  // amount extra we must write for boundary alignment
    for(;runningrcdx<rcdendx;++runningrcdx){
      // Move the records one by one, reading references from disk
      // Get the length that will be added to filebuffer
      size_t valuelen = rcdend[runningrcdx] - (runningrcdx==0?0:rcdend[runningrcdx-1]);  // length allocated in file for this record slot
      char valuetype;   // this will hold the type of the next record
      // find the type of the next record, by going through valueclass until we come to the next one
      while(!((valuetype=valueclass[runningvaluex++])&(vIndirectRemapped|vIndirectFirstMap)));  // skip types without diskdata
      if(valuetype&vIndirectFirstMap){
        // Here the data itself is in 'bytes'.  Move it.
        memcpy((char *)filebuffer.data()+filebufferx,(char *)bytes.data()+runningbytesx,valuelen);
        runningbytesx += valuelen;  // indicate that we have moved all those bytes out of 'bytes'
      }else{
        // The data is indirect, being remapped.  Read it from the Value Log into the filebuffer
        // We don't decompress it or check CRC; we just pass it on.  We know that
        // the compression/CRC do not depend on anything outside the actual value
        VLogRingRef ref((char *)bytes.data()+runningbytesx);   // analyze the reference

        std::string diskreadarea; size_t offset;  // disk buffer and returned data offset
        if((current_vlog->rings_[ref.Ringno()]->VLogRingGet(ref,diskreadarea,offset)).ok()){
          memcpy((char *)filebuffer.data()+filebufferx,diskreadarea.data()+offset,valuelen);
        }else{
          memset((char *)filebuffer.data()+filebufferx,0,valuelen);
          remappingerror = 1;  // indicate error remapping the file
        }

        // Housekeep for next iteration
        runningbytesx += VLogRingRef::sstrefsize;  // indicate that we have moved the reference out of 'bytes'
      }
      filebufferx += valuelen;  // advance the output pointer to the next location to fill
    }

    // Buffer has been built.  Create the file as sequential, and write it out
    {
      std::unique_ptr<WritableFile> writable_file;
      iostatus = immdbopts_->env->NewWritableFile(pathnames.back(),&writable_file,envopts_);  // open the file
      if(!iostatus.ok()) {
        ROCKS_LOG_ERROR(immdbopts_->info_log,
          "Error opening VLog file %s for write, Status code/subcode=%d/%d",pathnames.back().c_str(),iostatus.code(),iostatus.subcode());
      }
#if DEBLEVEL&2
printf("file %s: %" PRIu64 " bytes\n",pathnames.back().c_str(), lenofthisfile);
#endif

      if(iostatus.ok()) {
        // For direct I/O, extend the length of the write to the required alignment.  RocksDB seems to think 4K is the needed alignment; we will go with what it tells us
        // We go ahead and do this for all writes, because if the user switches to direct reads, padding the read buffer might carry past end of file if the file
        // was not padded to a block boundary when written.  In case that's a problem, extend all writes.
        if(1||envopts_.use_direct_writes){
          // See how much we must add to pad to sector length
          padlen = (writable_file->GetRequiredBufferAlignment()-1)&-(int64_t)(filebufferx-filebufferalignoffset);
          // For paranoid security purposes, clear the end of the sector.  It might contain passwords or something that a user with mere
          // file access shouldn't see.
          memset((char *)filebuffer.data()+filebufferx,0,padlen);
        }        // no extension needed if not direct I/O
        iostatus = writable_file->Append(Slice((char *)filebuffer.data()+filebufferalignoffset,filebufferx+padlen-filebufferalignoffset));  // write out the data
        if(!iostatus.ok()) {
          ROCKS_LOG_ERROR(immdbopts_->info_log,
            "Error writing to VLog file %s, Status code/subcode=%d/%d",pathnames.back().c_str(),iostatus.code(),iostatus.subcode());
        }
      }

      // Log writing the file
      ROCKS_LOG_INFO(
        current_vlog->immdbopts_->info_log, "[%s] [JOB %d] wrote VLog file# %" PRIu64 ": %" PRIu64 " values, %" PRIu64 " bytes",
        current_vlog->cfd_->GetName().c_str(), job_id,
        VLogRingRef(ringno_,(int)fileno_for_writing+i).FileNumber(), filecumreccnts[i]-(i?filecumreccnts[i-1]:0), filebufferx+padlen-filebufferalignoffset);

      // Sync the written data.  We must make sure it is synced before the SSTs referring to it are committed to the manifest.
      // We might as well sync it right now
      if(iostatus.ok()) {
        iostatus = writable_file->Fsync();  // scaf should fsync once for all files?
        if(!iostatus.ok()) {
          ROCKS_LOG_ERROR(immdbopts_->info_log,
            "Error Fsyncing VLog file %s, Status code/subcode=%d/%d",pathnames.back().c_str(),iostatus.code(),iostatus.subcode());
        }
      }

// obsolete     // Log syncing the file   this is a useful progress point
// obsolete     ROCKS_LOG_INFO(
// obsolete         current_vlog->immdbopts_->info_log, "[%s] synced file# %" PRIu64,
// obsolete         current_vlog->cfd_->GetName().c_str(), 
// obsolete         VLogRingRef(ringno_,(int)fileno_for_writing+i).FileNumber());
// obsolete 
    }  // this closes the writable_file


    // Reopen the file as randomly readable
    std::unique_ptr<RandomAccessFile> fp;  // the open file

    if(iostatus.ok()) {
      iostatus = immdbopts_->env->NewRandomAccessFile(pathnames.back(), &fp, envopts_);
      if(!iostatus.ok()) {
        ROCKS_LOG_ERROR(immdbopts_->info_log,
          "Error reopening VLog file %s for reading, Status code/subcode=%d/%d",pathnames.back().c_str(),iostatus.code(),iostatus.subcode());
      }else{
        // Log reopening the file
        ROCKS_LOG_INFO(
            current_vlog->immdbopts_->info_log, "[%s] [JOB %d] reopened file# %" PRIu64,
            current_vlog->cfd_->GetName().c_str(), job_id,
            VLogRingRef(ringno_,(int)fileno_for_writing+i).FileNumber());
      }
    }

    filepointers.push_back(std::move(fp));  // push one fp, even if null, for every file slot

    // File errors take priority over remapping errors.  If there are only remapping errors, create a status for them
    if(remappingerror && iostatus.ok())iostatus =  Status::Corruption("Errors remapping values during compaction");

    // if there was an I/O error, return the error, localized to the file by a negative offset in the return area.
    // (the offset can never be zero)
    VLogRingRefFileOffset reportedlen = filebufferx-filebufferalignoffset;
    if(!iostatus.ok()) {
      resultstatus.push_back(iostatus);  // save the error details
      reportedlen = -reportedlen;  // flag the offending file
    }

    // There are two file 'lengths'.  The length of the valid data only is used internally for assigning offsets to references.  The length shown by the file metadata
    // is used to indicate the size of the file for space-reporting purposes.  The metadata length is longer than the actual data length if we have padded
    // the filelength in anticipation of direct reads.
    fileendoffsets.push_back(reportedlen);   // the actual data length, negative if error
    fileendoffsetspadded.push_back(filebufferx+padlen-filebufferalignoffset);  // the padded data length, never negative
    // If the metadata length is greater than the actual length, the difference must be included is initial fragmentation.
    initfrag += (filebufferx+padlen-filebufferalignoffset) - reportedlen;
  }

  // Files are written and reopened.  Transfer them to the fd_ring under lock.
  // ALSO: clear the unused ping-pong buffer, if the current one has been in place 'long enough'

  // We lock during the initialization of the added block because the ring may be resized at any time & thus all changes to it must be performed
  // under lock to make sure they get resized correctly.  The lock is not needed to make Get() work.
#if DELAYPROB
ProbDelay();
#endif
  bool clearedprevbuffer=false;  // set if we cleared the oprevious ping-pong buffer while we were here
  AcquireLock();
    // Figure out which ring slot the new file(s) will go into
    currentarray = atomics.currentarrayx.load(std::memory_order_acquire);  // fetch current ping-pong side
#if DELAYPROB
ProbDelay();
#endif 
    std::vector<VLogRingFile> *fdring = fd_ring + currentarray;  // the current ring array
    size_t new_ring_slot = Ringx(*fdring,fileno_for_writing);  // position therein

    // Loop to add each file to the ring
    for(uint32_t i=0;i<pathnames.size();++i) {
      (*fdring)[new_ring_slot]=std::move(VLogRingFile(filepointers[i],fileendoffsetspadded[i],(i?0:1)));  // mark the first file (only) as nondeletable.  That will be enough to protect them all.
      if(++new_ring_slot==(*fdring).size()){
        //  The write wraps around the end of the ring buffer...
        new_ring_slot = 0;   // advance to next slot, and handle wraparound (avoiding divide)
        // ...and at that point we check to see if the head pointer has been incremented through half of its size.
        // Surely every core has by now been updated with the new ping-pong buffer; so we can clear
        // the previous buffer.
        // Almost always, this will be fast because the previous buffer will already be empty.  Only the first time after
        // a resize will this clear a large vector.  And even then, the elements of the vector will already have been
        // invalidated (when they were moved to the current buffer) and thus will take no processing beyond visiting the pointer.
        // Thus, it is safe to do this under lock; it is the completion of the ring resizing.  We will only have delay if
        // freeing the buffer causes this thread to be preempted by a higher-priority thread, leaving this thread waiting while holding
        // the lock; but nothing could be done to prevent that anyway.
        if(atomics.fd_ring_head_fileno.load(std::memory_order_acquire)>atomics.fd_ring_prevbuffer_clear_fileno.load(std::memory_order_acquire)){
          fd_ring[1-currentarray].clear();  // clear the previous buffer, to save space
          clearedprevbuffer=true;  // remember what we did for later logging
        }
#if DELAYPROB
ProbDelay();
#endif 
      }
    }
  ReleaseLock();
  if(clearedprevbuffer)ROCKS_LOG_INFO(immdbopts_->info_log,"[JOB %d] Previous VLogRing buffer has been cleared\n",job_id);

  // fill in the FileRef for the first value, with its length
  firstdataref.FillVLogRingRef(ringno_,fileno_for_writing,0,rcdend[0]);

  ROCKS_LOG_INFO(
      current_vlog->immdbopts_->info_log, "[%s] [JOB %d] all files added to VLogRing",
      current_vlog->cfd_->GetName().c_str(),job_id);

  return;

}

// Read the bytes referred to in the given VLogRingRef.  Uses release-acquire ordering to verify validity of ring
// Returns the bytes.  ?? Should this return to user area to avoid copying?
Status VLogRing::VLogRingGet(
  VLogRingRef& request,  // the values to read
  std::string& response,   // return value - the data pointed to by the reference
  size_t& offset   // return value - offset in response of the start of the data
)
{
  Slice resultslice;  // place to get back pointer to data, which may be different from scratch (if cached)
  // Read the reference.  Make sure we have the most recent copy of the ring information, and the most recent
  // copy of the file pointer.

  // We have to get the latest copy of the validity information, and also the latest copy of the memory pointer.
  // We have no way of knowing when these become valid, as they are not protected by any lock.  But they should have
  // been updated long before we need them.  If the current values indicate invalid references or nonexistent pointers
  // wait a little while and retry, before giving up.
  //
  // The tricky part is when the ring has to grow owing to growth in the database.  We don't want Get() to have to acquire a lock,
  // so we keep two rings hanging around: the current one and its predecessor, which is half as big.  As part of request processing
  // we get a pointer to the current ring, which might be replaced before we finish processing.  But that's OK: the old ring will
  // be valid enough to get us to the correct file or to indicate that we need to reacquire the ring pointer.
  //
  // We hold the previous ring around for a long time, long enough to be sure the new ring is in use in all threads.

  // Even though we should never be presented with an invalid request (i. e. a request for a file that does not exist),
  // we must nevertheless verify that the request is between the head and tail pointers, because if the ring has grown and we are
  // still using the previous ring, a valid reference in the new ring to a slot that doesn't exist in the old ring will be aliased
  // to an incorrect slot in the old ring.  This is very unlikely because the ring grows in the middle of compaction and the
  // references to new slots are not available to the user until the compaction is finished and versioned; but on non-Intel machines that
  // have loose memory coherence it is possible that a stale cache might persist for long enough to make trouble.

  RandomAccessFile *selectedfile;  // this will be the pointer to the file, once we have validated everything

  // We use acquire/release memory ordering on all accesses to the ring pointers (acquire for load, release for store).  This memory ordering means
  // that if we load a value X, we can be sure that subsequent reads to ANY memory (not just ring pointers) will see any store by the writer of X that happened before the
  // store to X.  It doesn't mean that the load gets the 'current' value, whatever that would mean; in particular, if a store is in flight, a second load from the same address
  // might return a different value even though no new store has been issued anywhere.
  //
  // The way to think of this is: if I load x=a and y=b with acquire/release ordering, the value of y may be from a later value of b than the one in effect when x was loaded,
  // but it can never be from an earlier value of b.

  // On Intel architectures acquire/release ordering does not take any extra time.

  // loop until we have resolved the pointer:
  int retrystate = 0;   // number of times we have retried

  while(1) {
    // acquire the head pointer
    VLogRingRefFileno headfile = atomics.fd_ring_head_fileno.load(std::memory_order_acquire);
#if DELAYPROB
ProbDelay();
#endif 
    // acquire the tail pointer.  This may be ahead of the status matching the head pointer, but that's OK as any valid reference should be
    // ahead of any valid tailpointer
    VLogRingRefFileno tailfile = atomics.fd_ring_tail_fileno.load(std::memory_order_acquire);
#if DELAYPROB
ProbDelay();
#endif 
    // acquire the current ring.  It is guaranteed to be able to hold the head and tail pointers that we have read, even if we are in the
    // process of switching rings, because we know that the head/tail pointers are valid from a time earlier than the ring
    std::vector<VLogRingFile> *fdring = fd_ring + atomics.currentarrayx.load(std::memory_order_acquire);  // the current ring array
#if DELAYPROB
ProbDelay();
#endif 
    // get the filepointer at the slot indicated for this file.  The pointer fetched may be bogus, if the file number is out of range, but
    // it will fetch without error.  The filepointer fetched here will be valid as of the time the head/tail pointers were set, meaning that
    // if the filenumber is valid, the pointer will be, UNLESS the ring was switched, in which case it will be either valid or null.  If null, we
    // will retry.  We don't use an atomic load operation, because we can't have an atomic unique_ptr (at least not in C++17); but loading a pointer will
    // be atomic on any modern CPU anyway
    selectedfile = (*fdring)[Ringx(*fdring,request.Fileno())].filepointer.get();  // fetch the filepointer, unconditionally

    // If the loaded pointer is valid and nonnull, go use it.  If invalid, set retrystate to so signify
    if(request.Fileno()>headfile || request.Fileno()<tailfile){ retrystate=-1; break;}   // detect invalid filenumber request
    if(selectedfile!=nullptr)break;  //  normal exit: reference found

    // Here we did not resolve the file correctly.  The possible causes, in order of severity, are:
    // 1. The head pointer that we fetched was stale and the reference is to a later file.  Since there is no guarantee that the load-for-acquire fetched the
    //  latest value stored in another thread, this may not be an error, and the problem will go away 'soon', where soon depends on the CPU architecture.  On
    //  an Intel architecture with its strict concurrency rules, this problem will never show up because there is a long delay between the time a file is created and
    //  the first time it can be referenced; but on a permissive-concurrency architecture it is possible, especially if a core wakes up after a delay with stale data in caches.
    //  We should wait a very short time and retry.
    // 2. The ring was resized.  This happens very rarely - every few weeks, perhaps.  The ring is locked for a few milliseconds (a few dozen instructions per file),
    //  but the old ring should suffice to handle Get() requests without interruption.  After the new ring is in place, all pointers in the previous one are released,
    //  and we must have fetched one of those.  A very short wait should suffice, then retry
    // 3. The reference is unusable - either it is corrupted or the file did not exist.  Either of those is Major Bad News, and we delay quite some time in hopes it will go away.
    //
    // We just run through delays of increasing length until the problem goes away or we declare it permanent
    atomics.writelock.load(std::memory_order_acquire);  // updates to the ring are always under lock, so we will have done a write-for-release to the lock after the update.
                // Here we do an acquire on the lock to make sure we see the updates
    if(retrystate<3){}   // first 3 times, just loop fast
    else if(retrystate<4){std::this_thread::sleep_for(std::chrono::microseconds(1));}  // then, 1usec - case 1/2
    else if(retrystate<6){std::this_thread::sleep_for(std::chrono::milliseconds(100));}  // then, 100msec - case 3
    else{selectedfile = nullptr; break;}   // then, permanent error.  Set selectedfile null as the error indicator

    ++retrystate;  // count the number of retries
  }
  Status iostatus;   // where we accumulate errors.  initialized to OK

  if(selectedfile==nullptr){
    // error fetching the file pointer
    if(retrystate<0){
      // file number not between head and tail 
      ROCKS_LOG_ERROR(immdbopts_->info_log,
        "Invalid file number %" PRIu64 " in reference in ring %d: headfile=%" PRIu64 ", tailfile=%" PRIu64 "",request.Fileno(),ringno_,atomics.fd_ring_head_fileno.load(std::memory_order_acquire),atomics.fd_ring_tail_fileno.load(std::memory_order_acquire));
      iostatus = Status::Corruption("Indirect reference to file out of bounds");
    } else {
      // file not opened in fdring
      ROCKS_LOG_ERROR(immdbopts_->info_log,
        "Reference to file number %" PRIu64 " in ring %d, but file was not opened",request.Fileno(),ringno_);
      iostatus = Status::Corruption("Indirect reference to unopened file");
    } 
    response.clear();   // error, return empty string
  } else {  // no error on file pointer, resolve the reference
    VLogRingRefFileLen readlen;  // the length of the read request
    VLogRingRefFileLen readfileoffset;  // the offset of the read from beginning-of-file
    size_t alignoffset;  // the offset in response of the actual read buffer
    // Make the user's buffer long enough to hold the string, and set the offset to the data within the allocated block
    if(envopts_.use_direct_reads){
      int64_t alignbdy = selectedfile->GetRequiredBufferAlignment();   // align buffers & lengths to this size
      readfileoffset=request.Offset()&-alignbdy;   // the offset within the file of the sector containing the requested data
      readlen=((request.Offset()-readfileoffset+request.Len()-1)|(alignbdy-1))+1;  // starting offset in sector; add len of read; back up to last byte; round up to last byte of sector; add 1 to get true length
      response.resize(readlen+alignbdy-1);  // make the buffer big enough to ensure there is an aligned section within it
      alignoffset=(alignbdy-1)&-(int64_t)response.data();   // calculate number of bytes to skip to get to aligned part
      offset=alignoffset+request.Offset()-readfileoffset;  // position of payload is alignment distance plus offset from start of sector
    }else{
      // not direct I/O.  Read only the amount needed, from its given offset
      readfileoffset=request.Offset();  // start at the data location
      readlen = request.Len();  // just the amount needed
      response.resize(readlen);  // make the buffer big enough to hold the read
      alignoffset=0;  // read into the beginning of the response buffer
      offset=0; // tell the caller the result is at the beginning of the read buffer
    }
    iostatus = selectedfile->Read(readfileoffset, readlen, &resultslice, (char *)response.data()+alignoffset);  // Read the reference
    if(!iostatus.ok()) {
      ROCKS_LOG_ERROR(immdbopts_->info_log,
        "Error reading reference from file number %" PRIu64 ", offset%" PRIu64 ", length %" PRIu64 " in ring %d",request.Fileno(),request.Offset(),request.Len(),ringno_);
      response.clear();   // error, return empty string
    } else {
      // normal path.  if the data was read into the user's buffer, leave it there; otherwise copy it in
      // this copy should never be needed for direct I/O, so we don't bother discarding the padding for that case
      if(response.data()+alignoffset!=resultslice.data())response.replace(alignoffset,readlen,resultslice.data(),readlen);
    }
  }
  return iostatus;
}

// Install a new SST into the ring, with the given earliest-VLog reference.  Increment refcounts
void VLogRing::VLogRingSstInstall(
  FileMetaData& newsst   // the SST that has just been created & filled in
){
#if DELAYPROB
ProbDelay();
#endif 
  AcquireLock();  // lock at the Ring level to allow multiple compactions on the same CF

    std::vector<VLogRingFile> *fdring = fd_ring + atomics.currentarrayx.load(std::memory_order_acquire);  // the current ring array
#if DELAYPROB
ProbDelay();
#endif 
    // Find the slot that this reference maps to
    VLogRingRefFileno reffile = newsst.indirect_ref_0[ringno_];
    size_t slotx = Ringx(*fdring,reffile);  // the place this file's info is stored - better be valid
// scaf out of bounds?

    // Increment the usecounts for the slot.  The usecounts are -1 in the first file of a new group, until the first reference is added.  This prevents deleting the group until the references have been installed
    (*fdring)[slotx].refcount_deletion=std::max((*fdring)[slotx].refcount_deletion,0)+1;  // starting from now until this SST is deleted, this VLog file is in use locally
    (*fdring)[slotx].refcount_manifest=std::max((*fdring)[slotx].refcount_manifest,0)+1;  // starting from now until this SST is uncurrent, this VLog file is in use in the manifest

    // Install the SST into the head of the chain for the slot
    newsst.ringfwdchain[ringno_] = (*fdring)[slotx].queue;   // attach old chain after new head
    newsst.ringbwdchain[ringno_] = nullptr;   // there is nothing before the new head
    if((*fdring)[slotx].queue!=nullptr)(*fdring)[slotx].queue->ringbwdchain[ringno_] = &newsst;  // if there is an old chain, point old head back to new head
    (*fdring)[slotx].queue = &newsst;  // point anchor to the new head

    // If this reference is lower than the queued pointer for the ring (rare), back up the ququed pointer
    if(reffile<atomics.fd_ring_queued_fileno.load(std::memory_order_acquire))atomics.fd_ring_queued_fileno.store(reffile,std::memory_order_release);
  ReleaseLock();
#if DEBLEVEL&1
printf("VLogRingSstInstall newsst=%p ref0=%lld refs=%d",&newsst,newsst.indirect_ref_0[ringno_],newsst.refs);
printf("\n");
#endif
#if DEBLEVEL&128
printf("Ring pointers: tail=%" PRIu64 " head=%" PRIu64 "\n   queue=",atomics.fd_ring_tail_fileno.load(),atomics.fd_ring_head_fileno.load());
for(uint64_t i = atomics.fd_ring_tail_fileno.load();i<=atomics.fd_ring_head_fileno.load();++i){int ct=0; FileMetaData* qp=(*fdring)[Ringx(*fdring,i)].queue;while(qp){++ct; qp=qp->ringfwdchain[ringno_];}printf("%d ",ct);}
printf("\nrefcount_deletion=");
for(uint64_t i = atomics.fd_ring_tail_fileno.load();i<=atomics.fd_ring_head_fileno.load();++i){printf("%d ",(*fdring)[Ringx(*fdring,i)].refcount_deletion);}
printf("\n");
printf("\nrefcount_manifest=");
for(uint64_t i = atomics.fd_ring_tail_fileno.load();i<=atomics.fd_ring_head_fileno.load();++i){printf("%d ",(*fdring)[Ringx(*fdring,i)].refcount_manifest);}
printf("\n");
#endif
}

// Remove an SST from the ring when it is no longer current.  Adjust manifest refcount, but not deletion refcount
void VLogRing::VLogRingSstUnCurrent(
  FileMetaData& retiringsst   // the SST that has just been obsoleted
){
#if DELAYPROB
ProbDelay();
#endif 
  AcquireLock();  // lock at the Ring level to allow multiple compactions on the same CF
#if DELAYPROB
ProbDelay();
#endif 

    std::vector<VLogRingFile> *fdring = fd_ring + atomics.currentarrayx.load(std::memory_order_acquire);  // the current ring array
#if DELAYPROB
ProbDelay();
#endif 
    // Find the slot that this reference maps to
    size_t slotx = Ringx(*fdring,retiringsst.indirect_ref_0[ringno_]);  // the place this file's info is stored - better be valid

    // Take the SST out of the ring.  It must be there.
    assert(retiringsst.ringbwdchain[ringno_] != &retiringsst);
    if(retiringsst.ringfwdchain[ringno_]!=nullptr)retiringsst.ringfwdchain[ringno_]->ringbwdchain[ringno_] = retiringsst.ringbwdchain[ringno_];  // if there is a next, point it back to the elements before this
    if(retiringsst.ringbwdchain[ringno_]!=nullptr)retiringsst.ringbwdchain[ringno_]->ringfwdchain[ringno_] = retiringsst.ringfwdchain[ringno_];  // if there is a prev, point it to the elements after this
    else (*fdring)[slotx].queue = retiringsst.ringfwdchain[ringno_]; // but if deleting the head, just make the next ele the head
    // Mark the chain fields to show that this block is no longer on the ring.  We use chain-to-self to indicate this
    retiringsst.ringbwdchain[ringno_] = &retiringsst;  // indicate 'block not in ring'

    // Decrement the refcount used for deciding which files are needed in the manifest
    --(*fdring)[slotx].refcount_manifest;  // this reference is no longer in the manifest

  ReleaseLock();
#if DEBLEVEL&1
printf("VLogRingSstUnCurrent retiringsst=%p ref0=%lld refs=%d",&retiringsst,retiringsst.indirect_ref_0[ringno_],retiringsst.refs);
printf("\n");
#endif
#if DEBLEVEL&128
printf("Ring pointers: tail=%" PRIu64 " head=%" PRIu64 "\n   queue=",atomics.fd_ring_tail_fileno.load(),atomics.fd_ring_head_fileno.load());
for(uint64_t i = atomics.fd_ring_tail_fileno.load();i<=atomics.fd_ring_head_fileno.load();++i){int ct=0; FileMetaData* qp=(*fdring)[Ringx(*fdring,i)].queue;while(qp){++ct; qp=qp->ringfwdchain[ringno_];}printf("%d ",ct);}
printf("\nrefcount_deletion=");
for(uint64_t i = atomics.fd_ring_tail_fileno.load();i<=atomics.fd_ring_head_fileno.load();++i){printf("%d ",(*fdring)[Ringx(*fdring,i)].refcount_deletion);}
printf("\n");
printf("\nrefcount_manifest=");
for(uint64_t i = atomics.fd_ring_tail_fileno.load();i<=atomics.fd_ring_head_fileno.load();++i){printf("%d ",(*fdring)[Ringx(*fdring,i)].refcount_manifest);}
printf("\n");
#endif
}

// Remove the VLog file's dependency on an SST, and delete the VLog file if it is now unused.  Advance tail pointer past deleted files
void VLogRing::VLogRingSstDelete(
  FileMetaData& expiringsst   // the SST that is about to be destroyed
){
  std::vector<VLogRingFileDeletion> deleted_files;  // files put here will be deleted at the end of this routine
  // If this file is still on the queue for this ring, we must be closing and releasing resources.  We don't want to delete all the files just because
  // the references went away!  But we can be sure that if this SST is still on the queue for a file, that file is still needed.  So, we detect that
  // case and DO NOT reduce the usecount for the file.  Eventually the rings will be freed, which will close the files but will not delete the data
  if(expiringsst.ringbwdchain[ringno_] != &expiringsst){
#if DEBLEVEL&1
  printf("Deleting file on ring: %p\n",&expiringsst);
#endif
    VLogRingSstUnCurrent(expiringsst);  // If backchain is valid (not looped to self), this block is still on the queue.  Remove from queue but leave (permanently) referenced
    return;
  }
#if DELAYPROB
ProbDelay();
#endif 
  AcquireLock();  // lock at the Ring level
#if DELAYPROB
ProbDelay();
#endif 

    std::vector<VLogRingFile> *fdring = fd_ring + atomics.currentarrayx.load(std::memory_order_acquire);  // the current ring array
#if DELAYPROB
ProbDelay();
#endif 
    // Find the slot that this reference maps to
    size_t slotx = Ringx(*fdring,expiringsst.indirect_ref_0[ringno_]);  // the place this file's info is stored - better be valid

    // Decrement the usecount
    if(!--(*fdring)[slotx].refcount_deletion) {
      // The usecount went to 0.  We can free the file, if it is the oldest in the tail.
      // NOTE that the tailfile coming in might have a 0 refcount, if the previous deletion was interrupted.  We will never catch that case here, because
      // we are looking only for NEW deletions at the tail.  Interrupted deletions will be resumed the next time we CREATE a file.  That's not really a problem,
      // because whenever we delete SSTs for compaction we are also creating new ones; & if we aren't, there's no hurry to delete anything.
      VLogRingRefFileno tailfile = atomics.fd_ring_tail_fileno.load(std::memory_order_acquire);  // get up-to-date copy of tail file
#if DELAYPROB
ProbDelay();
#endif 
      if(expiringsst.indirect_ref_0[ringno_]==tailfile) {
        // the expiring sst was earliest in the tail.  See how many files in the tail have 0 usecount.  Stop looking
        // after we have checked the head.
        VLogRingRefFileno headfile = atomics.fd_ring_head_fileno.load(std::memory_order_acquire);  // get up-to-date copy of head file
#if DELAYPROB
ProbDelay();
#endif 
        // We need to make sure we don't perform any large memory allocations that might block while we are holding a lock on the ring, so we reserve space for the
        // deletions - while NOT holding the lock - and then count the number of deletions
        ReleaseLock(); deleted_files.reserve(max_simultaneous_deletions); AcquireLock(); // reserve space to save deletions, outside of lock
        // Go get list of files to delete, updating the tail pointer if there are any.  CollectDeletions will reestablish tailptr, arrayx, slotx
        CollectDeletions(tailfile,headfile,deleted_files);  // find deletions
      }
    }

  ReleaseLock();

  ApplyDeletions(deleted_files);  // Now that we have released the lock, delete any files we marked for deletion

#if DEBLEVEL&1
printf("VLogRingSstDelete expiringsst=%p ref0=%lld refs=%d",&expiringsst,expiringsst.indirect_ref_0[ringno_],expiringsst.refs);
printf("\n");
#endif
#if DEBLEVEL&128
printf("Ring pointers: tail=%" PRIu64 " head=%" PRIu64 "\n   queue=",atomics.fd_ring_tail_fileno.load(),atomics.fd_ring_head_fileno.load());
for(uint64_t i = atomics.fd_ring_tail_fileno.load();i<=atomics.fd_ring_head_fileno.load();++i){int ct=0; FileMetaData* qp=(*fdring)[Ringx(*fdring,i)].queue;while(qp){++ct; qp=qp->ringfwdchain[ringno_];}printf("%d ",ct);}
printf("\nrefcount_deletion=");
for(uint64_t i = atomics.fd_ring_tail_fileno.load();i<=atomics.fd_ring_head_fileno.load();++i){printf("%d ",(*fdring)[Ringx(*fdring,i)].refcount_deletion);}
printf("\n");
printf("\nrefcount_manifest=");
for(uint64_t i = atomics.fd_ring_tail_fileno.load();i<=atomics.fd_ring_head_fileno.load();++i){printf("%d ",(*fdring)[Ringx(*fdring,i)].refcount_manifest);}
printf("\n");
#endif

}

// Return a vector of SSTs that have the smallest oldest-reference-filenumbers.  The maximum number is the capacity of laggingssts; the
// actual number returned will be a number that avoids getting only part of the references to a VLog file, but never less than n unless
// there are fewer SSTs than that.
// We don't return SSTs that are being compacted or are not current.
// The results are in ascending order of starting key
// This routine runs under the compaction mutex (so that it doesn't choose a file twice).  We also acquire the ring lock, because
// otherwise we would have to use memory-acquire ordering on reads from the sst chain fields, lest there be a problem with store
// ordering when another thread is writing to the chains
void VLogRing::VLogRingFindLaggingSsts(
  size_t minfreevlogfiles,  // minimum number of files to free up.  0 is a debugging mode: we stop checking for SSTs as soon as the minimum SST count is reached
  size_t minssts,   // minimum number of SSTs to put into the compaction.  0 is a debugging mode, stopping SST search
  std::vector<CompactionInputFiles>& laggingssts,  // result: vector of SSTs that should be recycled.  The capacity on entry is the maximum size allowed
  VLogRingRefFileno& lastfile   // result: the last VLog file that is being freed
) {
  laggingssts.resize(0);   // int size to 0, but leave the capacity.  clear() may change the capacity

  // Process the oldest files, starting at queued_fileno and going until we have collected enough files, or at most
  // to the head.  In case there is ring resizing, we update the ring pointer every time we acquire a lock.  In case the queued_pointer
  // moves backwards, we fetch it once and never back up (and never move the queued_fileno backwards).  Backward movements of the
  // queued_fileno would be possible if a new write does not remap a write that falls into the Active Recycling region: not normal,
  // but depending on user settings it is possible, especially if the Active Recycling region turns out to be very large because of a number of
  // undeleted files.  We lock/unlock the ring over each file, to ensure that the chains are not messed up while we are chasing them AND
  // to ensure that the queued_fileno is not written back (or to the same spot) before we advance it.

  // We accept SSTs for processing as long as they are worth the trouble.  They are worth the trouble if: (1) they are needed to free up
  // a minimum number of files so that we make progress in defragmenting; (2) the ratio of files freed to SSTs processed is high enough.
  // We can't answer this last question about a file until we see how many empty files follow it.

  VLogRingRefFileno currentfile = 0;  // the file we are working on (at top of loop, the file we worked on in the previous iteration).
  VLogRingRefFileno startingnonemptyfile = 0;  // the first file that will be freed by action here.  Init to invalid low value

   // init to a low_value
  size_t viablesstct = 0;  // the number of SSTs in the last acceptable solution found (lastfile will hold the # VLog files)

  // Get the head pointer, beyond which we will not read.  This may become obsolete, but that's no big deal.  It will surely be big enough.
  VLogRingRefFileno headfile = atomics.fd_ring_head_fileno.load(std::memory_order_acquire);

  // There has to be enough room between the recycling queue pointer and the head to actually allow files to be deleted; otherwise
  // we just grind away trying to recycle but never getting anywhere
  if(headfile<=atomics.fd_ring_queued_fileno.load(std::memory_order_acquire)+deletion_deadband+minfreevlogfiles){lastfile = currentfile; return;}

  // Loop until we have collected enough references.  Stop if we have run out of files to look at
  while(laggingssts.size()<laggingssts.capacity()) {
    // Acquire lock on the current ring.  A single file will not usually have many SSTs chained to it - approximately the number in a compaction, say
    AcquireLock();

      // Reacquire ring base, in case it was resized
      std::vector<VLogRingFile> *vlogringbase = fd_ring+atomics.currentarrayx.load(std::memory_order_acquire);
      // Reacquire the queued-file pointer: necessary in case a bunch of files were deleted and the old tail, including the
      //  old queued area, were overwritten by new files.  We know the queued-file pointer is never below the tail pointer
      VLogRingRefFileno queuefile = atomics.fd_ring_queued_fileno.load(std::memory_order_acquire);
      // slot to process is larger of (previous slot processed+1) and queued_fileno
      ++currentfile; if(queuefile>currentfile)currentfile = queuefile;
      // if we have processed all the deletable files, exit.   In fact, stop one file early for safety
      if(currentfile+deletion_deadband>=headfile){ReleaseLock(); break;}  // *** early loop exit, must release lock

      // Chase the chain.  If the chain is empty, and we are processing queued_fileno, increment queued_fileno
      size_t slotx = Ringx(*vlogringbase,currentfile);    // get slot number for the file we are working on
      FileMetaData *chainptr = (*vlogringbase)[slotx].queue;  // first SST in chain
      if(chainptr==nullptr && currentfile==queuefile)atomics.fd_ring_queued_fileno.store(queuefile+1,std::memory_order_release);  // if this chain empty, don't look here again
      // Remember the file# of the first file we try to free up
      if(chainptr!=nullptr && startingnonemptyfile==0)startingnonemptyfile = currentfile;
      bool nonemptychain = chainptr!=nullptr;  // was this chain nonempty?

      for(;chainptr!=nullptr&&laggingssts.size()<laggingssts.capacity();chainptr=chainptr->ringfwdchain[ringno_]) {
        // If the file is not marked as being compacted, copy it to the result area
        // But never recycle a file from L0, because it cannot be reinserted into the database in the correct order
        if(!chainptr->being_compacted&&!(chainptr->level==0))laggingssts.emplace_back(*chainptr);
      }
      // at the end of the loop chainptr is null if we were able to visit all the SSTs for the file

    // release lock
    ReleaseLock();

    // If all the SSTs of the first file are in compaction, stop.
    // This is how we keep from repeatedly starting an Active Recycling pass when there is one running, or a compaction that will serve as well.
    if(nonemptychain && laggingssts.size()==0)return;  // stop looking if we encounter a compaction going on

    // Assess whether the current selection is a keeper.  It is, if
    // (1) we have not freed up the minimum # files
    // (2) we have not processed the minimum # SSTs
    // (3) we got all the SST files for the last VLog file, and the ratio of files/SST is acceptable
    if(  (currentfile-startingnonemptyfile+1<=minfreevlogfiles)  // VLog file minimum requirement not exceeded by the new file
      || (laggingssts.size()<=minssts)      // SST minimum requirement requirement not exceeded by the new file
      || (chainptr==nullptr && minssts!=0 && minfreevlogfiles!=0 && (currentfile-startingnonemptyfile+1)>((float)laggingssts.size()*ARdesired_Vlog_files_per_SST)) ) {  // getting enough VLog files for the SSTs we are processing - not if debug
      // The current solution is viable.  Save it so we can revert to it if we don't get anything better
      lastfile = currentfile;  // save the last VLog file being freed, in the result
      viablesstct = laggingssts.size();   // save the corresponding # input files
    }
     // (if the result buffer exactly fills up, we will not stick around to see if a run of following empty files would make the
     // solution viable.  That's no great loss.)

    // continue looking if there is more room for results.  If minfreevlogfiles==0 we could stop now, but that's for debug only.  Normally we check up to the max # SSTs to see how many we can take
  }

  // if there is a viable solution point saved, revert to it; otherwise use all we collected
  if(viablesstct)laggingssts.resize(viablesstct); else lastfile=currentfile;  // lastfile is valid if we revert; if not, set it to current file#.

  // Sort the files on starting key
  std::sort(laggingssts.begin(), laggingssts.end(),
            [&](const CompactionInputFiles& f1, const CompactionInputFiles& f2) -> bool {
              return ((cfd_->user_comparator)()->Compare)(Slice(*f1.files[0]->smallest.rep()),Slice(*f2.files[0]->smallest.rep())) < 0;
            });

  return;
}


//
//
// ******************************************** Vlog ***********************************************
// One per CF, containing one or more VLogRings.  Vlog is created for any CF that uses a table type
// that is compatible with indirect values.
//
//

// A VLog is a set of VLogRings for a single column family.  The VLogRings are distinguished by an index.
// The VLog is initialized with a set of empty rings.  If the ring is being created because a column family is being
// added to an existing database, these empty rings are a valid starting point.  If the ring is being created at
// database-(re)open time, there will be a subsequent call to VLogInit
VLog::VLog(
  ColumnFamilyData *cfd,  // the info for the column family
  const ImmutableDBOptions& immdbopts,   // The current options
  const EnvOptions& file_options  // options to use for all VLog files
) :
  rings_(std::vector<std::unique_ptr<VLogRing>>()),  // start with empty rings
  starting_level_for_ring_(std::vector<int>()),
  cfd_(cfd),
  waiting_sst_queues(std::vector<FileMetaData *>{4})  // init max possible # ring anchors
{  writelock.store(0,std::memory_order_release);
   initcomplete.store(0,std::memory_order_release);
#if DEBLEVEL&1
printf("VLog cfd=%p name=%s\n",cfd,cfd->GetName().data());
#endif
  VLogInit(std::vector<std::string>{},std::vector<VLogRingRefFileLen>{},immdbopts,file_options);
}


// Initialize each ring to make it ready for reading and writing.
// It would be better to do this when the ring is created, but with the existing interfaces there is
// no way to get the required information to the constructor
Status VLog::VLogInit(
    std::vector<std::string> vlg_filenames,    // all the filenames that exist for this database - at least, all the vlg files
    std::vector<VLogRingRefFileLen> vlg_filesizes,   // corresponding file sizes
    const ImmutableDBOptions& immdbopts,   // The current Env
    const EnvOptions& file_options  // options to use for all VLog files
) {
#if DEBLEVEL&1
printf("VLogInit cfd_=%p name=%s\n",cfd_,cfd_->GetName().data());
#endif
  immdbopts_ = &immdbopts;  // save the options
  // Save the ring starting levels
  starting_level_for_ring_.clear();
  for(auto l : cfd_->VLogRingActivationLevel()){if (l<0)l+=cfd_->NumberLevels(); starting_level_for_ring_.push_back(l);}

  // If the database is not new, the CF will have the stats for the rings.  If the CF has no stats, create null stats for each level
  if(cfd_->vloginfo().size()==0)cfd_->vloginfo().resize(starting_level_for_ring_.size());
    // scaf worry about mismatch between # rings given & # in database
#if DEBLEVEL&0x800
    {printf("VLogInit: ");
       const std::vector<VLogRingRestartInfo> *vring = &cfd_->vloginfo();
       for(int i=0;i<vring->size();++i){printf("ring %d: size=%" PRIu64 ", frag=%" PRIu64 ", space amp=%5.2f, files=",i,(*vring)[i].size,(*vring)[i].frag,((double)(*vring)[i].size)/(1+(*vring)[i].size-(*vring)[i].frag));for(int j=0;j<(*vring)[i].valid_files.size();++j){printf("%" PRIu64 " ",(*vring)[i].valid_files[j]);};printf("\n");}
    }
#endif

  // Delete any rings left from initialization or a closed database, which should go through and free up all their resources.  Then we will continue with new rings.
  rings_.clear();  // delete any rings that are hanging around

  // cull the list of files to leave only those that apply to this cf
  std::vector<std::string> existing_vlog_files_for_cf;  // .vlg files for this cf
  std::vector<VLogRingRefFileLen> existing_vlog_sizes_for_cf;  // corresponding file sizes
  std::string columnsuffix = "." + kRocksDbVLogFileExt + cfd_->GetName();  // suffix for this CF's vlog files
  for(uint32_t i = 0;i<vlg_filenames.size();++i){
    if(vlg_filenames[i].size()>columnsuffix.size() && 0==vlg_filenames[i].substr(vlg_filenames[i].size()-columnsuffix.size()).compare(columnsuffix)){
      existing_vlog_files_for_cf.emplace_back(vlg_filenames[i]);  // if suffix matches, keep the filename
      existing_vlog_sizes_for_cf.push_back(vlg_filesizes[i]);   // and size
    }
  }

  // For each ring, allocate & initialize the ring, and save the resulting object address
  for(uint32_t i = 0; i<cfd_->vloginfo().size(); ++i) {
    // Create the ring and save it
// use these 3 lines when make_unique is supported    rings_.push_back(std::move(std::make_unique<VLogRing>(i /* ring# */, cfd_ /* ColumnFamilyData */, existing_vlog_files_for_cf /* filenames */,
//      (VLogRingRefFileno)early_refs[i] /* earliest_ref */, (VLogRingRefFileno)cfd_->GetRingEnds()[i]/* latest_ref */,
//      immdbopts /* immdbopts */, file_options)));
    std::unique_ptr<VLogRing> vptr(new VLogRing(i /* ring# */, cfd_ /* ColumnFamilyData */, existing_vlog_files_for_cf /* filenames */,
      existing_vlog_sizes_for_cf /* filesizes */, 
      &immdbopts /* immdbopts */, file_options));
    rings_.push_back(std::move(vptr));

    // if there was an error creating the ring, abort
    if(!rings_.back()->initialstatus.ok())return rings_.back()->initialstatus;

    // Traverse the sst waiting queue, taking each SST off and installing it into the queue according to its reference.
    // Each call will lock the ring, but the ones after the first should be processed very quickly because the lock
    // will still be held by this core
    FileMetaData *sstptr = waiting_sst_queues[i];  // start on the chain for this ring
    while(sstptr!=nullptr){FileMetaData *nextsstptr = sstptr->ringfwdchain[i]; rings_[i]->VLogRingSstInstall(*sstptr); sstptr = nextsstptr; }
       // remove new block from chain before installing it into the ring

    // Now that the ring hss all the files and all the references, verify that all the needed files are present
    Status s  = rings_[i]->VerifyFilesPresent();  // Verify initial validity
    if(!s.ok())return s;   // abort if files missing.  Error has beenn logged
  }

  return Status();
}


// Read the bytes referred to in the given VLogRingRef.  Uses release-acquire ordering to verify validity of ring
// Returns the bytes.  ?? Should this return to user area to avoid copying?
Status VLog::VLogGet(
  const Slice& reference,  // the reference
  std::string& result   // where the result is built
)
{
  Status s = VLogRingRef::VLogRefAuditOpaque(reference);  // return status, initialized to ok
  if(!s.ok()){
    ROCKS_LOG_ERROR(immdbopts_->info_log,
      "Indirect reference is not %d bytes long",VLogRingRef::sstrefsize);
    return(s);
  }

  VLogRingRef ref = VLogRingRef(reference.data());   // analyze the reference

  // Because of the OS kludge that doesn't allow zero-length files to be memory-mapped, we have to check to make
  // sure that the reference doesn't have 0 length: because the 0-length reference might be contained in a
  // (nonexistent) 0-length file, and we'd better not try to read it
  if(static_cast<uint64_t>(ref.Len())==0){ result.clear(); return s; }   // length 0; return empty string, no error

  if(ref.Len()<(1+4)){
    ROCKS_LOG_ERROR(immdbopts_->info_log,
        "Reference too short in file %" PRIu64 " in ring %d",ref.Fileno(),ref.Ringno());
    return s = Status::Corruption("indirect reference is too short.");
  }

  uint64_t starttime = immdbopts_->env->NowMicros();

  // Vector to the appropriate ring to do the read
  std::string ringresult;  // place where ring value will be read
  size_t dataoffset;   // offset in ringresult where the data starts (0 except for direct I/O)
  if(!(s = rings_[ref.Ringno()]->VLogRingGet(ref,ringresult,dataoffset)).ok())
    return s;  // read the data; if error reading, return the error.  Was logged in the ring

  uint64_t readtime = immdbopts_->env->NowMicros(); uint64_t readdur = readtime - starttime;

  // check the CRC
  // CRC the type/data and compare the CRC to the value in the record
  if(crc32c::Value(ringresult.data()+dataoffset,ref.Len())!=0xffffffff){  // take CRC of the header byte/data.  Should come out to 0 before complement
    ROCKS_LOG_ERROR(immdbopts_->info_log,
      "CRC error reading from file number %" PRIu64 ", offset %" PRIu64 ", length %" PRIu64 " in ring %d",ref.Fileno(),ref.Offset(),ref.Len(),ref.Ringno());
    return s = Status::Corruption("indirect reference CRC mismatch.");
  }
  uint64_t comptime = immdbopts_->env->NowMicros(); uint64_t compdur = comptime - readtime;

   // extract the compression type and decompress the data
  //unsigned char ctype = ringresult[dataoffset];
  CompressionType ctype = (CompressionType) ringresult[dataoffset];
  if(ctype==CompressionType::kNoCompression) {
    // data is uncompressed; move it to the user's area
    result.assign(&ringresult[dataoffset+1],ref.Len()-(1+4));   // data starts at offset 1, and doesn't include 1-byte header or 4-byte trailer 
  } else {
    BlockContents contents;
   const UncompressionContext ucontext(ctype);
   const UncompressionInfo uinfo(ucontext,UncompressionDict::GetEmptyDict(),ctype);
// scaf this moves the data twice, which we could avoid if we rewrite the subroutine
    if(!(s = UncompressBlockContentsForCompressionType(uinfo,
        &ringresult[dataoffset+1], ref.Len()-(1+4), &contents,
          kVLogCompressionVersionFormat,
        *(cfd_->ioptions()))).ok()){
      ROCKS_LOG_ERROR(immdbopts_->info_log,
        "Decompression error reading from file number %" PRIu64 ", offset %" PRIu64 ", length %" PRIu64 " in ring %d",ref.Fileno(),ref.Offset(),ref.Len(),ref.Ringno());
      return s;
    }
    result.assign(contents.data.data(),contents.data.size());  // move data to user's buffer
  }


  // Store the durations and lengths for statistics.  We are going to calculate a linear model a + b*length for each statistic.  Thus, we store
  // AtA for the lengths and Aty for each statistic.  Each can be calculated incrementally as a sum of outer products
  AcquireLock();  // lock before modifying shared stats.  Eccch!  All that trouble avoiding locks on the ring, and now this.
    statslenata00 += 1; statslenata01 += ref.Len(); statslenata11[0] += ref.Len()*ref.Len();
    statslenata11[1] += statslenata11[0]>>32; statslenata11[0] &= 0xffffffff;   // propagate low half to high half as needed
    statsreadata0 += readdur;  statsreadata1[0] += readdur*ref.Len();  // add to ata for read time
    statsreadata1[1] += statsreadata1[0]>>32; statsreadata1[0] &= 0xffffffff;   // propagate low half to high half as needed
    statscompata0 += compdur;  statscompata1[0] += compdur*ref.Len();  // add to ata for comp time
    statscompata1[1] += statscompata1[0]>>32; statscompata1[0] &= 0xffffffff;   // propagate low half to high half as needed
  ReleaseLock();

  return s;

}

  void VLog::VLogSstInstall(
    FileMetaData& newsst   // the SST that has just been created & filled in
  ) {
#if DEBLEVEL&64
if(!rings_.size() || !newsst.indirect_ref_0.size())printf("VLogSstInstall newsst=%p\n",&newsst);
#endif
    // Initialization of SSTs that have been installed on the ring:
    // put the address of this VLog into the SST, so we can get back to this VLog from a reference to the file
    newsst.vlog=cfd_->vlog();   // Point the SST to this VLog, through a shared_ptr.  We have to have this chain field since cfd is not available in most functions
    // Make sure there is one chain-field pair per ring containing a reference
    newsst.ringfwdchain.clear(); newsst.ringfwdchain.resize(newsst.indirect_ref_0.size(),nullptr);
    newsst.ringbwdchain.clear(); newsst.ringbwdchain.resize(newsst.indirect_ref_0.size(),&newsst);
    // We set the backchain pointer to point to the block itself as an indication that the block is not
    // on the queue for a VLog file.  We need to know the queue status so that we don't leave a block on
    // a ring after the block itself has been deleted.  The not-on-queue indication persists throughout for
    // those rings that the block doesn't have a reference for; for the others it changes based on queue status

    if(initcomplete.load(std::memory_order_relaxed)||initcomplete.load(std::memory_order_acquire)){  // once set, it stays set; so we don't need any locked loads after the first nonzero is read
      // Normal case after initial recovery.  Put the SST into each ring for which it has a reference
      if(newsst.level<0)
        printf("installing SST with no level\n");  // scaf debug only
      for (uint32_t i=0;i<newsst.indirect_ref_0.size();++i)if(newsst.indirect_ref_0[i])rings_[i].get()->VLogRingSstInstall(newsst);
    } else {
      // if the rings have not been, or never will be, created, enqueue the SSTs here in the Vlog.  In case initialization ever
      // becomes multi-threaded, acquire a lock on the ring headers
      AcquireLock();
        // for each ring that has a reference in the SST, enqueue the SST to that ring's chains.  We know that there
        // are chain-fields for each ring containing a reference.  We use only the forward chains here since this queue
        // is consumed from head to tail
        for (uint32_t i=0;i<newsst.indirect_ref_0.size();++i)
          if(newsst.indirect_ref_0[i]){newsst.ringfwdchain[i] = waiting_sst_queues[i]; waiting_sst_queues[i] = &newsst;};
      ReleaseLock();
    }
  }

  // Remove an SST from the ring when it is no longer current.
  void VLog::VLogSstUnCurrent(
    FileMetaData& retiringsst   // the SST that has just been obsoleted
  ) {
#if DEBLEVEL&64
if(!rings_.size() || !retiringsst.indirect_ref_0.size())printf("VLogSstUnCurrent retiringsst=%p rings_.size()=%" PRIu64 "\n",&retiringsst,rings_.size());
#endif
    if(!rings_.size())return;  // no action if the rings have not, or never will be, created
    for (uint32_t i=0;i<retiringsst.indirect_ref_0.size();++i)if(retiringsst.indirect_ref_0[i])rings_[i].get()->VLogRingSstUnCurrent(retiringsst);
    }

  // Remove the VLog file's dependency on an SST, and delete the VLog file if it is now unused
  void VLog::VLogSstDelete(
    FileMetaData& expiringsst   // the SST that is about to be destroyed
  ) {
#if DEBLEVEL&64
if(!rings_.size() || !expiringsst.indirect_ref_0.size())printf("VLogSstDelete expiringsst=%p rings_.size()=%" PRIu64 "\n",&expiringsst,rings_.size());
#endif
    if(!rings_.size())return;  // no action if the rings have not, or never will be, created
    for (uint32_t i=0;i<expiringsst.indirect_ref_0.size();++i)if(expiringsst.indirect_ref_0[i])rings_[i].get()->VLogRingSstDelete(expiringsst);
    }


// Routine to look at the manifest refcounts and decide which files can be removed from the new Version.
// Called as part of applying the edits after compaction.
extern void DetectVLogDeletions(ColumnFamilyData *cfd,   // CF to work on
  std::vector<VLogRingRestartInfo> *vlogedits   // result: edits to make to the current files (all deletions).  Initially empty
  ) {
  if(cfd==nullptr || cfd->vlog()==nullptr)return;   // return if no CF, or CF does not support vlogs
  VLog *vlog = cfd->vlog().get();
  // During initial recovery we come through LogAndApply before we have run VLogInit to set the rings.  We should not be deleting VLog files then anyway,
  // so just return in that case
  if(vlog->rings_.size()==0)return;
  // See if there are VLog files that will not be needed after a restart.  These are files whose manifest refcount has gone to 0.
  // They may still be in use internally, but they will not be needed after all current business is finished.  If there are any, collect
  // them into an edit, which will be applied by the caller
  if(vlogedits != nullptr) {  // if the caller has requested deletions...
    // check for deletions in each ring.  At the beginning there are no rings and no first file in any ring; there can't be deletions then either
    // but we mustn't fail
    for(size_t i = 0;i<cfd->vloginfo().size();++i) {
      VLogRingRestartInfo ringchanges{};   // where we accumulate the changes for this 
      // start looking at the first file indicated as valid in the manifested data
      if(cfd->vloginfo()[i].valid_files.size()) {     // make sure there IS a first file
        VLogRingRefFileno currentoldest = cfd->vloginfo()[i].valid_files[0];  // first valid file
        VLogRingRefFileno saveoldest = currentoldest;  // save so we can see if we moved
        // Acquire lock on the ring so we know we have the correct ping-pong buffer.  Writes to the ring, and resizes, are performed outside the
        // current mutex
        { vlog->rings_[i]->AcquireLock();
          // Get the headpointer for the ring
          VLogRingRefFileno headfile = vlog->rings_[i]->atomics.fd_ring_head_fileno.load(std::memory_order_acquire);  // lock guarantees memory ordering
          // Get index of the current ring
          size_t currentpong = vlog->rings_[i]->atomics.currentarrayx.load(std::memory_order_acquire);  // lock guarantees memory ordering
          // Get pointer to current ring
          std::vector<VLogRingFile> *currentring = vlog->rings_[i]->fd_ring+currentpong;
          // Get slot number for the first file
          size_t slotx = vlog->rings_[i]->Ringx(*currentring, currentoldest);
          // Look at files. Accumulate stats of frag/size that will be removed.  End pointing to the file AFTER the last deletable file
          while(currentoldest+/* obsolete vlog->rings_[i]->*/deletion_deadband<headfile && (*currentring)[slotx].refcount_manifest==0){  // avoiding unsigned overflow
            ringchanges.size -= (*currentring)[slotx].length;
            if(++slotx == (*currentring).size())slotx=0;  // advance ring, wrapping if needed
            ++currentoldest;
          }
        vlog->rings_[i]->ReleaseLock(); }
        // if there are deletable files, add a deletion entry for them to the result area
        if(currentoldest>saveoldest) {
          ringchanges.frag = ringchanges.size;   // the reduction in size is also a reduction in fragmentation, since an empty file is 100% frag
          ringchanges.valid_files.push_back(0); ringchanges.valid_files.push_back(currentoldest-1);   // make the edit a delete for all files up to the last one we deleted
        }
      }
       // Move the (possibly empty) edit to the result
      (*vlogedits).push_back(ringchanges);  // tell the caller how to account for the deletion
    }
  }
}

  // Mark the VLog files that we have created as fully initialized, including being deletable when they have no referencess.
  // This must be called after the new SSTs have been installed into the VLogRingFiles
  void VLog::MarkVlogFilesDeletable(
    std::vector<VLogRingRestartInfo> vlog_edits){  // Info on the changes, one per ring.  All we care about is added files
    // Go through each ring
    for(size_t ringno = 0; ringno<vlog_edits.size();++ringno){
      rings_[ringno]->AcquireLock();
        // Fetch the up-to-date value for the current ring index
        uint64_t arrayx = rings_[ringno]->atomics.currentarrayx.load(std::memory_order_acquire);
        // Go through each (start,end) segment
        for(size_t startendx = 0; startendx<vlog_edits[ringno].valid_files.size();startendx+=2) {
          if(vlog_edits[ringno].valid_files[startendx]==0)continue;  // startfile=0 means deleted files; ignore that (should be first pair only)
          // Go through each file.  We do this under lock, but it's probably not necessary to lock, because no SST has a reference to these new VLog files yet,
          // so there is no question of simultaneous access.  The only worry would be resizing the ring while we modify the old ping-pong, but that is
          // done under the same mutex that this is.  For peace of mind, we take the lock anyway.  It is possible that multiple compactions' results have been
          // coalesced into this one edit, but this is normally a few dozen files at most (exception for opening huge compaction, but that's sui generis)
          //
          // Only the first file in a batch is marked non-deletable, but we don't know how many batches or subcompactions there are, so we have to check each file.
          for(size_t filex=vlog_edits[ringno].valid_files[startendx];filex<=vlog_edits[ringno].valid_files[startendx+1];++filex){
            // If the file is nondeletable, mark it empty.  A file may have started nondeletable but then had a reference added; don't disturb the counts in that case.
            size_t fdringx = rings_[ringno]->Ringx(rings_[ringno]->fd_ring[arrayx],filex);
            if(rings_[ringno]->fd_ring[arrayx][fdringx].refcount_deletion<0){
              rings_[ringno]->fd_ring[arrayx][fdringx].refcount_deletion = rings_[ringno]->fd_ring[arrayx][fdringx].refcount_manifest = 0;
            }
          }
      }
      rings_[ringno]->ReleaseLock();
    }
  }




}   // namespace rocksdb


	
