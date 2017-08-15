// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::Statistics

#include "rocksjni/statisticsjni.h"
#include "monitoring/histogram.h"
#include "monitoring/histogram_windowing.h"


namespace rocksdb {

  template <class T>
  StatisticsJni<T>::StatisticsJni(std::shared_ptr<Statistics> stats)
      : StatisticsImpl<T>(stats, false), m_ignore_histograms() {
  }

  template <class T>
  StatisticsJni<T>::StatisticsJni(std::shared_ptr<Statistics> stats,
      const std::set<uint32_t> ignore_histograms) : StatisticsImpl<T>(stats, false),
      m_ignore_histograms(ignore_histograms) {
  }

  template <class T>
  bool StatisticsJni<T>::HistEnabledForType(uint32_t type) const {
    if (type >= HISTOGRAM_ENUM_MAX) {
      return false;
    }
    
    if (m_ignore_histograms.count(type) > 0) {
        return false;
    }

    return true;
  }

  template class StatisticsJni<HistogramImpl>;
  template class StatisticsJni<HistogramWindowingImpl>;

};
