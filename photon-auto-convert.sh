#!/bin/bash

set -e

cc_files=$(find . -type f -name "*.cc" -not -path "./build/*" -not -path "./third-party/PhotonLibOS/*")
h_files=$(find . -type f -name "*.h" -not -path "./build/*" -not -path "./third-party/PhotonLibOS/*")
files="${cc_files} ${h_files}"

sed -i 's|#include <thread>|#include "port/port.h"|g' $files
sed -i 's|#include <mutex>|#include "port/port.h"|g' $files
sed -i 's|#include <condition_variable>|#include "port/port.h"|g' $files
sed -i 's/std::mutex/photon_std::mutex/g' $files
sed -i 's/std::condition_variable/photon_std::condition_variable/g' $files
sed -i 's/std::lock_guard/photon_std::lock_guard/g' $files
sed -i 's/std::unique_lock/photon_std::unique_lock/g' $files
sed -i 's/std::thread/photon_std::thread/g' $files
sed -i 's/std::this_thread/photon_std::this_thread/g' $files
