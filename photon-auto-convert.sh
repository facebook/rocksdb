#!/bin/bash

set -e

cc_files=$(find . -type f -name "*.cc" -not -path "./build/*")
h_files=$(find . -type f -name "*.h" -not -path "./build/*")
files="${cc_files} ${h_files}"

sed -i 's|#include <thread>|#include "port/port.h"|g' $files
sed -i 's|#include <mutex>|#include "port/port.h"|g' $files
sed -i 's|#include <condition_variable>|#include "port/port.h"|g' $files
sed -i 's/std::mutex/photon::std::mutex/g' $files
sed -i 's/std::condition_variable/photon::std::condition_variable/g' $files
sed -i 's/std::lock_guard/photon::std::lock_guard/g' $files
sed -i 's/std::unique_lock/photon::std::unique_lock/g' $files
sed -i 's/std::thread/photon::std::thread/g' $files
sed -i 's/std::this_thread/photon::std::this_thread/g' $files
