# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

# NOTE: if ... Exit $LASTEXITCODE lines are needed to report failures from
# native commands.
echo ===================== Install Dependencies =====================
if (!$env:JAVA_HOME -or !(Test-Path (Join-Path $env:JAVA_HOME "bin\javac.exe"))) {
  $javac = Get-Command javac -ErrorAction SilentlyContinue
  if (!$javac) {
    Write-Error "javac was not found on PATH and JAVA_HOME is not set."
    Exit 1
  }
  $env:JAVA_HOME = Split-Path (Split-Path $javac.Source -Parent) -Parent
}
echo "JAVA_HOME=$env:JAVA_HOME"
& "$env:JAVA_HOME\bin\java.exe" -version
& "$env:JAVA_HOME\bin\javac.exe" -version
mkdir $env:THIRDPARTY_HOME
cd $env:THIRDPARTY_HOME
echo "Building Snappy dependency..."
curl -Lo snappy-1.2.2.zip https://github.com/google/snappy/archive/refs/tags/1.2.2.zip
if (!$?) { Exit $LASTEXITCODE }
unzip -q snappy-1.2.2.zip
if (!$?) { Exit $LASTEXITCODE }
cd snappy-1.2.2
mkdir build
cd build
& cmake -G "$env:CMAKE_GENERATOR" .. -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF
if (!$?) { Exit $LASTEXITCODE }
cmake --build . --config Debug --parallel 32 -- /p:Platform=x64
if (!$?) { Exit $LASTEXITCODE }
echo ======================== Build RocksDB =========================
cd $env:GITHUB_WORKSPACE
$env:Path = "$env:JAVA_HOME\bin;" + $env:Path
mkdir build
cd build
& cmake -G "$env:CMAKE_GENERATOR" -DCMAKE_BUILD_TYPE=Debug -DWIN_CI=1 -DPORTABLE="$env:CMAKE_PORTABLE" -DSNAPPY=1 -DXPRESS=1 -DJNI=1 ..
if (!$?) { Exit $LASTEXITCODE }
cd ..
echo "Building with VS version: $env:CMAKE_GENERATOR"
# Use more parallel processes than available processors because most compile
# commands are expected to be cache hits.
cmake --build build --config Debug --parallel 32 -- /p:LinkIncremental=false /p:Platform=x64
if (!$?) { Exit $LASTEXITCODE }
echo ========================= Test RocksDB =========================
if ($env:ROCKSDB_CI_SUITE_RUN -ne "") {
  $suiteArray = $env:ROCKSDB_CI_SUITE_RUN -split ','
  build_tools\run_ci_db_test.ps1 -SuiteRun $suiteArray -Concurrency 16
  if (!$?) { Exit $LASTEXITCODE }
} else {
  echo "Skipping C++ tests (suite-run is empty)"
}
if ($env:ROCKSDB_CI_RUN_JAVA -eq "true") {
  echo ======================== Test RocksJava ========================
  cd build\java
  & ctest -C Debug -j 16
  if (!$?) { Exit $LASTEXITCODE }
} else {
  echo "Skipping Java tests"
}
