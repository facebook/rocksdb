@echo off
REM Helper for configuration
REM Assumes third parties are set 

cmake -G "Visual Studio 14 2015 Win64" -DGFLAGS=1 -DSNAPPYDLL=1 -DXPRESS=1 -DJEMALLOC=0 -DWITH_AVX2=0 ..