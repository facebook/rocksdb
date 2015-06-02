@echo off

REM Record the version of the source that we are compiling.
REM We keep a record of the git revision in util/version.cc. This source file
REM is then built as a regular source file as part of the compilation process.
REM One can run "strings executable_filename | grep _build_" to find the version of
REM the source that we used to build the executable file.

set CONFIGURATION=%1

pushd "%~dp0"
set "OUTFILE="..\util\build_version_%CONFIGURATION%.cc"

REM GIT_SHA=""
REM if command -v git >/dev/null 2>&1; then
REM     GIT_SHA=$(git rev-parse HEAD 2>/dev/null)
REM fi

@echo #include "build_version.h" > %OUTFILE%
@echo const char* rocksdb_build_git_sha = "rocksdb_build_git_sha:${GIT_SHA}"; >> %OUTFILE%
@echo const char* rocksdb_build_git_datetime = "rocksdb_build_git_datetime:$(date)"; >> %OUTFILE%
@echo const char* rocksdb_build_compile_date = __DATE__; >> %OUTFILE%

@popd
