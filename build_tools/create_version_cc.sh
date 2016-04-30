BUILD_VERSION_CC=$1;
shift;

GIT_SHA=$(git rev-parse HEAD 2>/dev/null)
GIT_DATE_TIME=$(date)

echo "#include \"build_version.h\"" > ${BUILD_VERSION_CC}
echo "const char* rocksdb_build_git_sha = \"${GIT_SHA}\";" >> ${BUILD_VERSION_CC}
echo "const char* rocksdb_build_git_datetime = \"${GIT_DATE_TIME}\";" >> ${BUILD_VERSION_CC}
echo "const char* rocksdb_build_compile_date = __DATE__;" >> ${BUILD_VERSION_CC}
