#pragma once
namespace rocksdb {

    inline Status CreateWal(bool is_open, std::string log_fname, rocksdb::Env * env,
                        uint64_t recycle_log_number,
                        std::unique_ptr<WritableFile>& lfile,
                        ImmutableDBOptions immutable_db_options,
                        EnvOptions opt_env_opt) {
        Status s;
        if (is_open) {
          s = NewWritableFile(env, log_fname, &lfile, opt_env_opt);
        }
        else {
          if (recycle_log_number) {
            ROCKS_LOG_INFO(immutable_db_options.info_log,
                         "reusing log %" PRIu64 " from recycle list\n",
                         recycle_log_number);
            std::string old_log_fname =
              LogFileName(immutable_db_options.wal_dir, recycle_log_number);
          s = env->ReuseWritableFile(log_fname, old_log_fname, &lfile,
                                      opt_env_opt);
          }
          else {
              s = NewWritableFile(env, log_fname, &lfile, opt_env_opt);
          }
        }
      return s;
    }

    inline log::Writer* CreateLogWriter(std::unique_ptr<WritableFile>& lfile,
                                std::string log_fname, EnvOptions opt_env_opt,
                                rocksdb::Env * env,
                                ImmutableDBOptions immutable_db_options,
                                uint64_t new_log_number, bool manual_flush) {
    std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(lfile), log_fname, opt_env_opt, env, nullptr /* stats */,
        immutable_db_options.listeners));
    return new log::Writer(std::move(file_writer), new_log_number,
        immutable_db_options.recycle_log_file_num > 0, manual_flush);
}
}
