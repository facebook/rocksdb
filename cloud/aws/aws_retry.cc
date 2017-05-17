//  Copyright (c) 2017-present, Rockset, Inc.  All rights reserved.
//
//
#ifdef USE_AWS

#include "cloud/aws/aws_retry.h"

namespace rocksdb {

AwsRetryStrategy::~AwsRetryStrategy() {}

AwsRetryStrategy::AwsRetryStrategy(const CloudEnvOptions& env_options,
                                   std::shared_ptr<Logger> info_log)
    : env_options_(env_options), info_log_(info_log) {
  default_strategy_ = std::make_shared<Aws::Client::DefaultRetryStrategy>();
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[aws] Configured custom retry policy");
}

//
// Returns true if the error can be retried given the error and the number of
// times already tried.
//
bool AwsRetryStrategy::ShouldRetry(const AWSError<CoreErrors>& error,
                                   long attemptedRetries) const {
  CoreErrors ce = error.GetErrorType();

  // Internal errors are unknown errors and we try harder to fix them
  if (ce == CoreErrors::INTERNAL_FAILURE) {
    if (attemptedRetries <= internal_failure_num_retries_) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[aws] Encountered INTERNAL_FAILURE "
          " retry attempt %d is lesser than max retries %d. Retrying...",
          attemptedRetries, internal_failure_num_retries_);
      return true;
    }
    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[aws] Encountered INTERNAL_FAILURE "
        " retry attempt %d exceeds max retries %d. Aborting...",
        attemptedRetries, internal_failure_num_retries_);
    return false;
  }
  const Aws::String errmsg = error.GetMessage();
  std::string err(errmsg.c_str(), errmsg.size());
  Log(InfoLogLevel::WARN_LEVEL, info_log_,
      "[aws] Encountered S3 failure %s"
      " retry attempt %d max retries %d. "
      "Using default retry policy...",
      err.c_str(), attemptedRetries, internal_failure_num_retries_);
  return default_strategy_->ShouldRetry(error, attemptedRetries);
}

//
// Calculates the time in milliseconds the client should sleep before
// attempting another request based on the error and attemptedRetries count.
//
long AwsRetryStrategy::CalculateDelayBeforeNextRetry(
    const AWSError<CoreErrors>& error, long attemptedRetries) const {
  return default_strategy_->CalculateDelayBeforeNextRetry(error,
                                                          attemptedRetries);
}

}  // namespace

#endif /* USE_AWS */
