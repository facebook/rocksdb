//  Copyright (c) 2017-present, Rockset, Inc.  All rights reserved.
//
//

#include <cinttypes>

#include "cloud/aws/aws_file.h"
#include "rocksdb/cloud/cloud_env_options.h"
#ifdef USE_AWS
#include <aws/core/client/AWSError.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/SpecifiedRetryableErrorsRetryStrategy.h>
#include <aws/core/client/RetryStrategy.h>
#endif  // USE_AWS

namespace ROCKSDB_NAMESPACE {
#ifdef USE_AWS
//
// Ability to configure retry policies for the AWS client
//
class AwsRetryStrategy : public Aws::Client::RetryStrategy {
 public:
  AwsRetryStrategy(CloudEnv* env) : env_(env) {
    // In many environments, AccessDenied and ExpiredToken errors are retryable.
    // This is because HTTP requests are involved in fetching the new tokens and
    // credentials, which can fail.
    Aws::Vector<Aws::String> retryableErrors;
    retryableErrors.push_back("AccessDenied");
    retryableErrors.push_back("ExpiredToken");
    retryableErrors.push_back("InternalError");
    default_strategy_ =
        std::make_shared<Aws::Client::SpecifiedRetryableErrorsRetryStrategy>(
            retryableErrors);
    Log(InfoLogLevel::INFO_LEVEL, env_->GetLogger(),
        "[aws] Configured custom retry policy");
  }

  ~AwsRetryStrategy() override {}

  // Returns true if the error can be retried given the error and the number of
  // times already tried.
  bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
                   long attemptedRetries) const override;

  // Calculates the time in milliseconds the client should sleep before
  // attempting another request based on the error and attemptedRetries count.
  long CalculateDelayBeforeNextRetry(
      const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
      long attemptedRetries) const override;

 private:
  // rocksdb retries, etc
  CloudEnv* env_;

  // The default strategy implemented by AWS client
  std::shared_ptr<Aws::Client::RetryStrategy> default_strategy_;

  // The number of times an internal-error failure should be retried
  const int internal_failure_num_retries_{10};
};

//
// Returns true if the error can be retried given the error and the number of
// times already tried.
//
bool AwsRetryStrategy::ShouldRetry(
    const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
    long attemptedRetries) const {
  auto ce = error.GetErrorType();
  const Aws::String errmsg = error.GetMessage();
  const Aws::String exceptionMsg = error.GetExceptionName();
  std::string err(errmsg.c_str(), errmsg.size());
  std::string emsg(exceptionMsg.c_str(), exceptionMsg.size());

  // Internal errors are unknown errors and we try harder to fix them
  //
  if (ce == Aws::Client::CoreErrors::INTERNAL_FAILURE ||
      ce == Aws::Client::CoreErrors::UNKNOWN ||
      err.find("try again") != std::string::npos) {
    if (attemptedRetries <= internal_failure_num_retries_) {
      Log(InfoLogLevel::INFO_LEVEL, env_->GetLogger(),
          "[aws] Encountered retriable failure: %s (code %d, http %d). "
          "Exception %s. retry attempt %ld is lesser than max retries %d. "
          "Retrying...",
          err.c_str(), static_cast<int>(ce),
          static_cast<int>(error.GetResponseCode()), emsg.c_str(),
          attemptedRetries, internal_failure_num_retries_);
      return true;
    }
    Log(InfoLogLevel::INFO_LEVEL, env_->GetLogger(),
        "[aws] Encountered retriable failure: %s (code %d, http %d). Exception "
        "%s. retry attempt %ld exceeds max retries %d. Aborting...",
        err.c_str(), static_cast<int>(ce),
        static_cast<int>(error.GetResponseCode()), emsg.c_str(),
        attemptedRetries, internal_failure_num_retries_);
    return false;
  }
  Log(InfoLogLevel::WARN_LEVEL, env_->GetLogger(),
      "[aws] Encountered S3 failure %s (code %d, http %d). Exception %s."
      " retry attempt %ld max retries %d. Using default retry policy...",
      err.c_str(), static_cast<int>(ce),
      static_cast<int>(error.GetResponseCode()), emsg.c_str(), attemptedRetries,
      internal_failure_num_retries_);
  return default_strategy_->ShouldRetry(error, attemptedRetries);
}

//
// Calculates the time in milliseconds the client should sleep before
// attempting another request based on the error and attemptedRetries count.
//
long AwsRetryStrategy::CalculateDelayBeforeNextRetry(
    const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
    long attemptedRetries) const {
  return default_strategy_->CalculateDelayBeforeNextRetry(error,
                                                          attemptedRetries);
}

Status AwsCloudOptions::GetClientConfiguration(
    CloudEnv* env, const std::string& region,
    Aws::Client::ClientConfiguration* config) {
  config->connectTimeoutMs = 30000;
  config->requestTimeoutMs = 600000;

  const auto& cloud_env_options = env->GetCloudEnvOptions();
  // Setup how retries need to be done
  config->retryStrategy = std::make_shared<AwsRetryStrategy>(env);
  if (cloud_env_options.request_timeout_ms != 0) {
    config->requestTimeoutMs = cloud_env_options.request_timeout_ms;
  }

  config->region = ToAwsString(region);
  return Status::OK();
}
#else
Status AwsCloudOptions::GetClientConfiguration(
    CloudEnv*, const std::string&, Aws::Client::ClientConfiguration*) {
  return Status::NotSupported("Not configured for AWS support");
}
#endif /* USE_AWS */

}  // namespace ROCKSDB_NAMESPACE
