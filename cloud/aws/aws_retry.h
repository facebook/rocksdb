//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//
#pragma once
#ifdef USE_AWS

#include "cloud/aws/aws_env.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "util/stderr_logger.h"

#include <aws/core/Aws.h>
#include <aws/core/client/AWSError.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/client/RetryStrategy.h>

using Aws::Client::AWSError;
using Aws::Client::CoreErrors;

namespace rocksdb {
//
// Ability to configure retry policies for the AWS client
//
class AwsRetryStrategy : public Aws::Client::RetryStrategy {
 public:
  AwsRetryStrategy(const CloudEnvOptions& env_options,
                   std::shared_ptr<Logger> info_log);
  virtual ~AwsRetryStrategy();

  // Returns true if the error can be retried given the error and the number of
  // times already tried.
  virtual bool ShouldRetry(const AWSError<CoreErrors>& error,
                           long attemptedRetries) const override;

  // Calculates the time in milliseconds the client should sleep before
  // attempting another request based on the error and attemptedRetries count.
  virtual long CalculateDelayBeforeNextRetry(
      const AWSError<CoreErrors>& error, long attemptedRetries) const override;

 private:
  // rocksdb retries, etc
  CloudEnvOptions env_options_;

  // The info logger
  std::shared_ptr<Logger> info_log_;

  // The default strategy implemented by AWS client
  std::shared_ptr<Aws::Client::RetryStrategy> default_strategy_;

  // The numbe of times an internal-error failure should be retried
  const int internal_failure_num_retries_{25};
};

}  // namepace rocksdb

#endif /* USE_AWS */
