<?php
// Copyright 2004-present Facebook. All Rights Reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

class FacebookFbcodeUnitTestEngine extends ArcanistUnitTestEngine {

  public function run() {
      // For a call to `arc call-conduit differential.updateunitresults` to
      // succeed we need at least one entry here.
      $result = new ArcanistUnitTestResult();
      $result->setName("dummy_placeholder_entry");
      $result->setResult(ArcanistUnitTestResult::RESULT_PASS);
      return array($result);
  }
}
