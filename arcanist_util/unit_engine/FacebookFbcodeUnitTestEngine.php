<?php
// Copyright 2004-present Facebook. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

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
