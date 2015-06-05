<?php
// Copyright 2004-present Facebook. All Rights Reserved.

class FacebookFbcodeUnitTestEngine extends ArcanistBaseUnitTestEngine {

  public function run() {
    // Here we create a new unit test "jenkins_async_test" and promise we'll
    // update the results later.
    // Jenkins updates the results using `arc call-conduit
    // differential.updateunitresults` call. If you change the name here, also
    // make sure to change the name in Jenkins script that updates the test
    // result -- they have to be the same.
    $result = new ArcanistUnitTestResult();
    $result->setName("jenkins_async_test");
    $result->setResult(ArcanistUnitTestResult::RESULT_POSTPONED);
    return array($result);
  }
}
