<?php
// Copyright 2004-present Facebook. All Rights Reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

class FacebookArcanistConfiguration extends ArcanistConfiguration {

  public function didRunWorkflow($command,
                                 ArcanistBaseWorkflow $workflow,
                                 $error_code) {
    if ($command == 'diff' && !$workflow->isRawDiffSource()) {
      $this->startTestsInJenkins($workflow);
      $this->startTestsInSandcastle($workflow);
    }
  }

  //////////////////////////////////////////////////////////////////////
  /*  Run tests in sandcastle */
  function getSteps($diffID, $username) {
    $arcrc_content = exec("cat ~/.arcrc | base64 -w0");

    $setup = array(
      "name" => "Setup arcrc",
      "shell" => "echo " . $arcrc_content . " | base64 --decode > ~/.arcrc",
      "user" => "root"
    );

    $fix_permission = array(
      "name" => "Fix environment",
      "shell" => "chmod 600 ~/.arcrc",
      "user" => "root"
    );

    $fix_git_ignore = array(
      "name" => "Fix git ignore",
      "shell" => "echo fbcode >> .git/info/exclude",
      "user" => "root"
    );

    $patch = array(
      "name" => "Patch " . $diffID,
      "shell" => "HTTPS_PROXY=fwdproxy:8080 arc --arcrc-file ~/.arcrc "
                  . "patch D" . $diffID . " || rm -f ~/.arcrc",
      "user" => "root"
    );

    $cleanup = array(
      "name" => "Arc cleanup",
      "shell" => "rm -f ~/.arcrc",
      "user" => "root"
    );

    $steps[] = $setup;
    $steps[] = $fix_permission;
    $steps[] = $fix_git_ignore;
    $steps[] = $patch;
    $steps[] = $cleanup;

    $tests = array(
      "unit", "clang_unit", "tsan", "asan", "valgrind"
    );

    foreach ($tests as $test) {
      $run_test = array(
        "name" => "Run " . $test,
        "shell" => "EMAIL=" . $username . "@fb.com "
                    . "./build_tools/rocksdb-lego-determinator " . $test,
        "user" => "root",
        "determinator" => true
      );

      $steps[] = $run_test;
    }

    return $steps;
  }

  function startTestsInSandcastle($workflow) {
    $diffID = $workflow->getDiffId();
    $username = exec("whoami");

    if ($diffID == null || $username == null) {
      return;
    }

    $arg = array(
      "name" => "RocksDB diff D" . $diffID . "testing for " . $username,
      "steps" => $this->getSteps($diffID, $username)
    );

    $url = 'https://interngraph.intern.facebook.com/sandcastle/generate?'
            .'command=SandcastleUniversalCommand'
            .'&vcs=rocksdb-git&revision=origin%2Fmaster&type=lego'
            .'&user=krad&alias=ci-util'
            .'&command-args=' . urlencode(json_encode($arg));

    $cmd = 'https_proxy= HTTPS_PROXY= curl -s -k -F app=659387027470559 '
            . '-F token=AeO_3f2Ya3TujjnxGD4 "' . $url . '"';

    echo "\n====================================================== \n";
    echo "Scheduling sandcastle job for D" . $diffID . " for " . $username;
    echo "\n";
    echo "Please follow the URL for details on the job. \n";
    echo "An email will be sent to " . $username . "@fb.com on failure. \n";
    echo "\n";
    echo "Job details: \n";

    $output = shell_exec($cmd);

    echo $output;

    echo "\n====================================================== \n";
  }

  //////////////////////////////////////////////////////////////////////
  /* Send off builds to jenkins */
  function startTestsInJenkins($workflow) {
    $diffID = $workflow->getDiffID();
    if ($diffID === null) {
      return;
    }

    $results = $workflow->getTestResults();
    if (!$results) {
      return;
    }

    $url = "https://ci-builds.fb.com/view/rocksdb/job/rocksdb_diff_check/"
               ."buildWithParameters?token=AUTH&DIFF_ID=$diffID";
    system("curl --noproxy '*' \"$url\" > /dev/null 2>&1");
  }

}
