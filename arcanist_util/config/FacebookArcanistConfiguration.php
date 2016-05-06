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
  function postURL($diffID, $url) {
    $cmd = 'echo \'{"diff_id": "' . $diffID . '", '
           . '"name":"click here for sandcastle tests for D' . $diffID . '", '
           . '"link":"' . $url . '"}\' | '
           . 'http_proxy=fwdproxy.any.facebook.com:8080 '
           . 'https_proxy=fwdproxy.any.facebook.com:8080 arc call-conduit '
           . 'differential.updateunitresults';
    shell_exec($cmd);
  }

  function updateTestCommand($diffID, $test, $status) {
    $cmd = 'echo \'{"diff_id": "' . $diffID . '", '
           . '"name":"' . $test . '", '
           . '"result":"' . $status . '"}\' | '
           . 'http_proxy=fwdproxy.any.facebook.com:8080 '
           . 'https_proxy=fwdproxy.any.facebook.com:8080 arc call-conduit '
           . 'differential.updateunitresults';
    return $cmd;
  }

  function updateTest($diffID, $test) {
    shell_exec($this->updateTestCommand($diffID, $test, "waiting"));
  }

  function getSteps($diffID, $username, $test) {
    $arcrc_content = exec("cat ~/.arcrc | gzip -f | base64 -w0");

    // Sandcastle machines don't have arc setup. We copy the user certificate
    // and authenticate using that in sandcastle
    $setup = array(
      "name" => "Setup arcrc",
      "shell" => "echo " . $arcrc_content . " | base64 --decode"
                 . " | gzip -d > ~/.arcrc",
      "user" => "root"
    );

    // arc demands certain permission on its config
    $fix_permission = array(
      "name" => "Fix environment",
      "shell" => "chmod 600 ~/.arcrc",
      "user" => "root"
    );

    // fbcode is a sub-repo. We cannot patch until we add it to ignore otherwise
    // git thinks it is uncommited change
    $fix_git_ignore = array(
      "name" => "Fix git ignore",
      "shell" => "echo fbcode >> .git/info/exclude",
      "user" => "root"
    );

    // Patch the code (keep your fingures crossed)
    $patch = array(
      "name" => "Patch " . $diffID,
      "shell" => "HTTPS_PROXY=fwdproxy:8080 arc --arcrc-file ~/.arcrc "
                  . "patch --diff " . $diffID,
      "user" => "root"
    );

    // Clean up the user arc config we are using
    $cleanup = array(
      "name" => "Arc cleanup",
      "shell" => "rm -f ~/.arcrc",
      "user" => "root"
    );

    // Construct the steps in the order of execution
    $steps[] = $setup;
    $steps[] = $fix_permission;
    $steps[] = $fix_git_ignore;
    $steps[] = $patch;

    // Run the actual command
    $this->updateTest($diffID, $test);
    $cmd = $this->updateTestCommand($diffID, $test, "running") . ";"
           . "./build_tools/precommit_checker.py " . $test
           . "; exit_code=$?; ([[ \$exit_code -eq 0 ]] &&"
           . $this->updateTestCommand($diffID, $test, "pass") . ")"
           . "||" . $this->updateTestCommand($diffID, $test, "fail")
           . "; cat /tmp/precommit-check.log"
           . "; for f in `ls t/log-*`; do echo \$f; cat \$f; done;"
           . "[[ \$exit_code -eq 0 ]]";

    $run_test = array(
      "name" => "Run " . $test,
      "shell" => $cmd,
      "user" => "root",
    );

    $steps[] = $run_test;
    $steps[] = $cleanup;

    return $steps;
  }

  function startTestsInSandcastle($workflow) {
    // extract information we need from workflow or CLI
    $diffID = $workflow->getDiffId();
    $username = exec("whoami");

    if ($diffID == null || $username == null) {
      // there is no diff and we can't extract username
      // we cannot schedule sandcasstle job
      return;
    }

    if (strcmp(getenv("ROCKSDB_CHECK_ALL"), 1) == 0) {
      // extract all tests from the CI definition
      $output = file_get_contents("build_tools/rocksdb-lego-determinator");
      preg_match_all('/[ ]{2}([a-zA-Z0-9_]+)[\)]{1}/', $output, $matches);
      $tests = $matches[1];
    } else {
      // manually list of tests we want to run in sandcastle
      $tests = array(
        "unit", "unit_481", "clang_unit", "tsan", "asan", "lite_test", "valgrind"
      );
    }

    // construct a job definition for each test and add it to the master plan
    foreach ($tests as $test) {
      $arg[] = array(
        "name" => "RocksDB diff " . $diffID . " test " . $test,
        "steps" => $this->getSteps($diffID, $username, $test)
      );
    }

    // we cannot submit the parallel execution master plan to sandcastle
    // we need supply the job plan as a determinator
    // so we construct a small job that will spit out the master job plan
    // which sandcastle will parse and execute
    // Why compress ? Otherwise we run over the max string size.
    $cmd = "echo " . base64_encode(json_encode($arg))
           . " | gzip -f | base64 -w0";
    $arg_encoded = shell_exec($cmd);

    $command = array(
      "name" => "Run diff " . $diffID . "for user " . $username,
      "steps" => array()
    );

    $command["steps"][] = array(
      "name" => "Generate determinator",
      "shell" => "echo " . $arg_encoded . " | base64 --decode | gzip -d"
                 . " | base64 --decode",
      "determinator" => true,
      "user" => "root"
    );

    // submit to sandcastle
    $url = 'https://interngraph.intern.facebook.com/sandcastle/generate?'
            .'command=SandcastleUniversalCommand'
            .'&vcs=rocksdb-git&revision=origin%2Fmaster&type=lego'
            .'&user=krad&alias=rocksdb-precommit'
            .'&command-args=' . urlencode(json_encode($command));

    $cmd = 'https_proxy= HTTPS_PROXY= curl -s -k -F app=659387027470559 '
            . '-F token=AeO_3f2Ya3TujjnxGD4 "' . $url . '"';

    $output = shell_exec($cmd);

    // extract sandcastle URL from the response
    preg_match('/url": "(.+)"/', $output, $sandcastle_url);

    echo "\nSandcastle URL: " . $sandcastle_url[1] . "\n";

    // Ask phabricator to display it on the diff UI
    $this->postURL($diffID, $sandcastle_url[1]);
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
