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
                  . "patch --diff " . $diffID,
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

    $this->updateTest($diffID, $test);
    $cmd = $this->updateTestCommand($diffID, $test, "running") . ";"
           . "(./build_tools/precommit_checker.py " . $test
           . "&& "
           . $this->updateTestCommand($diffID, $test, "pass") . ")"
           . "|| " . $this->updateTestCommand($diffID, $test, "fail")
           . "; cat /tmp/precommit-check.log"
           . "; for f in `ls t/log-*`; do echo \$f; cat \$f; done";

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
    $diffID = $workflow->getDiffId();
    $username = exec("whoami");

    if ($diffID == null || $username == null) {
      return;
    }

    $tests = array(
      "unit", "unit_481", "clang_unit", "tsan", "asan", "lite"
    );

    foreach ($tests as $test) {
      $arg[] = array(
        "name" => "RocksDB diff " . $diffID . " test " . $test,
        "steps" => $this->getSteps($diffID, $username, $test)
      );
    }

    $arg_encoded = base64_encode(json_encode($arg));

    $command = array(
      "name" => "Run diff " . $diffID . "for user " . $username,
      "steps" => array()
    );

    $command["steps"][] = array(
      "name" => "Generate determinator",
      "shell" => "echo " . $arg_encoded . " | base64 --decode",
      "determinator" => true,
      "user" => "root"
    );

    $url = 'https://interngraph.intern.facebook.com/sandcastle/generate?'
            .'command=SandcastleUniversalCommand'
            .'&vcs=rocksdb-git&revision=origin%2Fmaster&type=lego'
            .'&user=krad&alias=rocksdb-precommit'
            .'&command-args=' . urlencode(json_encode($command));

    $cmd = 'https_proxy= HTTPS_PROXY= curl -s -k -F app=659387027470559 '
            . '-F token=AeO_3f2Ya3TujjnxGD4 "' . $url . '"';

    echo "\n====================================================== \n";
    echo "Scheduling sandcastle job for D" . $diffID . " for " . $username;
    echo "\n";
    echo "Please follow the URL for details on the job. \n";
    echo "An email will be sent to " . $username . "@fb.com on failure. \n";
    echo "\n";

    $output = shell_exec($cmd);

    preg_match('/url": "(.+)"/', $output, $sandcastle_url);

    echo "url: " . $sandcastle_url[1] . "\n";

    $this->postURL($diffID, $sandcastle_url[1]);

    echo "====================================================== \n";
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
