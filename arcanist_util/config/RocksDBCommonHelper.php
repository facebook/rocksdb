<?php
// Copyright 2004-present Facebook. All Rights Reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

// Name of the environment variables which need to be set by the entity which
// triggers continuous runs so that code at the end of the file gets executed
// and Sandcastle run starts.
define("ENV_POST_RECEIVE_HOOK", "POST_RECEIVE_HOOK");
define("ENV_HTTPS_APP_VALUE", "HTTPS_APP_VALUE");
define("ENV_HTTPS_TOKEN_VALUE", "HTTPS_TOKEN_VALUE");

define("PRIMARY_TOKEN_FILE", '/home/krad/.sandcastle');
define("SECONDARY_TOKEN_FILE", '$HOME/.sandcastle');
define("CONT_RUN_ALIAS", "leveldb");

//////////////////////////////////////////////////////////////////////
/*  Run tests in sandcastle */
function postURL($diffID, $url) {
  assert(strlen($diffID) > 0);
  assert(is_numeric($diffID));
  assert(strlen($url) > 0);

  $cmd = 'echo \'{"diff_id": "' . $diffID . '", '
         . '"name":"click here for sandcastle tests for D' . $diffID . '", '
         . '"link":"' . $url . '"}\' | '
         . 'http_proxy=fwdproxy.any.facebook.com:8080 '
         . 'https_proxy=fwdproxy.any.facebook.com:8080 arc call-conduit '
         . 'differential.updateunitresults';
  shell_exec($cmd);
}

function buildUpdateTestStatusCmd($diffID, $test, $status) {
  assert(strlen($diffID) > 0);
  assert(is_numeric($diffID));
  assert(strlen($test) > 0);
  assert(strlen($status) > 0);

  $cmd = 'echo \'{"diff_id": "' . $diffID . '", '
         . '"name":"' . $test . '", '
         . '"result":"' . $status . '"}\' | '
         . 'http_proxy=fwdproxy.any.facebook.com:8080 '
         . 'https_proxy=fwdproxy.any.facebook.com:8080 arc call-conduit '
         . 'differential.updateunitresults';
  return $cmd;
}

function updateTestStatus($diffID, $test) {
  assert(strlen($diffID) > 0);
  assert(is_numeric($diffID));
  assert(strlen($test) > 0);

  shell_exec(buildUpdateTestStatusCmd($diffID, $test, "waiting"));
}

function getSteps($applyDiff, $diffID, $username, $test) {
  assert(strlen($username) > 0);
  assert(strlen($test) > 0);

  if ($applyDiff) {
    assert(strlen($diffID) > 0);
    assert(is_numeric($diffID));

    $arcrc_content = exec("cat ~/.arcrc | gzip -f | base64 -w0");
    assert(strlen($arcrc_content) > 0);

    // Sandcastle machines don't have arc setup. We copy the user certificate
    // and authenticate using that in Sandcastle.
    $setup = array(
      "name" => "Setup arcrc",
      "shell" => "echo " . $arcrc_content . " | base64 --decode"
                 . " | gzip -d > ~/.arcrc",
      "user" => "root"
    );

    // arc demands certain permission on its config.
    // also fix the sticky bit issue in sandcastle
    $fix_permission = array(
      "name" => "Fix environment",
      "shell" => "chmod 600 ~/.arcrc && chmod +t /dev/shm",
      "user" => "root"
    );

    // Construct the steps in the order of execution.
    $steps[] = $setup;
    $steps[] = $fix_permission;
  }

  // fbcode is a sub-repo. We cannot patch until we add it to ignore otherwise
  // Git thinks it is an uncommited change.
  $fix_git_ignore = array(
    "name" => "Fix git ignore",
    "shell" => "echo fbcode >> .git/info/exclude",
    "user" => "root"
  );

  $steps[] = $fix_git_ignore;

  // This will be the command used to execute particular type of tests.
  $cmd = "";

  if ($applyDiff) {
    // Patch the code (keep your fingures crossed).
    $patch = array(
      "name" => "Patch " . $diffID,
      "shell" => "HTTPS_PROXY=fwdproxy:8080 arc --arcrc-file ~/.arcrc "
                  . "patch --nocommit --diff " . $diffID,
      "user" => "root"
    );

    $steps[] = $patch;

    updateTestStatus($diffID, $test);
    $cmd = buildUpdateTestStatusCmd($diffID, $test, "running") . "; ";
  }

  // Run the actual command.
  $cmd = $cmd . "J=$(nproc) ./build_tools/precommit_checker.py " . $test
           . "; exit_code=$?; ";

  if ($applyDiff) {
    $cmd = $cmd . "([[ \$exit_code -eq 0 ]] &&"
                . buildUpdateTestStatusCmd($diffID, $test, "pass") . ")"
                . "||" . buildUpdateTestStatusCmd($diffID, $test, "fail")
                . "; ";
  }

  $cmd = $cmd . " cat /tmp/precommit-check.log"
           . "; for f in `ls t/log-*`; do echo \$f; cat \$f; done;"
           . "[[ \$exit_code -eq 0 ]]";
  assert(strlen($cmd) > 0);

  $run_test = array(
    "name" => "Run " . $test,
    "shell" => $cmd,
    "user" => "root",
    "parser" => "python build_tools/error_filter.py " . $test,
  );

  $steps[] = $run_test;

  if ($applyDiff) {
    // Clean up the user arc config we are using.
    $cleanup = array(
      "name" => "Arc cleanup",
      "shell" => "rm -f ~/.arcrc",
      "user" => "root"
    );

    $steps[] = $cleanup;
  }

  assert(count($steps) > 0);
  return $steps;
}

function getSandcastleConfig() {
  $sandcastle_config = array();

  // This is a case when we're executed from a continuous run. Fetch the values
  // from the environment.
  if (getenv(ENV_POST_RECEIVE_HOOK)) {
    $sandcastle_config[0] = getenv(ENV_HTTPS_APP_VALUE);
    $sandcastle_config[1] = getenv(ENV_HTTPS_TOKEN_VALUE);
  } else {
    // This is a typical `[p]arc diff` case. Fetch the values from the specific
    // configuration files.
    assert(file_exists(PRIMARY_TOKEN_FILE) ||
           file_exists(SECONDARY_TOKEN_FILE));

    // Try the primary location first, followed by a secondary.
    if (file_exists(PRIMARY_TOKEN_FILE)) {
      $cmd = 'cat ' . PRIMARY_TOKEN_FILE;
    } else {
      $cmd = 'cat ' . SECONDARY_TOKEN_FILE;
    }

    assert(strlen($cmd) > 0);
    $sandcastle_config = explode(':', rtrim(shell_exec($cmd)));
  }

  // In this case be very explicit about the implications.
  if (count($sandcastle_config) != 2) {
    echo "Sandcastle configuration files don't contain valid information " .
         "or the necessary environment variables aren't defined. Unable " .
         "to validate the code changes.";
    exit(1);
  }

  assert(strlen($sandcastle_config[0]) > 0);
  assert(strlen($sandcastle_config[1]) > 0);
  assert(count($sandcastle_config) > 0);

  return $sandcastle_config;
}

// This function can be called either from `[p]arc diff` command or during
// the Git post-receive hook.
 function startTestsInSandcastle($applyDiff, $workflow, $diffID) {
  // Default options don't terminate on failure, but that's what we want. In
  // the current case we use assertions intentionally as "terminate on failure
  // invariants".
  assert_options(ASSERT_BAIL, true);

  // In case of a diff we'll send notificatios to the author. Else it'll go to
  // the entire team because failures indicate that build quality has regressed.
  $username = $applyDiff ? exec("whoami") : CONT_RUN_ALIAS;
  assert(strlen($username) > 0);

  if ($applyDiff) {
    assert($workflow);
    assert(strlen($diffID) > 0);
    assert(is_numeric($diffID));
  }

  if (strcmp(getenv("ROCKSDB_CHECK_ALL"), 1) == 0) {
    // Extract all tests from the CI definition.
    $output = file_get_contents("build_tools/rocksdb-lego-determinator");
    assert(strlen($output) > 0);

    preg_match_all('/[ ]{2}([a-zA-Z0-9_]+)[\)]{1}/', $output, $matches);
    $tests = $matches[1];
    assert(count($tests) > 0);
  } else {
    // Manually list of tests we want to run in Sandcastle.
    $tests = array(
      "unit", "unit_non_shm", "unit_481", "clang_unit", "tsan", "asan",
      "lite_test", "valgrind", "release", "release_481", "clang_release"
    );
  }

  $send_email_template = array(
    'type' => 'email',
    'triggers' => array('fail'),
    'emails' => array($username . '@fb.com'),
  );

  // Construct a job definition for each test and add it to the master plan.
  foreach ($tests as $test) {
    $stepName = "RocksDB diff " . $diffID . " test " . $test;

    if (!$applyDiff) {
      $stepName = "RocksDB continuous integration test " . $test;
    }

    $arg[] = array(
      "name" => $stepName,
      "report" => array($send_email_template),
      "steps" => getSteps($applyDiff, $diffID, $username, $test)
    );
  }

  // We cannot submit the parallel execution master plan to Sandcastle and
  // need supply the job plan as a determinator. So we construct a small job
  // that will spit out the master job plan which Sandcastle will parse and
  // execute. Why compress the job definitions? Otherwise we run over the max
  // string size.
  $cmd = "echo " . base64_encode(json_encode($arg))
         . " | gzip -f | base64 -w0";
  assert(strlen($cmd) > 0);

  $arg_encoded = shell_exec($cmd);
  assert(strlen($arg_encoded) > 0);

  $runName = "Run diff " . $diffID . "for user " . $username;

  if (!$applyDiff) {
    $runName = "RocksDB continuous integration build and test run";
  }

  $command = array(
    "name" => $runName,
    "steps" => array()
  );

  $command["steps"][] = array(
    "name" => "Generate determinator",
    "shell" => "echo " . $arg_encoded . " | base64 --decode | gzip -d"
               . " | base64 --decode",
    "determinator" => true,
    "user" => "root"
  );

  // Submit to Sandcastle.
  $url = 'https://interngraph.intern.facebook.com/sandcastle/generate?'
          .'command=SandcastleUniversalCommand'
          .'&vcs=rocksdb-git&revision=origin%2Fmaster&type=lego'
          .'&user=' . $username . '&alias=rocksdb-precommit'
          .'&command-args=' . urlencode(json_encode($command));

  // Fetch the configuration necessary to submit a successful HTTPS request.
  $sandcastle_config = getSandcastleConfig();

  $app = $sandcastle_config[0];
  $token = $sandcastle_config[1];

  $cmd = 'https_proxy= HTTPS_PROXY= curl -s -k -F app=' . $app . ' '
          . '-F token=' . $token . ' "' . $url . '"';

  $output = shell_exec($cmd);
  assert(strlen($output) > 0);

  // Extract Sandcastle URL from the response.
  preg_match('/url": "(.+)"/', $output, $sandcastle_url);

  assert(count($sandcastle_url) > 0, "Unable to submit Sandcastle request.");
  assert(strlen($sandcastle_url[1]) > 0, "Unable to extract Sandcastle URL.");

  if ($applyDiff) {
    echo "\nSandcastle URL: " . $sandcastle_url[1] . "\n";
    // Ask Phabricator to display it on the diff UI.
    postURL($diffID, $sandcastle_url[1]);
  } else {
    echo "Continuous integration started Sandcastle tests. You can look at ";
    echo "the progress at:\n" . $sandcastle_url[1] . "\n";
  }
}

// Continuous run cript will set the environment variable and based on that
// we'll trigger the execution of tests in Sandcastle. In that case we don't
// need to apply any diffs and there's no associated workflow either.
if (getenv(ENV_POST_RECEIVE_HOOK)) {
  startTestsInSandcastle(
    false /* $applyDiff */,
    NULL /* $workflow */,
    NULL /* $diffID */);
}
