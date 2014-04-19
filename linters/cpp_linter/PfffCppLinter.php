<?php
// Copyright 2004-present Facebook.  All rights reserved.

class PfffCppLinter extends ArcanistLinter {
  const PROGRAM      = "/home/engshare/tools/checkCpp";

  public function getLinterName() {
    return "checkCpp";
  }
  public function getLintNameMap() {
    return array(
    );
  }

  public function getLintSeverityMap() {
    return array(
    );
  }

  public function willLintPaths(array $paths) {
    $program = false;
    $ret_value = 0;
    $last_line = system("which checkCpp", $ret_value);
    if ($ret_value == 0) {
      $program = $last_line;
    } else if (file_exists(self::PROGRAM)) {
      $program = self::PROGRAM;
    }
    if ($program) {
      $futures = array();
      foreach ($paths as $p) {
        $futures[$p] = new ExecFuture("%s --lint %s 2>&1",
          $program, $this->getEngine()->getFilePathOnDisk($p));
      }
      foreach (Futures($futures)->limit(8) as $p => $f) {

        list($stdout, $stderr) = $f->resolvex();
        $raw = json_decode($stdout, true);
        if (!is_array($raw)) {
          throw new Exception(
            "checkCpp returned invalid JSON!".
            "Stdout: {$stdout} Stderr: {$stderr}"
          );
        }
        foreach($raw as $err) {
          $this->addLintMessage(
            ArcanistLintMessage::newFromDictionary(
              array(
                'path' => $err['file'],
                'line' => $err['line'],
                'char' => 0,
                'name' => $err['name'],
                'description' => $err['info'],
                'code' => $this->getLinterName(),
                'severity' => ArcanistLintSeverity::SEVERITY_WARNING,
              )
            )
          );
        }
      }
    }
    return;
  }

  public function lintPath($path) {
    return;
  }
}
