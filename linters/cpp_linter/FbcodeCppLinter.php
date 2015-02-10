<?php

class FbcodeCppLinter extends ArcanistLinter {
  const CPPLINT      = "/home/engshare/tools/cpplint";
  const LINT_ERROR   = 1;
  const LINT_WARNING = 2;
  const C_FLAG = "--c_mode=true";
  private $rawLintOutput = array();

  public function willLintPaths(array $paths) {
    $futures = array();
    $ret_value = 0;
    $last_line = system("which cpplint", $ret_value);
    $CPP_LINT = false;
    if ($ret_value == 0) {
      $CPP_LINT = $last_line;
    } else if (file_exists(self::CPPLINT)) {
      $CPP_LINT = self::CPPLINT;
    }

    if ($CPP_LINT) {
      foreach ($paths as $p) {
        $lpath = $this->getEngine()->getFilePathOnDisk($p);
        $lpath_file = file($lpath);
        if (preg_match('/\.(c)$/', $lpath) ||
            preg_match('/-\*-.*Mode: C[; ].*-\*-/', $lpath_file[0]) ||
            preg_match('/vim(:.*)*:\s*(set\s+)?filetype=c\s*:/', $lpath_file[0])
            ) {
          $futures[$p] = new ExecFuture("%s %s %s 2>&1",
                             $CPP_LINT, self::C_FLAG,
                             $this->getEngine()->getFilePathOnDisk($p));
        } else {
          $futures[$p] = new ExecFuture("%s %s 2>&1",
            $CPP_LINT, $this->getEngine()->getFilePathOnDisk($p));
        }
      }

      foreach (Futures($futures)->limit(8) as $p => $f) {
        $this->rawLintOutput[$p] = $f->resolvex();
      }
    }
    return;
  }

  public function getLinterName() {
    return "FBCPP";
  }

  public function lintPath($path) {
    $msgs = $this->getCppLintOutput($path);
    foreach ($msgs as $m) {
      $this->raiseLintAtLine($m['line'], 0, $m['severity'], $m['msg']);
    }
  }

  public function getLintSeverityMap() {
    return array(
      self::LINT_WARNING => ArcanistLintSeverity::SEVERITY_WARNING,
      self::LINT_ERROR   => ArcanistLintSeverity::SEVERITY_ERROR
    );
  }

  public function getLintNameMap() {
    return array(
      self::LINT_WARNING => "CppLint Warning",
      self::LINT_ERROR   => "CppLint Error"
    );
  }

  private function getCppLintOutput($path) {
    if (!array_key_exists($path, $this->rawLintOutput)) {
      return array();
    }
    list($output) = $this->rawLintOutput[$path];

    $msgs = array();
    $current = null;
    foreach (explode("\n", $output) as $line) {
      if (preg_match('/[^:]*\((\d+)\):(.*)$/', $line, $matches)) {
        if ($current) {
          $msgs[] = $current;
        }
        $line = $matches[1];
        $text = $matches[2];
        $sev  = preg_match('/.*Warning.*/', $text)
                  ? self::LINT_WARNING
                  : self::LINT_ERROR;
        $current = array('line'     => $line,
                         'msg'      => $text,
                         'severity' => $sev);
      } else if ($current) {
        $current['msg'] .= ' ' . $line;
      }
    }
    if ($current) {
      $msgs[] = $current;
    }

    return $msgs;
  }
}

