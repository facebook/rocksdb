<?php

/**
 * Uses google's cpplint.py to check code. RocksDB team forked this file from
 * phabricator's /src/lint/linter/ArcanistCpplintLinter.php, and customized it
 * for its own use.
 *
 * You can get it here:
 * http://google-styleguide.googlecode.com/svn/trunk/cpplint/cpplint.py
 * @group linter
 */
final class ArcanistCpplintLinter extends ArcanistLinter {

  public function willLintPaths(array $paths) {
    return;
  }

  public function getLinterName() {
    return 'cpplint.py';
  }

  public function getLintOptions() {
    $config = $this->getEngine()->getConfigurationManager();
    $options = $config->getConfigFromAnySource('lint.cpplint.options', '');

    return $options;
  }

  public function getLintPath() {
    $config = $this->getEngine()->getConfigurationManager();
    $prefix = $config->getConfigFromAnySource('lint.cpplint.prefix');
    $bin = $config->getConfigFromAnySource('lint.cpplint.bin', 'cpplint.py');

    if ($prefix !== null) {
      if (!Filesystem::pathExists($prefix.'/'.$bin)) {
        throw new ArcanistUsageException(
          "Unable to find cpplint.py binary in a specified directory. Make ".
          "sure that 'lint.cpplint.prefix' and 'lint.cpplint.bin' keys are ".
          "set correctly. If you'd rather use a copy of cpplint installed ".
          "globally, you can just remove these keys from your .arcconfig.");
      }

      $bin = csprintf("%s/%s", $prefix, $bin);

      return $bin;
    }

    // Search under current dir
    list($err) = exec_manual('which %s/%s', $this->linterDir(), $bin);
    if (!$err) {
      return $this->linterDir().'/'.$bin;
    }

    // Look for globally installed cpplint.py
    list($err) = exec_manual('which %s', $bin);
    if ($err) {
      throw new ArcanistUsageException(
        "cpplint.py does not appear to be installed on this system. Install ".
        "it (e.g., with 'wget \"http://google-styleguide.googlecode.com/".
        "svn/trunk/cpplint/cpplint.py\"') or configure 'lint.cpplint.prefix' ".
        "in your .arcconfig to point to the directory where it resides. ".
        "Also don't forget to chmod a+x cpplint.py!");
    }

    return $bin;
  }

  public function lintPath($path) {
    $bin = $this->getLintPath();
    $options = $this->getLintOptions();
    $path = $this->rocksdbDir().'/'.$path;

    $f = new ExecFuture("%C %C $path", $bin, $options);

    list($err, $stdout, $stderr) = $f->resolve();

    if ($err === 2) {
      throw new Exception("cpplint failed to run correctly:\n".$stderr);
    }

    $lines = explode("\n", $stderr);
    $messages = array();
    foreach ($lines as $line) {
      $line = trim($line);
      $matches = null;
      $regex = '/^[^:]+:(\d+):\s*(.*)\s*\[(.*)\] \[(\d+)\]$/';
      if (!preg_match($regex, $line, $matches)) {
        continue;
      }
      foreach ($matches as $key => $match) {
        $matches[$key] = trim($match);
      }
      $message = new ArcanistLintMessage();
      $message->setPath($path);
      $message->setLine($matches[1]);
      $message->setCode($matches[3]);
      $message->setName($matches[3]);
      $message->setDescription($matches[2]);
      $message->setSeverity(ArcanistLintSeverity::SEVERITY_WARNING);
      $this->addLintMessage($message);
    }
  }

  // The path of this linter
  private function linterDir() {
    return dirname(__FILE__);
  }

  // TODO(kaili) a quick and dirty way to figure out rocksdb's root dir.
  private function rocksdbDir() {
    return $this->linterDir()."/../..";
  }
}
