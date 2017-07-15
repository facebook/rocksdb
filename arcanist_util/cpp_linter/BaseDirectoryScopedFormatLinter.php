<?php
// Copyright 2004-present Facebook. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

abstract class BaseDirectoryScopedFormatLinter extends ArcanistLinter {

  const LINT_FORMATTING = 1;

  private $changedLines = array();
  private $rawLintOutput = array();

  abstract protected function getPathsToLint();

  protected function shouldLintPath($path) {
    foreach ($this->getPathsToLint() as $p) {
      // check if $path starts with $p
      if (strncmp($path, $p, strlen($p)) === 0) {
        return true;
      }
    }
    return false;
  }

  // API to tell this linter which lines were changed
  final public function setPathChangedLines($path, $changed) {
    $this->changedLines[$path] = $changed;
  }

  final public function willLintPaths(array $paths) {
    $futures = array();
    foreach ($paths as $path) {
      if (!$this->shouldLintPath($path)) {
        continue;
      }

      $changed = $this->changedLines[$path];
      if (!isset($changed)) {
        // do not run linter if there are no changes
        continue;
      }

      $futures[$path] = $this->getFormatFuture($path, $changed);
    }

    foreach (id(new FutureIterator($futures))->limit(8) as $p => $f) {
      $this->rawLintOutput[$p] = $f->resolvex();
    }
  }

  abstract protected function getFormatFuture($path, array $changed);
  abstract protected function getLintMessage($diff);

  final public function lintPath($path) {
    if (!isset($this->rawLintOutput[$path])) {
      return;
    }

    list($new_content) = $this->rawLintOutput[$path];
    $old_content = $this->getData($path);

    if ($new_content != $old_content) {
      $diff = ArcanistDiffUtils::renderDifferences($old_content, $new_content);
      $this->raiseLintAtOffset(
        0,
        self::LINT_FORMATTING,
        $this->getLintMessage($diff),
        $old_content,
        $new_content);
    }
  }

}
