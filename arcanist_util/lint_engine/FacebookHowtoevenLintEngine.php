<?php
// Copyright 2015-present Facebook. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

final class FacebookHowtoevenLintEngine extends ArcanistLintEngine {

  public function buildLinters() {
    $paths = array();

    foreach ($this->getPaths() as $path) {
      // Don't try to lint deleted files or changed directories.
      if (!Filesystem::pathExists($path) || is_dir($path)) {
        continue;
      }

      if (preg_match('/\.(cpp|c|cc|cxx|h|hh|hpp|hxx|tcc)$/', $path)) {
        $paths[] = $path;
      }
    }

    $howtoeven = new FacebookHowtoevenLinter();
    $howtoeven->setPaths($paths);
    return array($howtoeven);
  }
}
