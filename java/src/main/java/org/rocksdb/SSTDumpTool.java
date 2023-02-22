// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Map;

public class SSTDumpTool extends RocksObject {

  public SSTDumpTool() {
    super(0);
  }

  public void run(String[] args, Options options) {
    this.runInternal(args, options.getNativeHandle());
  }

  public void run(Map<String, String> args, Options options) {
    String[] argsList = new String[args.size()];
    int idx = 0;
    for(Map.Entry<String, String> entry: args.entrySet()) {
      argsList[idx] = "--" + (entry.getValue()== null || entry.getValue().isEmpty() ? entry.getKey() :
              (entry.getKey() + "="+entry.getValue()));
      idx += 1;
    }
    this.run(argsList, options);
  }

  private native void runInternal(String[] args, long optionsNativeHandle);

  @Override
  protected void disposeInternal(long handle) {

  }
}
