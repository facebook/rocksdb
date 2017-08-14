// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb.test;

import org.junit.internal.JUnitSystem;
import org.junit.internal.RealSystem;
import org.junit.internal.TextListener;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import java.util.ArrayList;
import java.util.List;

/**
 * Custom Junit Runner to print also Test classes
 * and executed methods to command prompt.
 */
public class RocksJunitRunner {

  /**
   * Listener which overrides default functionality
   * to print class and method to system out.
   */
  static class RocksJunitListener extends TextListener {

    /**
     * RocksJunitListener constructor
     *
     * @param system JUnitSystem
     */
    public RocksJunitListener(final JUnitSystem system) {
      super(system);
    }

    @Override
    public void testStarted(final Description description) {
       System.out.format("Run: %s testing now -> %s \n",
           description.getClassName(),
           description.getMethodName());
    }
  }

  /**
   * Main method to execute tests
   *
   * @param args Test classes as String names
   */
  public static void main(final String[] args){
    final JUnitCore runner = new JUnitCore();
    final JUnitSystem system = new RealSystem();
    runner.addListener(new RocksJunitListener(system));
    try {
      final List<Class<?>> classes = new ArrayList<>();
      for (final String arg : args) {
        classes.add(Class.forName(arg));
      }
      final Class[] clazzes = classes.toArray(new Class[classes.size()]);
      final Result result = runner.run(clazzes);
      if(!result.wasSuccessful()) {
        System.exit(-1);
      }
    } catch (final ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(-2);
    }
  }
}
