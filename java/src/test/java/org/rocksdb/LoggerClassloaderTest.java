package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Only this class can be on default classpath.
 * It loads rocksDB code with custom classloader and then test that all
 * log levels for log events can be instantiated.
 */
public class LoggerClassloaderTest {
  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void testWithCustomClassLoader() throws Exception {
    try {
      this.getClass().getClassLoader().loadClass("org.rocksdb.RocksDB");
      fail("It looks like RocksDB is on classpath. This test must load RocksDB via custom "
          + "classLoader"
          + " to verify that callback cache all class instances.");
    } catch (ClassNotFoundException e) {
      ;
    }

    String jarPath = System.getProperty("rocks-jar");
    assertThat(jarPath).isNotNull().as("Java property 'rocks-jar' was not setup properly");

    Path classesDir = Paths.get(jarPath);
    ClassLoader cl = new URLClassLoader(new URL[] {classesDir.toAbsolutePath().toUri().toURL()});

    Class rocksDBclazz = cl.loadClass("org.rocksdb.RocksDB");
    Method loadLibrary = rocksDBclazz.getMethod("loadLibrary");
    loadLibrary.invoke(null);

    Class classUnderTest = cl.loadClass("org.rocksdb.LoggerTest");
    Method customLogger = classUnderTest.getMethod("customLogger");
    Field dbFolderField = classUnderTest.getDeclaredField("dbFolder");

    Object testInstance = classUnderTest.getDeclaredConstructor().newInstance();

    dbFolderField.set(testInstance, dbFolder);

    customLogger.invoke(testInstance);
  }
}
