package org.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Test;

/**
 * Only this class can be on default classpath.
 * It loads rocksDB code with custom classloader and then test that all
 * event data for event listener can be instantiated.
 */
public class EventListenerClassloaderTest {
  @Test
  public void testCallback() throws Exception {
    assertThat(hasRocksDBonPath())
        .as("It looks like RocksDB is on classpath. "
            + "This test must load RocksDB via custom "
            + "classLoader to verify that callback cache all class instances.")
        .isFalse();

    String jarPath = System.getProperty("rocks-jar");
    assertThat(jarPath).as("Java property 'rocks-jar' was not setup properly").isNotNull();

    Path classesDir = Paths.get(jarPath);
    try (URLClassLoader cl =
             new URLClassLoader(new URL[] {classesDir.toAbsolutePath().toUri().toURL()})) {
      Class<?> rocksDBclazz = cl.loadClass("org.rocksdb.RocksDB");
      Method loadLibrary = rocksDBclazz.getMethod("loadLibrary");
      loadLibrary.invoke(null);

      Class<?> testableEventListenerClazz = cl.loadClass("org.rocksdb.test.TestableEventListener");
      Method invokeAllCallbacksInThread =
          testableEventListenerClazz.getMethod("invokeAllCallbacksInThread");
      Object instance = testableEventListenerClazz.getDeclaredConstructor().newInstance();
      invokeAllCallbacksInThread.invoke(instance);
    }
  }

  private boolean hasRocksDBonPath() {
    try {
      this.getClass().getClassLoader().loadClass("org.rocksdb.RocksDB");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
