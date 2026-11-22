/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.spark.utils

import org.apache.fluss.client.Connection
import org.apache.fluss.client.admin.Admin
import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.testutils.common.CommonTestUtils

import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doAnswer, mock, never, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.time.Duration
import java.util.concurrent.{Callable, CountDownLatch, ExecutionException, Executors, ExecutorService, Future, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

/** Lifecycle and concurrency tests for Spark's shared Fluss connections. */
class FlussConnectionCacheTest extends AnyFunSuite {

  test("a connection is shared and expires only after its final borrow is returned") {
    val now = new AtomicLong()
    val conn = connection()
    val pool = new FlussConnectionPool(_ => conn, () => now.get(), 10)
    try {
      val first = pool.acquire(new Configuration())
      val second = pool.acquire(new Configuration())
      assertThat(first.connection).isSameAs(second.connection)
      first.close()
      first.close()
      now.set(100)
      pool.clearIdleConnections()
      pool.evictIdleConnections()
      verify(conn, never()).close(any(classOf[Duration]))
      second.close()
      now.set(109)
      pool.evictIdleConnections()
      verify(conn, never()).close(any(classOf[Duration]))
      now.set(110)
      pool.evictIdleConnections()
      verify(conn).close(Duration.ofSeconds(30))
      verify(conn.getAdmin).close()
    } finally {
      pool.close()
    }
  }

  test("an invalid connection is replaced without closing another task's borrow") {
    val oldConnection = connection()
    val newConnection = connection()
    val created = new AtomicInteger()
    val pool = new FlussConnectionPool(
      _ => if (created.getAndIncrement() == 0) oldConnection else newConnection,
      () => 0L,
      10)
    try {
      val first = pool.acquire(new Configuration())
      val second = pool.acquire(new Configuration())
      first.invalidate()
      verify(oldConnection, never()).close(any(classOf[Duration]))
      val replacement = pool.acquire(new Configuration())
      assertThat(replacement.connection).isSameAs(newConnection)
      first.close()
      pool.evictIdleConnections()
      verify(oldConnection, never()).close(any(classOf[Duration]))
      second.close()
      second.close()
      verify(oldConnection, times(1)).close(any(classOf[Duration]))
      val another = pool.acquire(new Configuration())
      assertThat(another.connection).isSameAs(replacement.connection)
      replacement.close()
      another.close()
    } finally {
      pool.close()
    }
  }

  test("mutable caller and client configurations cannot change a cached key") {
    val created = new AtomicInteger()
    val pool = new FlussConnectionPool(
      conf => {
        created.incrementAndGet()
        conf.setString("client.metrics.reporters", "jmx")
        connection()
      },
      () => 0L,
      10)
    try {
      val config = new Configuration()
      config.setString("bootstrap.servers", "first:9123")
      val first = pool.acquire(config)
      assertThat(config.containsKey("client.metrics.reporters")).isFalse
      config.setString("bootstrap.servers", "second:9123")
      val second = pool.acquire(config)
      assertThat(first.connection).isNotSameAs(second.connection)
      val original = new Configuration()
      original.setString("bootstrap.servers", "first:9123")
      val repeated = pool.acquire(original)
      assertThat(repeated.connection).isSameAs(first.connection)
      assertThat(created.get()).isEqualTo(2)
      first.close()
      second.close()
      repeated.close()
    } finally {
      pool.close()
    }
  }

  test("provider configurations bypass connection reuse") {
    val created = new AtomicInteger()
    val pool = new FlussConnectionPool(
      _ => {
        created.incrementAndGet()
        connection()
      },
      () => 0L,
      10)
    try {
      val config = new Configuration()
      config.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "credentials")
      val first = pool.acquire(config)
      val second = pool.acquire(config)
      assertThat(first.connection).isNotSameAs(second.connection)
      first.close()
      second.close()
      verify(first.connection).close(any(classOf[Duration]))
      verify(second.connection).close(any(classOf[Duration]))
      assertThat(created.get()).isEqualTo(2)
    } finally {
      pool.close()
    }
  }

  test("slow connection shutdown does not block acquisitions for another configuration") {
    val closing = new CountDownLatch(1)
    val finishClose = new CountDownLatch(1)
    val slow = connection()
    doAnswer {
      _ =>
        closing.countDown()
        finishClose.await()
        null
    }.when(slow).close(any(classOf[Duration]))
    val pool = new FlussConnectionPool(
      conf => if (conf.containsKey("slow")) slow else connection(),
      () => 0L,
      10)
    val executor = Executors.newFixedThreadPool(2)
    try {
      val config = new Configuration()
      config.setString("slow", "true")
      pool.acquire(config).close()
      val cleanup = executor.submit(new Runnable {
        override def run(): Unit = pool.clearIdleConnections()
      })
      assertThat(closing.await(10, TimeUnit.SECONDS)).isTrue
      val acquisition = acquireAsync(pool, executor)
      val other = acquisition.get(5, TimeUnit.SECONDS)
      assertThat(other.connection).isNotSameAs(slow)
      other.close()
      finishClose.countDown()
      cleanup.get(10, TimeUnit.SECONDS)
    } finally {
      finishClose.countDown()
      executor.shutdownNow()
      pool.close()
    }
  }

  test("slow connection creation does not block other configurations or failure callbacks") {
    for (provider <- Seq(false, true)) {
      val creating = new CountDownLatch(1)
      val finishCreation = new CountDownLatch(1)
      val pool = new FlussConnectionPool(
        conf => {
          if (conf.containsKey("slow")) {
            creating.countDown()
            finishCreation.await()
          }
          connection()
        },
        () => 0L,
        10)
      val executor = Executors.newFixedThreadPool(2)
      val existing = pool.acquire(new Configuration())
      try {
        val config = new Configuration()
        config.setString("slow", "true")
        if (provider) {
          config.setString(ConfigOptions.CONFIG_PROVIDERS.key(), "credentials")
        }
        val slow = acquireAsync(pool, executor, config)
        assertThat(creating.await(10, TimeUnit.SECONDS)).isTrue
        val healthyOperations = executor.submit(new Runnable {
          override def run(): Unit = {
            val reused = pool.acquire(new Configuration())
            assertThat(reused.connection).isSameAs(existing.connection)
            reused.close()
            existing.invalidate()
            existing.close()
            pool.evictIdleConnections()
            val replacement = pool.acquire(new Configuration())
            assertThat(replacement.connection).isNotSameAs(existing.connection)
            replacement.close()
            pool.clearIdleConnections()
          }
        })
        healthyOperations.get(5, TimeUnit.SECONDS)
        assertThat(slow.isDone).isFalse
        finishCreation.countDown()
        slow.get(10, TimeUnit.SECONDS).close()
      } finally {
        finishCreation.countDown()
        executor.shutdownNow()
        existing.close()
        pool.close()
      }
    }
  }

  test("borrowers of one configuration wait for a single connection creation") {
    val creating = new CountDownLatch(1)
    val finishCreation = new CountDownLatch(1)
    val attempts = new AtomicInteger()
    val conn = connection()
    val pool = new FlussConnectionPool(
      _ => {
        attempts.incrementAndGet()
        creating.countDown()
        finishCreation.await()
        conn
      },
      () => 0L,
      10)
    val executor = Executors.newFixedThreadPool(2)
    try {
      val creator = acquireAsync(pool, executor)
      assertThat(creating.await(10, TimeUnit.SECONDS)).isTrue
      val waiterThread = new AtomicReference[Thread]()
      val waiter = acquireAsync(pool, executor, thread = waiterThread)
      awaitWaiting(waiterThread, waiter)
      assertThat(attempts.get()).isEqualTo(1)
      finishCreation.countDown()
      val first = creator.get(10, TimeUnit.SECONDS)
      val second = waiter.get(10, TimeUnit.SECONDS)
      assertThat(first.connection).isSameAs(second.connection)
      first.close()
      second.close()
      pool.clearIdleConnections()
      verify(conn, times(1)).close(any(classOf[Duration]))
    } finally {
      finishCreation.countDown()
      executor.shutdownNow()
      pool.close()
    }
  }

  test("a creation failure reaches waiting borrowers and permits a subsequent retry") {
    val creating = new CountDownLatch(1)
    val finishCreation = new CountDownLatch(1)
    val failure = new IllegalStateException("creation failed")
    val attempts = new AtomicInteger()
    val conn = connection()
    val pool = new FlussConnectionPool(
      _ => {
        if (attempts.getAndIncrement() == 0) {
          creating.countDown()
          finishCreation.await()
          throw failure
        }
        conn
      },
      () => 0L,
      10)
    val executor = Executors.newFixedThreadPool(2)
    try {
      val creator = acquireAsync(pool, executor)
      assertThat(creating.await(10, TimeUnit.SECONDS)).isTrue
      val waiterThread = new AtomicReference[Thread]()
      val waiter = acquireAsync(pool, executor, thread = waiterThread)
      awaitWaiting(waiterThread, waiter)
      finishCreation.countDown()
      for (result <- Seq(creator, waiter)) {
        assertThatThrownBy(() => result.get(10, TimeUnit.SECONDS))
          .isInstanceOf(classOf[ExecutionException])
          .hasCause(failure)
      }
      pool.acquire(new Configuration()).close()
      assertThat(attempts.get()).isEqualTo(2)
    } finally {
      finishCreation.countDown()
      executor.shutdownNow()
      pool.close()
    }
  }

  test("shutdown closes active connections, releases waiters and cleans up late creation") {
    val creating = new CountDownLatch(1)
    val finishCreation = new CountDownLatch(1)
    val late = connection()
    val pool = new FlussConnectionPool(
      conf => {
        if (conf.containsKey("slow")) {
          creating.countDown()
          finishCreation.await()
          late
        } else {
          connection()
        }
      },
      () => 0L,
      10)
    val executor = Executors.newFixedThreadPool(3)
    val invalid = pool.acquire(new Configuration())
    invalid.invalidate()
    val active = pool.acquire(new Configuration())
    val config = new Configuration()
    config.setString("slow", "true")
    try {
      val creator = acquireAsync(pool, executor, config)
      assertThat(creating.await(10, TimeUnit.SECONDS)).isTrue
      val waiterThread = new AtomicReference[Thread]()
      val waiter = acquireAsync(pool, executor, config, waiterThread)
      awaitWaiting(waiterThread, waiter)
      executor
        .submit(new Runnable {
          override def run(): Unit = pool.close()
        })
        .get(5, TimeUnit.SECONDS)
      assertThatThrownBy(() => waiter.get(5, TimeUnit.SECONDS))
        .hasCauseInstanceOf(classOf[IllegalArgumentException])
      assertThat(creator.isDone).isFalse
      assertThatThrownBy(() => pool.acquire(config))
        .isInstanceOf(classOf[IllegalArgumentException])
      invalid.close()
      active.close()
      pool.close()
      verify(invalid.connection, times(1)).close(any(classOf[Duration]))
      verify(active.connection, times(1)).close(any(classOf[Duration]))
      finishCreation.countDown()
      assertThatThrownBy(() => creator.get(10, TimeUnit.SECONDS))
        .hasCauseInstanceOf(classOf[IllegalArgumentException])
      verify(late, times(1)).close(any(classOf[Duration]))
      verify(late.getAdmin).close()
    } finally {
      finishCreation.countDown()
      executor.shutdownNow()
      invalid.close()
      active.close()
      pool.close()
    }
  }

  test("interrupting a waiting borrower does not cancel creation or retain a reference") {
    val creating = new CountDownLatch(1)
    val finishCreation = new CountDownLatch(1)
    val conn = connection()
    val pool = new FlussConnectionPool(
      _ => {
        creating.countDown()
        finishCreation.await()
        conn
      },
      () => 0L,
      10)
    val executor = Executors.newFixedThreadPool(2)
    try {
      val creator = acquireAsync(pool, executor)
      assertThat(creating.await(10, TimeUnit.SECONDS)).isTrue
      val waiterThread = new AtomicReference[Thread]()
      val waiter = acquireAsync(pool, executor, thread = waiterThread)
      awaitWaiting(waiterThread, waiter)
      waiterThread.get().interrupt()
      assertThatThrownBy(() => waiter.get(5, TimeUnit.SECONDS))
        .hasCauseInstanceOf(classOf[InterruptedException])
      assertThat(creator.isDone).isFalse
      finishCreation.countDown()
      creator.get(10, TimeUnit.SECONDS).close()
      pool.clearIdleConnections()
      verify(conn, times(1)).close(any(classOf[Duration]))
    } finally {
      finishCreation.countDown()
      executor.shutdownNow()
      pool.close()
    }
  }

  test("a JVM exits even when cache cleanup or connection creation remains blocked") {
    val java = new File(new File(System.getProperty("java.home"), "bin"), "java")
    for (mode <- Seq("cleanup", "creation")) {
      val process = new ProcessBuilder(
        java.getAbsolutePath,
        "-cp",
        System.getProperty("java.class.path"),
        "org.apache.fluss.spark.utils.TestingConnectionCacheShutdown",
        mode)
        .redirectErrorStream(true)
        .start()
      try {
        assertThat(process.waitFor(10, TimeUnit.SECONDS))
          .as("JVM with blocked %s must exit", mode)
          .isTrue
        assertThat(process.exitValue()).isZero
      } finally {
        process.destroyForcibly()
      }
    }
  }

  private def acquireAsync(
      pool: FlussConnectionPool,
      executor: ExecutorService,
      config: Configuration = new Configuration(),
      thread: AtomicReference[Thread] = new AtomicReference[Thread]())
      : Future[FlussConnectionCache.Lease] = {
    executor.submit(new Callable[FlussConnectionCache.Lease] {
      override def call(): FlussConnectionCache.Lease = {
        thread.set(Thread.currentThread())
        pool.acquire(config)
      }
    })
  }

  private def awaitWaiting(thread: AtomicReference[Thread], result: Future[_]): Unit = {
    CommonTestUtils.waitUntil(
      () => thread.get() != null && thread.get().getState == Thread.State.WAITING && !result.isDone,
      Duration.ofSeconds(10),
      "Borrower must wait for the connection creation result"
    )
  }

  private def connection(): Connection = {
    val conn = mock(classOf[Connection])
    when(conn.getAdmin).thenReturn(mock(classOf[Admin]))
    conn
  }
}

/** A child JVM with deliberately non-terminating cache cleanup for shutdown regression tests. */
object TestingConnectionCacheShutdown {

  /** Registers the real cache shutdown hook and lets the JVM exit naturally. */
  def main(args: Array[String]): Unit = {
    val cleanup: () => Unit = if (args(0) == "creation") {
      val creating = new CountDownLatch(1)
      val pool = new FlussConnectionPool(
        _ => {
          creating.countDown()
          new CountDownLatch(1).await()
          throw new IllegalStateException("Unreachable connection creation")
        },
        () => 0L,
        10)
      val borrower = new Thread(() => { pool.acquire(new Configuration()); () })
      borrower.setDaemon(true)
      borrower.start()
      creating.await()
      () => pool.close()
    } else { () => new CountDownLatch(1).await() }
    Runtime.getRuntime.addShutdownHook(new FlussConnectionCacheShutdownHook(cleanup, 100))
  }
}
