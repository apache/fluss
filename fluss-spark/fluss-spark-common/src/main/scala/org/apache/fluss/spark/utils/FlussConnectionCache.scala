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

import org.apache.fluss.client.{Connection, ConnectionFactory}
import org.apache.fluss.client.table.Table
import org.apache.fluss.config.{ConfigOptions, Configuration}
import org.apache.fluss.metadata.{TableInfo, TablePath}
import org.apache.fluss.utils.IOUtils
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory

import org.apache.spark.internal.Logging

import java.time.Duration
import java.util.concurrent.{CompletableFuture, ExecutionException, Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

/** Shares Fluss connections by default for reads, writes and batch planning within a Spark JVM. */
private[spark] object FlussConnectionCache extends Logging {

  private lazy val pool = new FlussConnectionPool(
    conf => ConnectionFactory.createConnection(conf),
    () => System.nanoTime(),
    TimeUnit.MINUTES.toNanos(10))

  private lazy val evictor: ScheduledExecutorService = {
    val executor = Executors.newSingleThreadScheduledExecutor(
      new ExecutorThreadFactory("fluss-connection-cache-evictor"))
    executor.scheduleAtFixedRate(
      () => {
        try {
          pool.evictIdleConnections()
        } catch {
          case NonFatal(error) => logWarning("Error evicting idle Fluss connections", error)
        }
      },
      1,
      1,
      TimeUnit.MINUTES)
    Runtime.getRuntime.addShutdownHook(
      new FlussConnectionCacheShutdownHook(
        () => {
          try {
            executor.shutdownNow()
          } finally {
            pool.close()
          }
        },
        TimeUnit.SECONDS.toMillis(30)))
    executor
  }

  /** Borrows a connection; callers must close the lease rather than the connection. */
  def acquire(conf: Configuration): Lease = {
    evictor
    pool.acquire(conf)
  }

  /** Opens a table, replacing connections that retain state from a dropped table instance. */
  def acquireWithTable(conf: Configuration, tablePath: TablePath): (Lease, Table) = {
    var acquired: Option[(Lease, Table)] = None
    while (acquired.isEmpty) {
      val lease = acquire(conf)
      var table: Table = null
      try {
        table = lease.connection.getTable(tablePath)
        if (lease.registerTable(table.getTableInfo)) {
          acquired = Some((lease, table))
        }
      } catch {
        case error: Throwable =>
          lease.invalidate()
          try {
            IOUtils.closeAll(table, lease)
          } catch {
            case closeError: Throwable =>
              if (closeError ne error) {
                error.addSuppressed(closeError)
              }
          }
          throw error
      }
      if (acquired.isEmpty) {
        IOUtils.closeAll(table, lease)
      }
    }
    acquired.get
  }

  /** Releases idle connections before a local test cluster is stopped. */
  def clearIdleConnections(): Unit = pool.clearIdleConnections()

  /** A single borrow of a connection, with an idempotent release. */
  final class Lease private[utils] (
      val connection: Connection,
      registerTableInstance: TableInfo => Boolean,
      invalidateConnection: () => Unit,
      releaseConnection: () => Unit)
    extends AutoCloseable {

    private val released = new AtomicBoolean()

    private[utils] def registerTable(info: TableInfo): Boolean = registerTableInstance(info)

    /** Prevents subsequent borrowers from using this connection. */
    def invalidate(): Unit = invalidateConnection()

    /** Returns this borrow exactly once. */
    override def close(): Unit = {
      if (released.compareAndSet(false, true)) {
        releaseConnection()
      }
    }
  }
}

/** Bounds the JVM's wait for cache cleanup, including pool locks and connection shutdown. */
private[spark] class FlussConnectionCacheShutdownHook(cleanup: () => Unit, timeoutMillis: Long)
  extends Thread("fluss-connection-cache-shutdown")
  with Logging {

  require(timeoutMillis > 0, "Shutdown timeout must be positive")

  /** Waits within one overall budget; blocked cleanup remains on a daemon thread. */
  override def run(): Unit = {
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
    val worker = new Thread(
      () => {
        try {
          cleanup()
        } catch {
          case _: InterruptedException => Thread.currentThread().interrupt()
          case NonFatal(error) => logWarning("Error shutting down cached Fluss connections", error)
        }
      },
      "fluss-connection-cache-cleanup"
    )
    worker.setDaemon(true)
    worker.start()
    try {
      val remaining = deadline - System.nanoTime()
      if (remaining > 0) {
        TimeUnit.NANOSECONDS.timedJoin(worker, remaining)
      }
    } catch {
      case _: InterruptedException => Thread.currentThread().interrupt()
    }
    // Do not acquire pool locks, close resources or log from this hook: those operations may
    // block indefinitely. Timed-out cleanup can continue until the JVM terminates its daemon.
  }
}

/** Reference-counted pool; a clock and connection factory are injected for lifecycle tests. */
private[spark] class FlussConnectionPool(
    createConnection: Configuration => Connection,
    nanoTime: () => Long,
    idleTimeoutNanos: Long)
  extends AutoCloseable
  with Logging {

  import FlussConnectionCache.Lease

  private class Entry(val key: Map[String, String], val connection: Connection) {
    val tableIds = mutable.HashMap.empty[TablePath, Long]
    var references = 0
    var idleSince = 0L
    var invalid = false
  }

  private val cache = mutable.HashMap.empty[Map[String, String], Entry]
  private val creations = mutable.HashMap.empty[Map[String, String], CompletableFuture[Void]]
  // Invalidated connections remain here until their last borrower returns them.
  private val entries = mutable.HashSet.empty[Entry]
  private var closed = false

  /** Borrows the connection for an immutable snapshot of the supplied configuration. */
  def acquire(conf: Configuration): Lease = {
    val snapshot = new Configuration(conf)
    val key = snapshot.toMap.asScala.toMap
    // Resolve dynamic configuration providers on every acquisition, preserving credential refresh.
    val cacheable = snapshot.get(ConfigOptions.CONFIG_PROVIDERS).isEmpty
    if (!cacheable) {
      synchronized {
        require(!closed, "Fluss connection pool is closed")
      }
      return createAndBorrow(key, snapshot, None)
    }

    var lease: Lease = null
    while (lease == null) {
      var creator = false
      val creation = synchronized {
        require(!closed, "Fluss connection pool is closed")
        cache.get(key) match {
          case Some(entry) =>
            lease = borrow(entry)
            None
          case None =>
            Some(
              creations.getOrElseUpdate(
                key, {
                  creator = true
                  new CompletableFuture[Void]()
                }))
        }
      }
      creation.foreach {
        future =>
          if (creator) {
            lease = createAndBorrow(key, snapshot, Some(future))
          } else {
            try {
              future.get()
            } catch {
              case error: ExecutionException => throw error.getCause
            }
            // Retry the lookup: the creator's lease may have invalidated or evicted the entry
            // before this borrower wakes up. Never borrow an entry directly from the future.
          }
      }
    }
    lease
  }

  /** Closes expired, unused connections, including failed connections with no remaining borrows. */
  def evictIdleConnections(): Unit = {
    val expired = synchronized {
      val now = nanoTime()
      detach(
        entry =>
          entry.references == 0 &&
            (entry.invalid || now - entry.idleSince >= idleTimeoutNanos))
    }
    expired.foreach(closeConnection)
  }

  /** Closes unused connections without interrupting active borrowers. */
  def clearIdleConnections(): Unit = {
    val idle = synchronized(detach(_.references == 0))
    idle.foreach(closeConnection)
  }

  /** Closes the pool and its connections on JVM shutdown. */
  override def close(): Unit = {
    val (remaining, pending) = synchronized {
      closed = true
      val pending = creations.values.toVector
      creations.clear()
      (detach(_ => true), pending)
    }
    pending.foreach(
      _.completeExceptionally(new IllegalArgumentException("Fluss connection pool is closed")))
    remaining.foreach(closeConnection)
  }

  private def createAndBorrow(
      key: Map[String, String],
      conf: Configuration,
      creation: Option[CompletableFuture[Void]]): Lease = {
    // Bootstrap RPCs and dynamic providers can block. Only callers for this key wait for them.
    val entry =
      try {
        new Entry(key, createConnection(conf))
      } catch {
        case error: Throwable =>
          failCreation(key, creation, error)
          throw error
      }
    val lease = synchronized {
      if (closed) {
        None
      } else {
        entry.invalid = creation.isEmpty
        entries += entry
        if (creation.isDefined) {
          cache.put(key, entry)
          creations.remove(key)
        }
        Some(borrow(entry))
      }
    }
    lease match {
      case Some(acquired) =>
        creation.foreach(_.complete(null))
        acquired
      case None =>
        val error = new IllegalArgumentException("Fluss connection pool is closed")
        failCreation(key, creation, error)
        closeConnection(entry)
        throw error
    }
  }

  private def failCreation(
      key: Map[String, String],
      creation: Option[CompletableFuture[Void]],
      error: Throwable): Unit = {
    synchronized {
      creation.foreach {
        future =>
          if (creations.get(key).contains(future)) {
            creations.remove(key)
          }
      }
    }
    creation.foreach(_.completeExceptionally(error))
  }

  private def borrow(entry: Entry): Lease = {
    entry.references += 1
    new Lease(
      entry.connection,
      info => registerTable(entry, info),
      () => invalidate(entry),
      () => release(entry))
  }

  private def invalidate(entry: Entry): Unit = synchronized {
    entry.invalid = true
    removeCached(entry)
    // A callback may run on a sender thread. Closing here could wait for that same thread.
    // Release or the evictor performs the actual close outside the pool lock.
  }

  private def registerTable(entry: Entry, info: TableInfo): Boolean = synchronized {
    val path = info.getTablePath
    val id = info.getTableId
    if (entry.tableIds.get(path).exists(_ != id)) {
      // WriterClient retains per-path batch and partition state. A recreated table must use
      // a fresh connection even though its path and the client configuration are unchanged.
      invalidate(entry)
      false
    } else {
      entry.tableIds.put(path, id)
      true
    }
  }

  private def release(entry: Entry): Unit = {
    val shouldClose = synchronized {
      if (!entries.contains(entry)) {
        false
      } else {
        require(entry.references > 0, "Fluss connection reference count must be positive")
        entry.references -= 1
        if (entry.references == 0) {
          entry.idleSince = nanoTime()
          if (entry.invalid) {
            entries -= entry
            removeCached(entry)
            true
          } else {
            false
          }
        } else {
          false
        }
      }
    }
    if (shouldClose) {
      closeConnection(entry)
    }
  }

  private def removeCached(entry: Entry): Unit = {
    if (cache.get(entry.key).contains(entry)) {
      cache.remove(entry.key)
    }
  }

  private def detach(predicate: Entry => Boolean): Seq[Entry] = {
    val removed = entries.filter(predicate).toVector
    removed.foreach {
      entry =>
        entries -= entry
        removeCached(entry)
    }
    removed
  }

  private def closeConnection(entry: Entry): Unit = {
    try {
      // FlussConnection owns a shared Admin but does not close it itself. Keep it alive while
      // pending writes drain, and release its metadata-refresh executor afterwards.
      val admin =
        try {
          entry.connection.getAdmin
        } catch {
          case NonFatal(error) =>
            logWarning("Error obtaining the Admin of a cached Fluss connection", error)
            null
        }
      try {
        entry.connection.close(Duration.ofSeconds(30))
      } finally {
        if (admin != null) {
          admin.close()
        }
      }
    } catch {
      case NonFatal(e) => logWarning("Error closing a cached Fluss connection", e)
    }
  }
}
