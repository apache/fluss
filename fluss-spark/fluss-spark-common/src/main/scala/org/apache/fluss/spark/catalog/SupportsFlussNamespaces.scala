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

package org.apache.fluss.spark.catalog

import org.apache.fluss.exception.DatabaseNotExistException
import org.apache.fluss.metadata.DatabaseDescriptor
import org.apache.fluss.utils.Preconditions

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.connector.catalog.{NamespaceChange, SupportsNamespaces}

import java.util
import java.util.concurrent.ExecutionException

import scala.collection.JavaConverters._

trait SupportsFlussNamespaces extends SupportsNamespaces with WithFlussAdmin {

  override def listNamespaces(): Array[Array[String]] = {
    admin.listDatabases.get.asScala.map(Array(_)).toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    if (namespace.isEmpty) {
      return listNamespaces()
    }

    doNamespaceOperator(namespace) {
      val dbname = admin.getDatabaseInfo(namespace.head).get().getDatabaseName
      Array(Array(dbname))
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    doNamespaceOperator(namespace) {
      admin.getDatabaseInfo(namespace.head).get().getDatabaseDescriptor.getCustomProperties
    }
  }

  override def createNamespace(
      namespace: Array[String],
      metadata: util.Map[String, String]): Unit = {
    doNamespaceOperator(namespace) {
      val databaseDescriptor = DatabaseDescriptor
        .builder()
        .customProperties(metadata)
        .build();
      admin.createDatabase(namespace.head, databaseDescriptor, false).get()
    }
  }

  override def alterNamespace(
      namespace: Array[String],
      namespaceChanges: NamespaceChange*): Unit = {
    new UnsupportedOperationException("Altering namespace is not supported now.")
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    doNamespaceOperator(namespace) {
      admin.dropDatabase(namespace.head, false, cascade).get()
      true
    }
  }

  protected def doNamespaceOperator[T](namespace: Array[String])(f: => T): T = {
    checkNamespace(namespace)
    try {
      f
    } catch {
      case e: ExecutionException if e.getCause.isInstanceOf[DatabaseNotExistException] =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  private def checkNamespace(namespace: Array[String]): Unit = {
    Preconditions.checkArgument(
      namespace.length == 1,
      "Only single namespace is supported in Fluss.")
  }
}
