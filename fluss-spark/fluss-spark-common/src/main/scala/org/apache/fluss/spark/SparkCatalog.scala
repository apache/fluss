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

package org.apache.fluss.spark

import org.apache.fluss.exception.{DatabaseNotExistException, TableAlreadyExistException, TableNotExistException}
import org.apache.fluss.metadata.TablePath
import org.apache.fluss.spark.catalog.{SupportsFlussNamespaces, WithFlussAdmin}

import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.concurrent.ExecutionException

import scala.collection.JavaConverters._

class SparkCatalog extends TableCatalog with SupportsFlussNamespaces with WithFlussAdmin {

  private var catalogName: String = "fluss"

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    doNamespaceOperator(namespace) {
      admin
        .listTables(namespace.head)
        .get()
        .asScala
        .map(table => Identifier.of(namespace, table))
        .toArray
    }
  }

  override def loadTable(ident: Identifier): Table = {
    try {
      SparkTable(admin.getTableInfo(toTablePath(ident)).get())
    } catch {
      case e: ExecutionException if e.getCause.isInstanceOf[TableNotExistException] =>
        throw new NoSuchTableException(ident)
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    try {
      val tableDescriptor = SparkConversions.toFlussTable(schema, partitions, properties)
      admin.createTable(toTablePath(ident), tableDescriptor, false).get()
      loadTable(ident)
    } catch {
      case e: ExecutionException =>
        if (e.getCause.isInstanceOf[DatabaseNotExistException]) {
          throw new NoSuchNamespaceException(ident.namespace())
        } else if (e.getCause.isInstanceOf[TableAlreadyExistException]) {
          throw new TableAlreadyExistsException(ident)
        } else {
          throw new RuntimeException(e)
        }
    }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("Altering table is not supported")
  }

  override def dropTable(ident: Identifier): Boolean = {
    try {
      admin.dropTable(toTablePath(ident), false).get()
      true
    } catch {
      case e: ExecutionException if e.getCause.isInstanceOf[TableNotExistException] =>
        throw new NoSuchTableException(ident)
    }
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Renaming table is not supported")
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name;
    initFlussClient(options)
  }

  override def name(): String = catalogName

  private def toTablePath(ident: Identifier): TablePath = {
    assert(ident.namespace().length == 1, "Only single namespace is supported")
    TablePath.of(ident.namespace().head, ident.name)
  }
}
