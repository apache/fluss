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

package org.apache.fluss.spark.procedure

import org.apache.fluss.client.admin.Admin
import org.apache.fluss.metadata.TablePath
import org.apache.fluss.spark.SparkTable
import org.apache.fluss.spark.catalog.{AbstractSparkTable, WithFlussAdmin}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}

/**
 * Base class for Fluss stored procedures.
 *
 * This class provides common utility methods for procedure implementations, including identifier
 * parsing, table loading, admin access, and result row construction.
 *
 * @param tableCatalog
 *   the Spark catalog that owns this procedure
 */
abstract class BaseProcedure(tableCatalog: TableCatalog) extends Procedure {

  /**
   * Converts a string identifier to a Spark Identifier object.
   *
   * @param identifierAsString
   *   the identifier string (e.g., "db.table" or "table")
   * @param argName
   *   the parameter name for error reporting
   * @return
   *   the Spark Identifier
   */
  protected def toIdentifier(identifierAsString: String, argName: String): Identifier = {
    if (identifierAsString == null || identifierAsString.isEmpty) {
      throw new IllegalArgumentException(s"Cannot handle an empty identifier for argument $argName")
    }

    val spark = SparkSession.active
    val multipartIdentifier = identifierAsString.split("\\.")

    if (multipartIdentifier.length == 1) {
      val defaultNamespace = spark.sessionState.catalogManager.currentNamespace
      Identifier.of(defaultNamespace, multipartIdentifier(0))
    } else if (multipartIdentifier.length == 2) {
      Identifier.of(Array(multipartIdentifier(0)), multipartIdentifier(1))
    } else {
      throw new IllegalArgumentException(
        s"Invalid identifier format for argument $argName: $identifierAsString")
    }
  }

  /**
   * Loads a Spark table from the catalog.
   *
   * @param ident
   *   the table identifier
   * @return
   *   the SparkTable instance
   */
  protected def loadSparkTable(ident: Identifier): SparkTable = {
    try {
      val table = tableCatalog.loadTable(ident)
      table match {
        case sparkTable: SparkTable => sparkTable
        case _ =>
          throw new IllegalArgumentException(
            s"$ident is not a Fluss table: ${table.getClass.getName}")
      }
    } catch {
      case e: Exception =>
        val errMsg = s"Couldn't load table '$ident' in catalog '${tableCatalog.name()}'"
        throw new RuntimeException(errMsg, e)
    }
  }

  /**
   * Gets the Fluss Admin client from the catalog.
   *
   * @return
   *   the Admin instance
   */
  protected def getAdmin(): Admin = {
    tableCatalog match {
      case withAdmin: WithFlussAdmin => withAdmin.getAdmin
      case _ =>
        throw new IllegalStateException(
          s"Catalog does not support Fluss admin: ${tableCatalog.getClass.getName}")
    }
  }

  /**
   * Gets the Fluss Admin client from a SparkTable.
   *
   * @param table
   *   the SparkTable instance
   * @return
   *   the Admin instance
   */
  protected def getAdmin(table: SparkTable): Admin = {
    table match {
      case abstractTable: AbstractSparkTable => abstractTable.admin
      case _ =>
        throw new IllegalArgumentException(
          s"Table is not an AbstractSparkTable: ${table.getClass.getName}")
    }
  }

  /**
   * Creates a new InternalRow with the given values.
   *
   * @param values
   *   the values for the row
   * @return
   *   the InternalRow instance
   */
  protected def newInternalRow(values: Any*): InternalRow = {
    new GenericInternalRow(values.toArray)
  }

  /**
   * Converts a Spark Identifier to a Fluss TablePath.
   *
   * @param ident
   *   the Spark identifier
   * @return
   *   the TablePath instance
   */
  protected def toTablePath(ident: Identifier): TablePath = {
    if (ident.namespace().length != 1) {
      throw new IllegalArgumentException("Only single namespace is supported")
    }
    TablePath.of(ident.namespace()(0), ident.name())
  }
}

object BaseProcedure {

  /**
   * Abstract builder class for BaseProcedure implementations.
   *
   * @tparam T
   *   the concrete procedure type to build
   */
  abstract class Builder[T <: BaseProcedure] extends ProcedureBuilder {
    private var tableCatalog: TableCatalog = _

    override def withTableCatalog(newTableCatalog: TableCatalog): Builder[T] = {
      this.tableCatalog = newTableCatalog
      this
    }

    override def build(): T = doBuild()

    protected def doBuild(): T

    protected def getTableCatalog: TableCatalog = tableCatalog
  }
}
