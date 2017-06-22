/*
 * ToroDB
 * Copyright © 2014 8Kdata Technology (www.8kdata.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.torodb.backend.tests.common;

import com.google.common.collect.ImmutableMap;
import com.torodb.backend.AbstractBackendBundle;
import com.torodb.backend.DbBackendService;
import com.torodb.backend.DslContextFactory;
import com.torodb.backend.SqlInterface;
import com.torodb.backend.ddl.DdlOps;
import com.torodb.backend.meta.SchemaUpdater;
import com.torodb.backend.meta.TorodbSchema;
import com.torodb.core.TableRefFactory;
import com.torodb.core.backend.IdentifierConstraints;
import com.torodb.core.d2r.D2RTranslatorFactory;
import com.torodb.core.d2r.R2DTranslator;
import com.torodb.core.transaction.metainf.ImmutableMetaSnapshot;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jooq.DSLContext;
import org.jooq.lambda.tuple.Tuple2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
public abstract class BackendTestContext<T extends AbstractBackendBundle & BackendTestBundle> {

  private final T bundle;

  public BackendTestContext(T bundle) {
    super();
    this.bundle = bundle;
  }

  public SqlInterface getSqlInterface() {
    return bundle.getExternalTestInterface().getSqlInterface();
  }

  public DdlOps getDdlOps() {
    return bundle.getExternalTestInterface().getDdlOps();
  }

  public TableRefFactory getTableRefFactory() {
    return bundle.getExternalTestInterface().getTableRefFactory();
  }

  public DslContextFactory getDslContextFactory() {
    return bundle.getExternalTestInterface().getDslContextFactory();
  }

  public SchemaUpdater getSchemaUpdater() {
    return bundle.getExternalTestInterface().getSchemaUpdater();
  }

  public D2RTranslatorFactory getD2RTranslatorFactory() {
    return bundle.getExternalTestInterface().getD2RTranslatorFactory();
  }

  public R2DTranslator getR2DTranslator() {
    return bundle.getExternalTestInterface().getR2DTranslator();
  }
  
  public void setupDatabase() throws SQLException {
    bundle.startAsync();
    bundle.awaitRunning();
    bundle.getExternalTestInterface().getReservedIdGenerator().load(
        new ImmutableMetaSnapshot(ImmutableMap.of()));
    try (Connection connection = getSqlInterface()
        .getDbBackend().createWriteConnection()) {
      dropDatabase();
      
      DSLContext dslContext = getDslContextFactory().createDslContext(connection);
      getSchemaUpdater().checkOrCreate(dslContext);

      connection.commit();
    }
  }

  public void tearDownDatabase() {
    bundle.stopAsync();
    bundle.awaitTerminated();
  }

  public void executeOnDbConnectionWithDslContext(Executor executor)
      throws Exception {

    try (Connection connection = getSqlInterface()
        .getDbBackend().createWriteConnection()) {
      DSLContext dslContext = getDslContextFactory().createDslContext(connection);
      
      executor.execute(dslContext);
      
      connection.rollback();
    }
  }

  public interface Executor {
    public void execute(DSLContext dslContext) throws Exception;
  }
  
  protected void dropDatabase() throws SQLException {
    DbBackendService dbBackend = getSqlInterface().getDbBackend();
    IdentifierConstraints identifierConstraints = getSqlInterface().getIdentifierConstraints();
    try (Connection connection = dbBackend.createSystemConnection()) {
      List<String> nextToDropSchemaList = new ArrayList<>();
      ResultSet schemas = getSchemas(connection);
      while (schemas.next()) {
        String schemaName = schemas.getString(getSchemaColumn());
        if (identifierConstraints.isAllowedSchemaIdentifier(schemaName) 
            || schemaName.equals(TorodbSchema.IDENTIFIER)) {
          nextToDropSchemaList.add(schemaName);
        }
      }
      
      if (!canDropFullSchema()) {
        List<Tuple2<String, String>> nextToDropSchemaTableList = new ArrayList<>();
        for (String schemaName : nextToDropSchemaList) {
          ResultSet tables = getTables(connection, schemaName);
    
          while (tables.next()) {
            String tableName = tables.getString("TABLE_NAME");
            nextToDropSchemaTableList.add(new Tuple2<>(schemaName, tableName));
          }
        }
        
        while (!nextToDropSchemaTableList.isEmpty()) {
          List<Tuple2<String, String>> toDropSchemaTableList = 
              new ArrayList<>(nextToDropSchemaTableList);
          nextToDropSchemaTableList.clear();
          for (Tuple2<String, String> toDropSchameTable : toDropSchemaTableList) {
            String schemaName = toDropSchameTable.v1;
            String tableName = toDropSchameTable.v2;
            if (identifierConstraints.isAllowedSchemaIdentifier(schemaName) 
                || schemaName.equals(TorodbSchema.IDENTIFIER)) {
              try (PreparedStatement preparedStatement = connection.prepareStatement("DROP TABLE "
                  + getQuoteChar() + schemaName + getQuoteChar()
                      + "." + getQuoteChar() + tableName + getQuoteChar())) {
                preparedStatement.executeUpdate();
                connection.commit();
              } catch (SQLException sqlException) {
                connection.rollback();
                if (getOnDropTableRetryStrategy().apply(sqlException)) {
                  nextToDropSchemaTableList.add(new Tuple2<>(schemaName, tableName));
                }
              }
            }
          }
        }
      }

      for (String schemaName : nextToDropSchemaList) {
        if (identifierConstraints.isAllowedSchemaIdentifier(schemaName) 
            || schemaName.equals(TorodbSchema.IDENTIFIER)) {
          String dropSchemaStatement = getDropSchemaStatement(schemaName);
          try (PreparedStatement preparedStatement = 
              connection.prepareStatement(dropSchemaStatement)) {
            preparedStatement.executeUpdate();
          }
        }
      }

      connection.commit();
    }
  }

  protected String getDropSchemaStatement(String schemaName) {
    return "DROP SCHEMA " + getQuoteChar() + schemaName + getQuoteChar() + " CASCADE";
  }

  protected boolean canDropFullSchema() {
    return true;
  }

  protected String getSchemaColumn() {
    return "TABLE_SCHEM";
  }

  protected char getQuoteChar() {
    return '"';
  }

  protected ResultSet getSchemas(Connection connection) throws SQLException {
    return connection.getMetaData().getSchemas("%", "%");
  }

  protected ResultSet getTables(Connection connection, 
      String schemaName) throws SQLException {
    return connection.getMetaData().getTables("%", schemaName, "%", null);
  }

  protected ResultSet getTable(Connection connection, 
      String schemaName, String tableName) throws SQLException {
    return connection.getMetaData().getTables("%", schemaName, tableName, null);
  }

  protected ResultSet getColumns(Connection connection, 
      String schemaName, String tableName) throws SQLException {
    return connection.getMetaData().getColumns("%", schemaName, tableName, "%");
  }

  protected ResultSet getIndexes(Connection connection, 
      String schemaName, String tableName) throws SQLException {
    return connection.getMetaData().getIndexInfo("%", schemaName, tableName, false, false);
  }
  
  protected Function<SQLException, Boolean> getOnDropTableRetryStrategy() {
    return ex -> true;
  }

}

