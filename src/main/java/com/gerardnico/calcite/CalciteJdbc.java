package com.gerardnico.calcite;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Static method that are related to Calcite and JDBC
 */
public class CalciteJdbc {


    /**
     * @param dataSource
     * @return the calcite schema representation of the Jdbc schema
     */
    public static SchemaPlus getSchema(DataSource dataSource) {
        try {
            SchemaPlus rootSchema = Frameworks.createRootSchema(true);
            Connection connection = dataSource.getConnection();
            String schemaName = connection.getSchema();
            String catalogName = connection.getCatalog();
            String schema = null == schemaName ? catalogName : schemaName;
            rootSchema.add(
                    schema,
                    JdbcSchema.create(rootSchema, schemaName, dataSource, catalogName, schemaName)
            );
            return rootSchema.getSubSchema(schema);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static SchemaPlus getSchema(String dbName, DataSource dataSource) {
        try {
            SchemaPlus rootSchema = Frameworks.createRootSchema(true);
            fillRootSchema(dataSource, rootSchema);
            return rootSchema.getSubSchema(dbName);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public static SchemaPlus getSchema(String dbName, DataSource dataSource,SchemaPlus rootSchema) {
        try {
            fillRootSchema(dataSource, rootSchema);
            return rootSchema.getSubSchema(dbName);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void fillRootSchema(DataSource dataSource, SchemaPlus rootSchema) throws SQLException {
        DatabaseMetaData metaData = dataSource.getConnection().getMetaData();
        ResultSet schemas = metaData.getSchemas();
        ResultSet catalogs = metaData.getCatalogs();

        while (schemas.next()) {
            String tableSchem = schemas.getString("TABLE_SCHEM");
            String tableCatalog = schemas.getString("TABLE_CATALOG");
            String schema = null == tableSchem ? tableCatalog : tableSchem;
            rootSchema.add(
                    schema,
                    JdbcSchema.create(rootSchema, tableSchem, dataSource, tableCatalog, tableSchem)
            );
        }
        while (catalogs.next()) {
            String tableCat = catalogs.getString("TABLE_CAT");
            rootSchema.add(
                    tableCat,
                    JdbcSchema.create(rootSchema, tableCat, dataSource, tableCat, null)
            );
        }
    }

    /**
     * Return a RelBuilder based on the schema of a data store
     *
     * @param dataSource
     * @return
     */
    public static RelBuilder getBuilder(DataSource dataSource) {
        return CalciteRel.createDataStoreBasedRelBuilder(dataSource);
    }


    /**
     * An utility function to print a resultSet
     *
     * @param resultSet
     */
    public static void printResultSet(ResultSet resultSet) {
        try {
            StringBuilder stringBuilder = new StringBuilder();
            while (resultSet.next()) {
                for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                    stringBuilder
                            .append(resultSet.getObject(i))
                            .append(",");
                }
                stringBuilder.append("\n");
            }
            System.out.println();
            System.out.println("Result:");
            System.out.println(stringBuilder.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                resultSet.close();
            } catch (SQLException ignore) {
            }
        }
    }
}
