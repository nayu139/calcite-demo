package com.gerardnico.calcite;

import com.gerardnico.calcite.demo.JdbcStore;
import com.gerardnico.calcite.mock.MockCatalogReaderSimple;
import com.gerardnico.calcite.mock.MockViewExpander;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Planner;

public class CalciteSql {

    /**
     * Transform a SQL Node to SQL back
     *
     * @param sqlNode
     * @return
     */
    public static String fromSqlNodeToSql(SqlNode sqlNode) {
        SqlPrettyWriter sqlWriter = new SqlPrettyWriter();
        sqlNode.unparse(sqlWriter, 0, 0);
        return sqlWriter.toString();
    }

    /**
     * @param sql - A sql statement (query, insert, ...)
     * @return - the tree
     */
    public static SqlNode fromSqlToSqlNode(String sql) {
        try {
            SqlParser parser = CalciteSqlParser.create(sql);
            return parser.parseStmt();
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * @param sql - A sql query statement
     * @return - the tree
     */
    public static SqlNode fromSqlQueryToSqlNode(String sql) {
        try {
            SqlParser parser = CalciteSqlParser.create(sql);
            return parser.parseQuery();
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * @param sql - A query
     * @param config - The Sql parser config
     * @return - the tree
     */
    public static SqlNode fromSqlToSqlNode(String sql, SqlParser.Config config) {
        try {
            SqlParser parser = CalciteSqlParser.create(
                    sql,
                    config
            );
            return parser.parseQuery();
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }

    }

    public static SqlNode fromSqlToSqlNodeMySql(String sql){
        return fromSqlToSqlNode(sql,CalciteSqlParser.createMySqlConfig());
    }

    /**
     *
     * @param sql
     * @return a relNode
     */
    public static RelRoot fromSqlToRelNode(String sql) {

        SqlNode sqlNode = fromSqlToSqlNode(sql);
        return CalciteSqlNode.fromSqlNodeToRelRoot(sqlNode);

    }




    /**
     * After having create a {@link RelNode regular expression} with for instance the  {@link JdbcStore#getRelBuilder() builder},
     * you can transform it into sql
     *
     * @param relNode
     * @param sqlDialect - A sql dialect (one of {@link CalciteSqlDialect#getDialect(CalciteSqlDialect.DIALECT)})
     * @return the sql representation of the relNode
     */
    static public String fromRelNodeToSql(RelNode relNode, SqlDialect sqlDialect) {
        SqlPrettyWriter sqlWriter = new SqlPrettyWriter(SqlPrettyWriter.config().withDialect(sqlDialect));
        RelToSqlConverter relToSqlConverter = new RelToSqlConverter(sqlDialect);
        SqlSelect sqlSelect = relToSqlConverter.visitChild(0, relNode).asSelect();
        return sqlWriter.format(sqlSelect);
    }

    /**
     * @param relNode
     * @return a sql in the ANSI dialect
     */
    static public String fromRelNodeToSql(RelNode relNode) {
        SqlDialect dialect = AnsiSqlDialect.DEFAULT;
        return fromRelNodeToSql(relNode, dialect);
    }



    /**
     * Print the tree
     */
    public static void print(SqlNode sqlNode) {
        SqlWriterConfig config = SqlPrettyWriter.config();
        print(sqlNode,config);
    }

    /**
     * Print the tree
     */
    public static void print(SqlNode sqlNode, SqlWriterConfig config) {
        System.out.println(new SqlPrettyWriter(config).format(sqlNode));
    }
}

