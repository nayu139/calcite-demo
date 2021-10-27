package com.gerardnico.calcite;

import com.gerardnico.calcite.demo.JdbcStore;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Test;

import java.sql.*;

public class CalciteDDLJdbcTest {



    @Test
    public void SqlParserTest() throws SqlParseException, SQLException {

        // Create a data store wrapper object
        JdbcStore jdbcStore = new JdbcStore("jdbc:mysql://localhost:3306", "com.mysql.cj.jdbc.Driver", "root", null);

        String sql = "CREATE TABLE DMS_ADMIN.aaa (\n" +
                "  ID int DEFAULT NULL,\n" +
                "  TEST_VALUE int DEFAULT NULL,\n" +
                "    CONSTRAINT ID_KEY UNIQUE (ID)\n" +
                ")";

        //0.创建Calcite链接
        CalciteConnection calciteConnection = CalciteConnections.getConnectionWithoutModel();
        //创建数据库链接
        Connection connection = jdbcStore.getDataSource().getConnection();
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        //1.Parser
        //获取数据库方言
        SqlDialect dialect = CalciteRelRunners.getDialect(connection);
        SqlParser.ConfigBuilder configBuilder = dialect.configureParser(SqlParser.configBuilder());

        //获取default schema
        SchemaPlus defaultSchema = CalciteJdbc.getSchema("DMS_ADMIN", jdbcStore.getDataSource(), rootSchema);
        FrameworkConfig config = CalciteFramework.getNewConfig(defaultSchema, configBuilder.setParserFactory(SqlDdlParserImpl.FACTORY).build());

        //创建Planner
        Planner planner = CalcitePlanner.getPlannerFromFrameworkConfig(config);
        SqlNode parseNode = planner.parse(sql);

        //5.执行
        SqlString sqlString = parseNode.toSqlString(dialect);
        System.out.println(sqlString);
        jdbcStore.execute(sqlString.toString());

    }



    @Test
    public void RelNodeTest() throws SqlParseException, SQLException {

        // Create a data store wrapper object
//        JdbcStore jdbcStore = new JdbcStore("jdbc:mysql://localhost:3306", "com.mysql.cj.jdbc.Driver", "root", null);
        JdbcStore jdbcStoreDM = new JdbcStore("jdbc:dm://172.37.35.63:5236", "dm.jdbc.driver.DmDriver", "SYSDBA", "SYSDBA");
//        JdbcStore jdbcStore = JdbcStore.createDefault();

        String xx = "select * from O51859_S4133_T3000314\n" +
                "    inner join O51859_S4133 on O51859_S4133_T3000314.id=O51859_S4133.id\n" +
                "where ((O51859_S4133_T3000314.show_status is null or O51859_S4133_T3000314.show_status in ('1'))\n" +
                "           and O51859_S4133_T3000314.enterprise_id = '4422'\n" +
                "           and O51859_S4133_T3000314.company_id = '4133'\n" +
                "           and O51859_S4133_T3000314.tenant_id = '3000314')\n" +
                "  and O51859_S4133_T3000314.is_deleted = '0'";
        String dbName = "DMS_ADMIN";


        //0.创建Calcite链接
        CalciteConnection calciteConnection = CalciteConnections.getConnectionWithoutModel();
        //创建数据库链接
        Connection connection = jdbcStoreDM.getDataSource().getConnection();
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        //1.Parser
        //获取数据库方言
        SqlDialect dialect = CalciteRelRunners.getDialect(connection);
        SqlParser.ConfigBuilder configBuilder = dialect.configureParser(SqlParser.configBuilder());

        //获取default schema
        SchemaPlus defaultSchema = CalciteJdbc.getSchema("DMS_ADMIN", jdbcStoreDM.getDataSource(), rootSchema);


        // Get the builder
        RelBuilder builder = jdbcStoreDM.getRelBuilder(dbName, configBuilder.build());

        // Project Fetch
        int offset = 0;
        int fetch = 2;

        //字段区分大小写
        RelNode relNode = builder.scan("O51859_S4133_T3000314").scan("O51859_S4133")
                .join(JoinRelType.INNER, "ID")
                .filter(builder.or(builder.isNull(builder.field("SHOW_STATUS")), builder.call(SqlStdOperatorTable.IN, builder.field("SHOW_STATUS"), builder.literal("1"))),
                        builder.call(SqlStdOperatorTable.AND,
                                builder.equals(builder.field("ENTERPRISE_ID"), builder.literal("4422")),
                                builder.equals(builder.field("COMPANY_ID"), builder.literal("4133")),
                                builder.equals(builder.field("TENANT_ID"), builder.literal("3000314")),
                                builder.equals(builder.field("IS_DELETED"), builder.literal("0"))))
                .build();
        CalciteRel.explain(relNode);

        String sql = CalciteRel.fromRelNodeToSql(relNode, dialect);
        System.out.println(sql);


        HepPlanner hepPlanner = CalcitePlanner.createHepPlanner();

        hepPlanner.setRoot(relNode);
        RelNode bestRelNode = hepPlanner.findBestExp();
        //打印优化后执行计划
        CalciteRel.explain(bestRelNode);
        String sqlBest = CalciteRel.fromRelNodeToSql(bestRelNode, dialect);
        System.out.println(sqlBest);

        //5.执行
        ResultSet resultSet = CalciteRel.executeQueryFromConnection(bestRelNode, calciteConnection);
        CalciteJdbc.printResultSet(resultSet);

    }
}
