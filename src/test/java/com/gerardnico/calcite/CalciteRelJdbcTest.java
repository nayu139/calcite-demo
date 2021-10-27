package com.gerardnico.calcite;

import com.gerardnico.calcite.demo.JdbcStore;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.junit.Test;

import java.sql.*;

public class CalciteRelJdbcTest {

    @Test
    public void relToSqlTest() {

        // Create a data store wrapper object
        JdbcStore jdbcStore = JdbcStore.createDefault();

        // Build the schema
        try {
            Connection connection = jdbcStore.getConnection();
            connection.createStatement().execute("CREATE TABLE TEST (TEST_VALUE INT)");
            String insertStatement = "INSERT INTO TEST (TEST_VALUE) VALUES(?)";
            for (int i = 0; i < 20; i++) {
                PreparedStatement preparedStatement = connection.prepareStatement(insertStatement);
                preparedStatement.setInt(1, i);
                preparedStatement.execute();
            }
            System.out.println("Records inserted");
            System.out.println();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Get the builder
        RelBuilder builder = jdbcStore.getRelBuilder();

        // Project Fetch
        int offset = 0;
        int fetch = 2;

        RelNode relNode = builder.scan("TEST")
                .project(builder.field("TEST_VALUE"))
                .sortLimit(offset, fetch, builder.field("TEST_VALUE"))
                .build();

        String sql = CalciteRel.fromRelNodeToSql(relNode, jdbcStore.getDialect());

        System.out.println(sql);
        System.out.println("Execute the SQL");
        jdbcStore.executeAndPrintQuery(sql);


    }

    @Test
    public void SqlTest() {

        // Create a data store wrapper object
        JdbcStore jdbcStore = new JdbcStore("jdbc:dm://172.37.35.63:5236", "dm.jdbc.driver.DmDriver", "SYSDBA", "SYSDBA");

        // Build the schema
        try {
            Connection connection = jdbcStore.getConnection();
//            connection.createStatement().execute("CREATE TABLE TEST (TEST_VALUE INT)");
//            String insertStatement = "INSERT INTO TEST (TEST_VALUE) VALUES(?)";
//            for (int i = 0; i < 20; i++) {
//                PreparedStatement preparedStatement = connection.prepareStatement(insertStatement);
//                preparedStatement.setInt(1, i);
//                preparedStatement.execute();
//            }
//            System.out.println("Records inserted");
//            System.out.println();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Get the builder
        RelBuilder builder = jdbcStore.getRelBuilder("DMS_ADMIN");

        // Project Fetch
        int offset = 0;
        int fetch = 2;

        RelNode relNode = builder.scan("11114")
                .sortLimit(offset, fetch)
                .build();
        SqlDialect dialect = jdbcStore.getDialect();
        String sql = CalciteRel.fromRelNodeToSql(relNode, dialect);

        System.out.println(sql);
        System.out.println("Execute the SQL");
        jdbcStore.executeAndPrintQuery(sql);


    }


    @Test
    public void fromJsonTest() throws SqlParseException, SQLException {

        // Create a data store wrapper object
        JdbcStore jdbcStore = new JdbcStore("jdbc:mysql://localhost:3306", "com.mysql.cj.jdbc.Driver", "root", null);
//        JdbcStore jdbcStore = new JdbcStore("jdbc:dm://172.37.35.63:5236", "dm.jdbc.driver.DmDriver", "SYSDBA", "SYSDBA");
//        JdbcStore jdbcStore = JdbcStore.createDefault();

        String sql = "select * from O51859_S4133_T3000314\n" +
                "    inner join O51859_S4133 on O51859_S4133_T3000314.id=O51859_S4133.id\n" +
                "where ((O51859_S4133_T3000314.show_status is null or O51859_S4133_T3000314.show_status in ('1'))\n" +
                "           and O51859_S4133_T3000314.enterprise_id = '4422'\n" +
                "           and O51859_S4133_T3000314.company_id = '4133'\n" +
                "           and O51859_S4133_T3000314.tenant_id = '3000314')\n" +
                "  and O51859_S4133_T3000314.is_deleted = '0'";

        sql = "select * from aaa";

        //1.Parser
        SqlDialect dialect = CalciteRelRunners.getDialect();
        SqlParser.ConfigBuilder configBuilder = dialect.configureParser(SqlParser.configBuilder());

        DatabaseMetaData metaData = CalciteRelRunners.getConnection().getMetaData();
        SchemaPlus dmsAdminSchemaPlus = CalciteJdbc.getSchema("TEST", jdbcStore.getDataSource());
        FrameworkConfig config = CalciteFramework.getNewConfig(dmsAdminSchemaPlus, configBuilder.build());

        Planner planner = CalcitePlanner.getPlannerFromFrameworkConfig(config);
        SqlNode parseNode = planner.parse(sql);

        //2.Validate
        SqlNode validateNode = CalciteSqlValidation.validateFromPlanner(planner, parseNode);

        //3.Logical Plan
        RelRoot relRoot = CalciteSqlNode.fromSqlNodeToRelRootViaPlanner(planner, validateNode);
        CalciteRel.explain(relRoot.project());

        //4.优化阶段
        HepPlanner hepPlanner = CalcitePlanner.createHepPlanner();
        hepPlanner.setRoot(relRoot.project());
        RelNode bestRelNode = hepPlanner.findBestExp();
        CalciteRel.explain(bestRelNode);

        //5.执行
        ResultSet resultSet = CalciteRel.executeQuery(bestRelNode);
        CalciteJdbc.printResultSet(resultSet);

    }

    @Test
    public void SqlParserTest() throws SqlParseException, SQLException {

        // Create a data store wrapper object
//        JdbcStore jdbcStore = new JdbcStore("jdbc:mysql://localhost:3306", "com.mysql.cj.jdbc.Driver", "root", null);
        JdbcStore jdbcStore = new JdbcStore("jdbc:dm://172.37.35.63:5236", "dm.jdbc.driver.DmDriver", "SYSDBA", "SYSDBA");
//        JdbcStore jdbcStore = JdbcStore.createDefault();

        String sql = "select * from O51859_S4133_T3000314\n" +
                "    inner join O51859_S4133 on O51859_S4133_T3000314.id=O51859_S4133.id\n" +
                "where ((O51859_S4133_T3000314.show_status is null or O51859_S4133_T3000314.show_status in ('1'))\n" +
                "           and O51859_S4133_T3000314.enterprise_id = '4422'\n" +
                "           and O51859_S4133_T3000314.company_id = '4133'\n" +
                "           and O51859_S4133_T3000314.tenant_id = '3000314')\n" +
                "  and O51859_S4133_T3000314.is_deleted = '0'";


        sql = "SELECT O52341_T3000402.modified_time, O52341_T3000402.create_user_id, O52341_T3000402.deleted_department_id, O52341_T3000402.modify_department_id, O52341_T3000402.modify_user_id, O52341_T3000402.platform_id, O52341_T3000402.deleted_user_id, O52341_T3000402.create_department_id, O52341_T3000402.company_id, O52341_T3000402.is_deleted, O52341_T3000402.is_modified, O52341_T3000402.deleted_tag_id, O52341_T3000402.self_version, O52341_T3000402.tenant_id, O52341_T3000402.created_time, O52341_T3000402.enterprise_id, O52341_T3000402.id, O52341_T3000402.deleted_time, O52341_T3000402.data_O886001, O52341_T3000402.data_O884173, O52341_T3000402.data_O882360, O52341_T3000402.data_O882420, O52341_T3000402.data_O884172, O52341_T3000402.data_O884171, O52341_T3000402.data_O882418, O52341_T3000402.data_O882419, O52341_T3000402.data_O883643, O52341_T3000402.data_O882411, O52341_T3000402.data_O883644, O52341_T3000402.data_O882412, O52341_T3000402.data_O882413, O52341_T3000402.data_O882414, O52341_T3000402.data_O882415, O52341_T3000402.data_O882416, O52341_T3000402.data_O882417 FROM dms_admin.O52341_T3000402 WHERE (O52341_T3000402.enterprise_id = 'D4714' AND (O52341_T3000402.company_id = '4192' AND O52341_T3000402.tenant_id = '3000402') AND (O52341_T3000402.show_status IS NULL OR O52341_T3000402.show_status IN ('1'))) AND O52341_T3000402.is_deleted = '0' ORDER BY O52341_T3000402.data_O883643, O52341_T3000402.id LIMIT 100";
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
        FrameworkConfig config = CalciteFramework.getNewConfig(defaultSchema, configBuilder.build());

        //创建Planner
        Planner planner = CalcitePlanner.getPlannerFromFrameworkConfig(config);
        SqlNode parseNode = planner.parse(sql);

        //2.Validate
        SqlNode validateNode = CalciteSqlValidation.validateFromPlanner(planner, parseNode);
        System.out.println(validateNode.toSqlString(dialect));
        System.out.println();
        System.out.println(validateNode.toString());
        //3.Logical Plan
        RelRoot relRoot = CalciteSqlNode.fromSqlNodeToRelRootViaPlanner(planner, validateNode);
        //打印优化前执行计划
        CalciteRel.explain(relRoot.project());

        //4.优化阶段
        HepPlanner hepPlanner = CalcitePlanner.createHepPlanner();
        hepPlanner.setRoot(relRoot.project());
        RelNode bestRelNode = hepPlanner.findBestExp();
        //打印优化后执行计划
        CalciteRel.explain(bestRelNode);

        //5.执行
        ResultSet resultSet = CalciteRel.executeQueryFromConnection(bestRelNode, calciteConnection);
        CalciteJdbc.printResultSet(resultSet);

    }


    @Test
    public void Sql2Test() throws SqlParseException, SQLException {

        // Create a data store wrapper object
        JdbcStore jdbcStore = new JdbcStore("jdbc:mysql://localhost:3306", "com.mysql.cj.jdbc.Driver", "root", null);
        JdbcStore jdbcStoreDM = new JdbcStore("jdbc:dm://172.37.35.63:5236", "dm.jdbc.driver.DmDriver", "SYSDBA", "SYSDBA");
//        JdbcStore jdbcStore = JdbcStore.createDefault();

        String sql = "select * from O51859_S4133_T3000314\n" +
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
//
        ResultSet resultSet = getResultSet(jdbcStoreDM, dbName, sql, calciteConnection, connection, rootSchema);
        CalciteJdbc.printResultSet(resultSet);

        sql = "select * from aaa";
        dbName = "TEST";
        resultSet = getResultSet(jdbcStore, dbName, sql, calciteConnection, jdbcStore.getDataSource().getConnection(), rootSchema);
        CalciteJdbc.printResultSet(resultSet);

    }

    private ResultSet getResultSet(JdbcStore jdbcStore, String dbName, String sql, CalciteConnection calciteConnection, Connection connection, SchemaPlus rootSchema) throws SqlParseException {
        //1.Parser
        //获取数据库方言
        //TODO: 可缓存
        SqlDialect dialect = CalciteRelRunners.getDialect(connection);
        SqlParser.ConfigBuilder configBuilder = dialect.configureParser(SqlParser.configBuilder());

        //获取default schema
        //TODO: 可缓存
        SchemaPlus defaultSchema = CalciteJdbc.getSchema(dbName, jdbcStore.getDataSource(), rootSchema);
        FrameworkConfig config = CalciteFramework.getNewConfig(defaultSchema, configBuilder.build());

        //创建Planner
        //TODO: 可缓存
        Planner planner = CalcitePlanner.getPlannerFromFrameworkConfig(config);

        //TODO: 重复执行
        SqlNode parseNode = planner.parse(sql);

        //2.Validate
        SqlNode validateNode = CalciteSqlValidation.validateFromPlanner(planner, parseNode);

        //3.Logical Plan
        RelRoot relRoot = CalciteSqlNode.fromSqlNodeToRelRootViaPlanner(planner, validateNode);
        //打印优化前执行计划
        CalciteRel.explain(relRoot.project());

        //4.优化阶段
        //TODO: 可缓存
        HepPlanner hepPlanner = CalcitePlanner.createHepPlanner();

        //TODO: 重复执行
        hepPlanner.setRoot(relRoot.project());
        RelNode bestRelNode = hepPlanner.findBestExp();
        //打印优化后执行计划
        CalciteRel.explain(bestRelNode);

        //5.执行
        //TODO: 重复执行
        return CalciteRel.executeQueryFromConnection(bestRelNode, calciteConnection);
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
