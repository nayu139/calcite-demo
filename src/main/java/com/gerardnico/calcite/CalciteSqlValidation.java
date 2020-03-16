package com.gerardnico.calcite;

import com.gerardnico.calcite.mock.MockCatalogReaderSimple;
import com.gerardnico.calcite.mock.MockJdbcCatalogReader;
import com.gerardnico.calcite.mock.MockSqlOperatorTable;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.ValidationException;

public class CalciteSqlValidation {


    static void validateFromPlanner(Planner planner, SqlNode sqlNode) throws ValidationException {
        planner.validate(sqlNode);
    }

    static void validateFromCustomValidator(SqlNode sqlNode)  {
        createCustomSqlValidator().validate(sqlNode);
    }

    /**
     * Sample code that returns a custom sql validator
     * @return
     */
    static public CalciteSqlValidatorCustom createCustomSqlValidator() {

        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        MockJdbcCatalogReader catalogReader = new MockCatalogReaderSimple(typeFactory, true).init();
        MockSqlOperatorTable sqlOperatorTable = new MockSqlOperatorTable(SqlStdOperatorTable.instance());
        MockSqlOperatorTable.addRamp(sqlOperatorTable);

        CalciteSqlValidatorCustom sqlValidator = new CalciteSqlValidatorCustom(
                sqlOperatorTable,
                catalogReader,
                typeFactory,
                SqlConformanceEnum.DEFAULT);

        sqlValidator.setEnableTypeCoercion(true);

        final CalciteConnectionConfig calciteConnectionConfig = CalciteConnectionStatic.getContext().unwrap(CalciteConnectionConfig.class);
        if (calciteConnectionConfig != null) {
            sqlValidator.setDefaultNullCollation(calciteConnectionConfig.defaultNullCollation());
        }

        return sqlValidator;
    }
}