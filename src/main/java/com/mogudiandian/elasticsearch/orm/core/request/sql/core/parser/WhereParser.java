package com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.ParseUtils;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.SQLFunctions;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.*;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.spatial.SpatialParamsFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by allwefantasy on 9/2/16.
 */
public class WhereParser {

    private MySqlSelectQueryBlock query;

    private SQLExpr where;

    private SqlParser sqlParser;

    public WhereParser(SqlParser sqlParser, MySqlSelectQueryBlock query) {
        this.sqlParser = sqlParser;
        this.query = query;
        this.where = query.getWhere();
    }

    public WhereParser(SqlParser sqlParser, SQLExpr expr) {
        this.sqlParser = sqlParser;
        this.where = expr;
    }

    public WhereParser(SqlParser sqlParser) {
        this.sqlParser = sqlParser;
    }

    public Where findWhere() {
        if (where == null) {
            return null;
        }

        Where myWhere = Where.newInstance();
        parseWhere(where, myWhere);
        return myWhere;
    }

    public void parseWhere(SQLExpr expr, Where where) {

        /*
         * SQLBinaryOpExpr举例：
         *   eg1：a = 1
         *   eg2：a = 1 AND b = 2 OR c = 3
         */
        if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr bExpr = (SQLBinaryOpExpr) expr;
            if (explanSpecialCondWithBothSidesAreLiterals(bExpr, where)) {
                return;
            }
            if (explanSpecialCondWithBothSidesAreProperty(bExpr, where)) {
                return;
            }
        }

        if (expr instanceof SQLBinaryOpExpr && !isCondition((SQLBinaryOpExpr) expr)) {
            SQLBinaryOpExpr bExpr = (SQLBinaryOpExpr) expr;
            routeCond(bExpr, bExpr.getLeft(), where);
            routeCond(bExpr, bExpr.getRight(), where);
        } else if (expr instanceof SQLNotExpr) {
            parseWhere(((SQLNotExpr) expr).getExpr(), where);
            negateWhere(where);
        } else {
            explainCondition("AND", expr, where);
        }
    }

    private void negateWhere(Where where) {
        for (Where subWhere : where.getWheres()) {
            if (subWhere instanceof Condition) {
                Condition subCondition = (Condition) subWhere;
                subCondition.setEsOperator(subCondition.getEsOperator().negative());
            } else {
                negateWhere(subWhere);
            }
            subWhere.setConn(subWhere.getConn().negative());
        }
    }

    // some where conditions eg. 1=1 or 3>2 or 'a'='b'
    private boolean explanSpecialCondWithBothSidesAreLiterals(SQLBinaryOpExpr bExpr, Where where) {
        if ((bExpr.getLeft() instanceof SQLNumericLiteralExpr || bExpr.getLeft() instanceof SQLCharExpr) &&
                (bExpr.getRight() instanceof SQLNumericLiteralExpr || bExpr.getRight() instanceof SQLCharExpr)
        ) {
            SQLMethodInvokeExpr sqlMethodInvokeExpr = new SQLMethodInvokeExpr("script", null);
            String operator = bExpr.getOperator().getName();
            if (operator.equals("=")) {
                operator = "==";
            }
            sqlMethodInvokeExpr.addParameter(
                    new SQLCharExpr(ParseUtils.expr2Object(bExpr.getLeft(), "'") +
                            " " + operator + " " +
                            ParseUtils.expr2Object(bExpr.getRight(), "'"))
            );

            explainCondition("AND", sqlMethodInvokeExpr, where);
            return true;
        }
        return false;
    }

    // some where conditions eg. field1=field2 or field1>field2
    private boolean explanSpecialCondWithBothSidesAreProperty(SQLBinaryOpExpr bExpr, Where where) {
        // join is not support
        if ((bExpr.getLeft() instanceof SQLPropertyExpr || bExpr.getLeft() instanceof SQLIdentifierExpr)
                && (bExpr.getRight() instanceof SQLPropertyExpr || bExpr.getRight() instanceof SQLIdentifierExpr)
                && Sets.newHashSet("=", "<", ">", ">=", "<=", "<>", "!=").contains(bExpr.getOperator().getName())) {
            SQLMethodInvokeExpr sqlMethodInvokeExpr = new SQLMethodInvokeExpr("script", null);
            String operator = bExpr.getOperator().getName();
            if (operator.equals("=")) {
                operator = "==";
            } else if (operator.equals("<>")) {
                operator = "!=";
            }

            String leftProperty = ParseUtils.expr2Object(bExpr.getLeft()).toString();
            String rightProperty = ParseUtils.expr2Object(bExpr.getRight()).toString();
            if (leftProperty.split("\\.").length > 1) {

                leftProperty = leftProperty.substring(leftProperty.split("\\.")[0].length() + 1);
            }

            if (rightProperty.split("\\.").length > 1) {
                rightProperty = rightProperty.substring(rightProperty.split("\\.")[0].length() + 1);
            }

            sqlMethodInvokeExpr.addParameter(new SQLCharExpr(
                    "doc['" + leftProperty + "'].value " +
                            operator +
                            " doc['" + rightProperty + "'].value"));


            explainCondition("AND", sqlMethodInvokeExpr, where);
            return true;
        }
        return false;
    }

    // 判断是不是一个判断条件，例如：a=1 或者 floor(a)=1这种最小的单元
    private boolean isCondition(SQLBinaryOpExpr expr) {
        SQLExpr leftSide = expr.getLeft();
        if (leftSide instanceof SQLMethodInvokeExpr) {
            return isAllowedMethodOnConditionLeft((SQLMethodInvokeExpr) leftSide, expr.getOperator());
        }
        return leftSide instanceof SQLIdentifierExpr
                || leftSide instanceof SQLPropertyExpr
                || leftSide instanceof SQLVariantRefExpr
                || leftSide instanceof SQLCastExpr;
    }

    private boolean isAllowedMethodOnConditionLeft(SQLMethodInvokeExpr method, SQLBinaryOperator operator) {
        return !operator.isLogical()
                && (method.getMethodName().toLowerCase().equals("nested")
                || method.getMethodName().toLowerCase().equals("children")
                || SQLFunctions.buildInFunctions.contains(method.getMethodName().toLowerCase()));
    }


    private void routeCond(SQLBinaryOpExpr bExpr, SQLExpr sub, Where where) {
        if (sub instanceof SQLBinaryOpExpr
                && (!isCondition((SQLBinaryOpExpr) sub)
                || (((SQLBinaryOpExpr) sub).getLeft() instanceof SQLIdentifierExpr
                && ((SQLBinaryOpExpr) sub).getRight() instanceof SQLIdentifierExpr))) {
            SQLBinaryOpExpr binarySub = (SQLBinaryOpExpr) sub;
            if (binarySub.getOperator().priority != bExpr.getOperator().priority) {
                Where subWhere = new Where(bExpr.getOperator().name);
                where.addWhere(subWhere);
                // 递归调用parseWhere()，解析出where子句中的多个条件
                parseWhere(binarySub, subWhere);
            } else {
                // 递归调用parseWhere()，解析出where子句中的多个条件
                parseWhere(binarySub, where);
            }
        } else if (sub instanceof SQLNotExpr) {
            Where subWhere = new Where(bExpr.getOperator().name);
            where.addWhere(subWhere);
            // 递归调用parseWhere()，解析出where子句中的多个条件
            parseWhere(((SQLNotExpr) sub).getExpr(), subWhere);
            negateWhere(subWhere);
        } else {
            explainCondition(bExpr.getOperator().name, sub, where);
        }
    }

    private void explainCondition(String operator, SQLExpr expr, Where where) {
        if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlExpr = (SQLBinaryOpExpr) expr;

            if (explanSpecialCondWithBothSidesAreLiterals(sqlExpr, where)) {
                return;
            }
            if (explanSpecialCondWithBothSidesAreProperty(sqlExpr, where)) {
                return;
            }

            boolean methodAsOperator = false, isNested = false, isChildren = false;

            NestedType nestedType = new NestedType();
            if (nestedType.tryFillFromExpr(sqlExpr.getLeft())) {
                sqlExpr.setLeft(new SQLIdentifierExpr(nestedType.field));
                isNested = true;
            }

            ChildrenType childrenType = new ChildrenType();
            if (childrenType.tryFillFromExpr(sqlExpr.getLeft())) {
                sqlExpr.setLeft(new SQLIdentifierExpr(childrenType.field));
                isChildren = true;
            }

            if (sqlExpr.getRight() instanceof SQLMethodInvokeExpr) {
                SQLMethodInvokeExpr method = (SQLMethodInvokeExpr) sqlExpr.getRight();
                String methodName = method.getMethodName().toLowerCase();

                if (ESOperator.methodNameToOperator.containsKey(methodName)) {
                    Object[] methodParametersValue = getMethodValuesWithSubQueries(method);

                    Condition condition;
                    // fix OPERATOR
                    ESOperator esOperator = ESOperator.methodNameToOperator.get(methodName);
                    if (sqlExpr.getOperator() == SQLBinaryOperator.LessThanOrGreater || sqlExpr.getOperator() == SQLBinaryOperator.NotEqual) {
                        esOperator = esOperator.negative();
                    }
                    if (isNested) {
                        condition = new Condition(Where.CONN.valueOf(operator), sqlExpr.getLeft().toString(), sqlExpr.getLeft(), esOperator, methodParametersValue, sqlExpr.getRight(), nestedType);
                    } else if (isChildren) {
                        condition = new Condition(Where.CONN.valueOf(operator), sqlExpr.getLeft().toString(), sqlExpr.getLeft(), esOperator, methodParametersValue, sqlExpr.getRight(), childrenType);
                    } else {
                        condition = new Condition(Where.CONN.valueOf(operator), sqlExpr.getLeft().toString(), sqlExpr.getLeft(), esOperator, methodParametersValue, sqlExpr.getRight(), null);
                    }
                    where.addWhere(condition);
                    methodAsOperator = true;
                }
            }

            if (!methodAsOperator) {
                Condition condition;

                if (isNested) {
                    condition = new Condition(Where.CONN.valueOf(operator), sqlExpr.getLeft().toString(), sqlExpr.getLeft(), sqlExpr.getOperator().name, parseValue(sqlExpr.getRight()), sqlExpr.getRight(), nestedType);
                } else if (isChildren) {
                    condition = new Condition(Where.CONN.valueOf(operator), sqlExpr.getLeft().toString(), sqlExpr.getLeft(), sqlExpr.getOperator().name, parseValue(sqlExpr.getRight()), sqlExpr.getRight(), childrenType);
                } else {
                    SQLMethodInvokeExpr sqlMethodInvokeExpr = parseSQLBinaryOpExprWhoIsConditionInWhere(sqlExpr);
                    if (sqlMethodInvokeExpr == null) {
                        condition = new Condition(Where.CONN.valueOf(operator), sqlExpr.getLeft().toString(), sqlExpr.getLeft(), sqlExpr.getOperator().name, parseValue(sqlExpr.getRight()), sqlExpr.getRight(), null);
                    } else {
                        ScriptFilter scriptFilter = new ScriptFilter();
                        if (!scriptFilter.tryParseFromMethodExpr(sqlMethodInvokeExpr)) {
                            throw new SqlParseException("could not parse script filter");
                        }
                        condition = new Condition(Where.CONN.valueOf(operator), null, null, "SCRIPT", scriptFilter, null);

                    }
                }
                where.addWhere(condition);
            }
        } else if (expr instanceof SQLInListExpr) {
            // 解析in和not in语句
            SQLInListExpr siExpr = (SQLInListExpr) expr;
            String leftSide = siExpr.getExpr().toString();

            boolean isChildren = false;

            NestedType nestedType = new NestedType();
            if (nestedType.tryFillFromExpr(siExpr.getExpr())) {
                leftSide = nestedType.field;
            }

            ChildrenType childrenType = new ChildrenType();
            if (childrenType.tryFillFromExpr(siExpr.getExpr())) {
                leftSide = childrenType.field;
                isChildren = true;
            }

            Condition condition;

            if (isChildren) {
                condition = new Condition(Where.CONN.valueOf(operator), leftSide, null, siExpr.isNot() ? "NOT IN" : "IN", parseValue(siExpr.getTargetList()), null, childrenType);
            } else if (siExpr.getExpr() instanceof SQLCaseExpr) {
                condition = new Condition(Where.CONN.valueOf(operator), leftSide, siExpr.getExpr(), siExpr.isNot() ? "NOT IN" : "IN", parseValue(siExpr.getTargetList()), null);
            } else {
                condition = new Condition(Where.CONN.valueOf(operator), leftSide, null, siExpr.isNot() ? "NOT IN" : "IN", parseValue(siExpr.getTargetList()), null);
            }
            where.addWhere(condition);
        } else if (expr instanceof SQLBetweenExpr) {
            SQLBetweenExpr between = ((SQLBetweenExpr) expr);
            String leftSide = between.getTestExpr().toString();

            boolean isNested = false;
            boolean isChildren = false;

            NestedType nestedType = new NestedType();
            if (nestedType.tryFillFromExpr(between.getTestExpr())) {
                leftSide = nestedType.field;
                isNested = true;
            }

            ChildrenType childrenType = new ChildrenType();
            if (childrenType.tryFillFromExpr(between.getTestExpr())) {
                leftSide = childrenType.field;
                isChildren = true;
            }

            Condition condition;

            if (isNested) {
                condition = new Condition(Where.CONN.valueOf(operator), leftSide, null, between.isNot() ? "NOT BETWEEN" : "BETWEEN", new Object[]{parseValue(between.beginExpr), parseValue(between.endExpr)}, null, nestedType);
            } else if (isChildren) {
                condition = new Condition(Where.CONN.valueOf(operator), leftSide, null, between.isNot() ? "NOT BETWEEN" : "BETWEEN", new Object[]{parseValue(between.beginExpr), parseValue(between.endExpr)}, null, childrenType);
            } else {
                condition = new Condition(Where.CONN.valueOf(operator), leftSide, null, between.isNot() ? "NOT BETWEEN" : "BETWEEN", new Object[]{parseValue(between.beginExpr), parseValue(between.endExpr)}, null, null);
            }
            where.addWhere(condition);
        } else if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodExpr = (SQLMethodInvokeExpr) expr;
            List<SQLExpr> methodParameters = methodExpr.getParameters();

            String methodName = methodExpr.getMethodName();
            if (SpatialParamsFactory.isAllowedMethod(methodName)) {
                String fieldName = methodParameters.get(0).toString();

                boolean isNested = false;
                boolean isChildren = false;

                NestedType nestedType = new NestedType();
                if (nestedType.tryFillFromExpr(methodParameters.get(0))) {
                    fieldName = nestedType.field;
                    isNested = true;
                }

                ChildrenType childrenType = new ChildrenType();
                if (childrenType.tryFillFromExpr(methodParameters.get(0))) {
                    fieldName = childrenType.field;
                    isChildren = true;
                }

                Object spatialParamsObject = SpatialParamsFactory.generateSpatialParamsObject(methodName, methodParameters);

                Condition condition;

                if (isNested) {
                    condition = new Condition(Where.CONN.valueOf(operator), fieldName, null, methodName, spatialParamsObject, null, nestedType);
                } else if (isChildren) {
                    condition = new Condition(Where.CONN.valueOf(operator), fieldName, null, methodName, spatialParamsObject, null, childrenType);
                } else {
                    condition = new Condition(Where.CONN.valueOf(operator), fieldName, null, methodName, spatialParamsObject, null, null);
                }

                where.addWhere(condition);
            } else if (methodName.toLowerCase().equals("nested")) {
                NestedType nestedType = new NestedType();

                if (!nestedType.tryFillFromExpr(expr)) {
                    throw new SqlParseException("could not fill nested from expr:" + expr);
                }

                Condition condition = new Condition(Where.CONN.valueOf(operator), nestedType.path, null, methodName.toUpperCase(), nestedType.where, null, nestedType);

                where.addWhere(condition);
            } else if (methodName.toLowerCase().equals("children")) {
                ChildrenType childrenType = new ChildrenType();

                if (!childrenType.tryFillFromExpr(expr)) {
                    throw new SqlParseException("could not fill children from expr:" + expr);
                }

                Condition condition = new Condition(Where.CONN.valueOf(operator), childrenType.childType, null, methodName.toUpperCase(), childrenType.where, null);

                where.addWhere(condition);
            } else if (methodName.toLowerCase().equals("script")) {
                /*
                 * 这里也是Script Query，但是貌似没见过有走这个分支的sql
                 * 1、文档 https://www.elastic.co/guide/en/elasticsearch/reference/6.1/query-dsl-script-query.html
                 * 2、java api https://www.elastic.co/guide/en/elasticsearch/client/java-api/6.1/java-specialized-queries.html
                 */
                ScriptFilter scriptFilter = new ScriptFilter();
                if (!scriptFilter.tryParseFromMethodExpr(methodExpr)) {
                    throw new SqlParseException("could not parse script filter");
                }
                Condition condition = new Condition(Where.CONN.valueOf(operator), null, null, "SCRIPT", scriptFilter, null);
                where.addWhere(condition);
            } else {
                throw new SqlParseException("unsupported method: " + methodName);
            }
        } else {
            throw new SqlParseException("err find condition " + expr.getClass());
        }
    }

    private MethodField parseSQLMethodInvokeExprWithFunctionInWhere(SQLMethodInvokeExpr soExpr) {
        return FieldMaker.makeMethodField(soExpr.getMethodName(), soExpr.getParameters(), null, null, query != null ? query.getFrom().getAlias() : null, false);
    }

    private MethodField parseSQLCastExprInWhere(SQLCastExpr soExpr) {
        MethodField methodField = FieldMaker.makeMethodField("cast", Collections.singletonList(soExpr), null, null, query != null ? query.getFrom().getAlias() : null, true);
        List<KVValue> params = methodField.getParams();
        KVValue param = params.get(0);
        params.clear();
        params.add(new KVValue(param.getKey()));
        params.add(new KVValue(param.getValue()));
        return methodField;
    }

    private SQLMethodInvokeExpr parseSQLBinaryOpExprWhoIsConditionInWhere(SQLBinaryOpExpr sqlExpr) {
        if (!(sqlExpr.getLeft() instanceof SQLCastExpr || sqlExpr.getRight() instanceof SQLCastExpr)) {
            if (!(sqlExpr.getLeft() instanceof SQLMethodInvokeExpr ||
                    sqlExpr.getRight() instanceof SQLMethodInvokeExpr)) {
                return null;
            }

            if (sqlExpr.getLeft() instanceof SQLMethodInvokeExpr) {
                if (!SQLFunctions.buildInFunctions.contains(((SQLMethodInvokeExpr) sqlExpr.getLeft()).getMethodName())) {
                    return null;
                }
            }

            if (sqlExpr.getRight() instanceof SQLMethodInvokeExpr) {
                if (!SQLFunctions.buildInFunctions.contains(((SQLMethodInvokeExpr) sqlExpr.getRight()).getMethodName())) {
                    return null;
                }
            }
        }

        MethodField leftMethod = new MethodField(null, Lists.newArrayList(new KVValue("", ParseUtils.expr2Object(sqlExpr.getLeft(), "'"))), null, null);
        if (sqlExpr.getLeft() instanceof SQLIdentifierExpr || sqlExpr.getLeft() instanceof SQLPropertyExpr) {
            leftMethod = new MethodField(null, Lists.newArrayList(new KVValue("", "doc['" + ParseUtils.expr2Object(sqlExpr.getLeft(), "'") + "'].value")), null, null);
        } else if (sqlExpr.getLeft() instanceof SQLMethodInvokeExpr) {
            leftMethod = parseSQLMethodInvokeExprWithFunctionInWhere((SQLMethodInvokeExpr) sqlExpr.getLeft());
        } else if (sqlExpr.getLeft() instanceof SQLCastExpr) {
            leftMethod = parseSQLCastExprInWhere((SQLCastExpr) sqlExpr.getLeft());
        }

        MethodField rightMethod = new MethodField(null, Lists.newArrayList(new KVValue("", ParseUtils.expr2Object(sqlExpr.getRight(), "'"))), null, null);
        if (sqlExpr.getRight() instanceof SQLIdentifierExpr || sqlExpr.getRight() instanceof SQLPropertyExpr) {
            rightMethod = new MethodField(null, Lists.newArrayList(new KVValue("", "doc['" + ParseUtils.expr2Object(sqlExpr.getRight(), "'") + "'].value")), null, null);
        } else if (sqlExpr.getRight() instanceof SQLMethodInvokeExpr) {
            rightMethod = parseSQLMethodInvokeExprWithFunctionInWhere((SQLMethodInvokeExpr) sqlExpr.getRight());
        } else if (sqlExpr.getRight() instanceof SQLCastExpr) {
            rightMethod = parseSQLCastExprInWhere((SQLCastExpr) sqlExpr.getRight());
        }

        String v1 = leftMethod.getParams().get(0).getValue().toString();
        String v1Dec = leftMethod.getParams().size() == 2 ? leftMethod.getParams().get(1).getValue().toString() + ";" : "";

        String v2 = rightMethod.getParams().get(0).getValue().toString();
        String v2Dec = rightMethod.getParams().size() == 2 ? rightMethod.getParams().get(1).getValue().toString() + ";" : "";

        String operator = sqlExpr.getOperator().getName();

        if ("=".equals(operator)) {
            operator = "==";
        }

        String finalStr = String.format("%s%s((Comparable)%s).compareTo(%s) %s 0", v1Dec, v2Dec, v1, v2, operator);

        SQLMethodInvokeExpr scriptMethod = new SQLMethodInvokeExpr("script", null);
        scriptMethod.addParameter(new SQLCharExpr(finalStr));
        return scriptMethod;

    }

    private Object[] getMethodValuesWithSubQueries(SQLMethodInvokeExpr method) {
        List<Object> values = new ArrayList<>();
        for (SQLExpr innerExpr : method.getParameters()) {
            if (innerExpr instanceof SQLQueryExpr) {
                Select select = sqlParser.parseSelect((MySqlSelectQueryBlock) ((SQLQueryExpr) innerExpr).getSubQuery().getQuery());
                values.add(new SubQueryExpression(select));
            } else if (innerExpr instanceof SQLTextLiteralExpr) {
                values.add(((SQLTextLiteralExpr) innerExpr).getText());
            } else {
                values.add(innerExpr);
            }

        }
        return values.toArray();
    }

    /**
     * 该放方法只用于解析in、not in括号中的列表，将括号中的多个值转为Object[]
     */
    private Object[] parseValue(List<SQLExpr> targetList) {
        Object[] value = new Object[targetList.size()];
        for (int i = 0; i < targetList.size(); i++) {
            value[i] = parseValue(targetList.get(i));
        }
        return value;
    }

    private Object parseValue(SQLExpr expr) {
        if (expr instanceof SQLNumericLiteralExpr) {
            Number number = ((SQLNumericLiteralExpr) expr).getNumber();
            if (number instanceof BigDecimal) {
                return number.doubleValue();
            }
            if (number instanceof BigInteger) {
                return number.longValue();
            }
            return ((SQLNumericLiteralExpr) expr).getNumber();
        } else if (expr instanceof SQLCharExpr) {
            return ((SQLCharExpr) expr).getText();
        } else if (expr instanceof SQLMethodInvokeExpr) {
            return expr;
        } else if (expr instanceof SQLNullExpr) {
            return null;
        } else if (expr instanceof SQLIdentifierExpr) {
            return expr;
        } else if (expr instanceof SQLPropertyExpr) {
            return expr;
        } else {
            // 解析where子查询时会抛出这样的异常
            throw new SqlParseException(String.format("Failed to parse SqlExpression of type %s. expression value: %s", expr.getClass(), expr));
        }
    }
}
