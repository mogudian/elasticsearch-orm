package com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.util.StringUtils;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.ParseUtils;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.SQLFunctions;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.Field;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.KVValue;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.MethodField;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.Where;
import com.google.common.collect.Lists;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 一些具有参数的一般在 select 函数.或者group by 函数
 *
 * @author ansj
 */
public class FieldMaker {
    public static Field makeField(SQLExpr expr, String alias, String tableAlias) {
        if (expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr || expr instanceof SQLVariantRefExpr) {
            return handleIdentifier(expr, alias, tableAlias);
        } else if (expr instanceof SQLQueryExpr) {
            throw new SqlParseException("unknow field name : " + expr);
        } else if (expr instanceof SQLBinaryOpExpr) {
            // make a SCRIPT method field;
            return makeField(makeBinaryMethodField((SQLBinaryOpExpr) expr), alias, tableAlias);

        } else if (expr instanceof SQLAllColumnExpr) {
            // 对应select * 的情况
        } else if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr mExpr = (SQLMethodInvokeExpr) expr;

            String methodName = mExpr.getMethodName();

            if (methodName.equalsIgnoreCase("nested") || methodName.equalsIgnoreCase("reverse_nested")) {
                NestedType nestedType = new NestedType();
                if (nestedType.tryFillFromExpr(mExpr)) {
                    return handleIdentifier(nestedType, alias, tableAlias);
                }
            } else if (methodName.equalsIgnoreCase("children")) {
                ChildrenType childrenType = new ChildrenType();
                if (childrenType.tryFillFromExpr(mExpr)) {
                    return handleIdentifier(childrenType, alias, tableAlias);
                }
            } else if (methodName.equalsIgnoreCase("filter")) {
                return makeFilterMethodField(mExpr, alias);
            } else if ("filters".equalsIgnoreCase(methodName)) {
                return makeFiltersMethodField(mExpr, alias);
            }

            return makeMethodField(methodName, mExpr.getParameters(), null, alias, tableAlias, true);
        } else if (expr instanceof SQLAggregateExpr) {
            SQLAggregateExpr sExpr = (SQLAggregateExpr) expr;
            return makeMethodField(sExpr.getMethodName(), sExpr.getArguments(), sExpr.getOption(), alias, tableAlias, true);
        } else if (expr instanceof SQLCaseExpr) {
            // case when 走这个分支
            String scriptCode = new CaseWhenParser((SQLCaseExpr) expr).parse();
            List<KVValue> methodParameters = new ArrayList<>();
            // group by子句中case when是没有别名的，这时alias=null
            if (alias != null && alias.trim().length() != 0) {
                methodParameters.add(new KVValue(alias));
            }
            methodParameters.add(new KVValue(scriptCode));
            return new MethodField("script", methodParameters, null, alias);
        } else if (expr instanceof SQLCastExpr) {
            SQLCastExpr castExpr = (SQLCastExpr) expr;
            if (alias == null) {
                alias = "cast_" + castExpr.getExpr().toString();
            }
            String scriptCode = new CastParser(castExpr).parse(true);
            List<KVValue> methodParameters = new ArrayList<>();
            methodParameters.add(new KVValue(alias));
            methodParameters.add(new KVValue(scriptCode));
            return new MethodField("script", methodParameters, null, alias);
        } else {
            throw new SqlParseException("unknown field name : " + expr);
        }
        return null;
    }

    private static Object getScriptValue(SQLExpr expr) {
        return ParseUtils.getScriptValue(expr);
    }

    private static Field makeScriptMethodField(SQLBinaryOpExpr binaryExpr, String tableAlias) {
        List<SQLExpr> params = new ArrayList<>();

        params.add(new SQLCharExpr(binaryExpr.toString()));

        Object left = getScriptValue(binaryExpr.getLeft());
        Object right = getScriptValue(binaryExpr.getRight());

        params.add(new SQLCharExpr(String.format("%s %s %s", left, binaryExpr.getOperator().getName(), right)));

        return makeMethodField("script", params, null, null, tableAlias, false);
    }


    private static Field makeFilterMethodField(SQLMethodInvokeExpr filterMethod, String alias) {
        List<SQLExpr> parameters = filterMethod.getParameters();
        int parametersSize = parameters.size();
        if (parametersSize != 1 && parametersSize != 2) {
            throw new SqlParseException("filter group by field should only have one or 2 parameters filter(Expr) or filter(name,Expr)");
        }
        String filterAlias = filterMethod.getMethodName();
        SQLExpr exprToCheck = null;
        if (parametersSize == 1) {
            exprToCheck = parameters.get(0);
            filterAlias = "filter(" + exprToCheck.toString().replaceAll("\n", " ") + ")";
        }
        if (parametersSize == 2) {
            filterAlias = ParseUtils.extendedToString(parameters.get(0));
            exprToCheck = parameters.get(1);
        }
        Where where = Where.newInstance();
        new WhereParser(new SqlParser()).parseWhere(exprToCheck, where);
        if (where.getWheres().size() == 0)
            throw new SqlParseException("unable to parse filter where.");
        List<KVValue> methodParameters = new ArrayList<>();
        methodParameters.add(new KVValue("where", where));
        methodParameters.add(new KVValue("alias", filterAlias + "@FILTER"));
        return new MethodField("filter", methodParameters, null, alias);
    }

    private static Field makeFiltersMethodField(SQLMethodInvokeExpr filtersMethod, String alias) {
        List<SQLExpr> parameters = filtersMethod.getParameters();
        int firstFilterMethod = -1;
        int parametersSize = parameters.size();
        for (int i = 0; i < parametersSize; ++i) {
            if (parameters.get(i) instanceof SQLMethodInvokeExpr) {
                firstFilterMethod = i;
                break;
            }
        }
        if (firstFilterMethod < 0) {
            throw new SqlParseException("filters group by field should have one more filter methods");
        }

        String filtersAlias = filtersMethod.getMethodName();
        String otherBucketKey = null;
        if (0 < firstFilterMethod) {
            filtersAlias = ParseUtils.extendedToString(parameters.get(0));
            if (1 < firstFilterMethod) {
                otherBucketKey = ParseUtils.extendedToString(parameters.get(1));
            }
        }
        List<Field> filterFields = new ArrayList<>();
        for (SQLExpr expr : parameters.subList(firstFilterMethod, parametersSize)) {
            filterFields.add(makeFilterMethodField((SQLMethodInvokeExpr) expr, null));
        }
        List<KVValue> methodParameters = new ArrayList<>();
        methodParameters.add(new KVValue("alias", filtersAlias + "@FILTERS"));
        methodParameters.add(new KVValue("otherBucketKey", otherBucketKey));
        methodParameters.add(new KVValue("filters", filterFields));
        return new MethodField("filters", methodParameters, null, alias);
    }

    private static Field handleIdentifier(NestedType nestedType, String alias, String tableAlias) {
        Field field = handleIdentifier(new SQLIdentifierExpr(nestedType.field), alias, tableAlias);
        field.setNested(nestedType);
        field.setChildren(null);
        return field;
    }

    private static Field handleIdentifier(ChildrenType childrenType, String alias, String tableAlias) {
        Field field = handleIdentifier(new SQLIdentifierExpr(childrenType.field), alias, tableAlias);
        field.setNested(null);
        field.setChildren(childrenType);
        return field;
    }

    // binary method can nested
    public static SQLMethodInvokeExpr makeBinaryMethodField(SQLBinaryOpExpr expr) {
        switch (expr.getOperator()) {
            case Add:
                return convertBinaryOperatorToMethod("add", expr);
            case Multiply:
                return convertBinaryOperatorToMethod("multiply", expr);
            case Divide:
                return convertBinaryOperatorToMethod("divide", expr);
            case Modulus:
                return convertBinaryOperatorToMethod("modulus", expr);
            case Subtract:
                return convertBinaryOperatorToMethod("subtract", expr);
            default:
                throw new SqlParseException(expr.getOperator().getName() + " is not support");
        }
    }

    private static SQLMethodInvokeExpr convertBinaryOperatorToMethod(String operator, SQLBinaryOpExpr expr) {
        SQLMethodInvokeExpr methodInvokeExpr = new SQLMethodInvokeExpr(operator, null);
        methodInvokeExpr.addParameter(expr.getLeft());
        methodInvokeExpr.addParameter(expr.getRight());
        return methodInvokeExpr;
    }

    private static Field handleIdentifier(SQLExpr expr, String alias, String tableAlias) {
        String name = expr.toString().replace("`", "");
        String newFieldName = name;
        Field field = null;
        if (tableAlias != null) {
            String aliasPrefix = tableAlias + ".";
            if (name.startsWith(aliasPrefix)) {
                newFieldName = name.replaceFirst(aliasPrefix, "");
                field = new Field(newFieldName, alias);
            }
        }

        if (tableAlias == null) {
            field = new Field(newFieldName, alias);
        }

        // 字段的别名不为空 && 别名和字段名不一样
        if (alias != null && !alias.equals(name)) {

            /*
             * newFieldName是字段原来的名字，这句话应该是用于es dsl的
             * 使用别名有很多种情况：
             *     1、最简单的就是select field_1 as a from tbl
             *     2、调用函数处理字段之后，select floor(field_1) as a from tbl
             *     3、执行表达式，select field_1 + field_2 as a from tbl
             *     4、case when field_1='a' then 'haha' else 'hehe' end as a
             *     5、........
             * 所以这个if分支就是为了处理以上这些情况的
             */
            List<SQLExpr> parameters = Lists.newArrayList();
            // 别名
            parameters.add(new SQLCharExpr(alias));
            parameters.add(new SQLCharExpr("doc['" + newFieldName + "'].value"));
            field = makeMethodField("script", parameters, null, alias, tableAlias, true);
        }
        return field;
    }

    public static MethodField makeMethodField(String name, List<SQLExpr> arguments, SQLAggregateOption option, String alias, String tableAlias, boolean first) {
        List<KVValue> parameters = new LinkedList<>();
        String finalMethodName = name;
        // 默认的二元操作符为 ==
        String binaryOperatorName = null;
        List<String> binaryOperatorNames = new ArrayList<>();
        for (SQLExpr object : arguments) {

            if (object instanceof SQLBinaryOpExpr) {

                SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) object;

                if (SQLFunctions.buildInFunctions.contains(binaryOpExpr.getOperator().toString().toLowerCase())) {
                    SQLMethodInvokeExpr mExpr = makeBinaryMethodField(binaryOpExpr);
                    MethodField mf = makeMethodField(mExpr.getMethodName(), mExpr.getParameters(), null, null, tableAlias, false);
                    String key = mf.getParams().get(0).toString(), value = mf.getParams().get(1).toString();
                    parameters.add(new KVValue(key, new SQLCharExpr(first && !SQLFunctions.buildInFunctions.contains(finalMethodName) ? String.format("%s;return %s;", value, key) : value)));
                } else {
                    // 增加 =、!= 以外二元操作符的支持
                    binaryOperatorName = binaryOpExpr.getOperator().getName().trim();
                    if (SQLFunctions.binaryOperators.contains(binaryOperatorName)) {
                        binaryOperatorNames.add(binaryOperatorName);
                        SQLExpr right = binaryOpExpr.getRight();

                        Object value = ParseUtils.expr2Object(right);

                        // 如果语法的二元操作符的值如果有引号，不能去掉
                        // select name, if(gender='m','男','女') as myGender from bank LIMIT 0, 10
                        if (binaryOpExpr.getParent() instanceof SQLMethodInvokeExpr) {
                            String methodName = ((SQLMethodInvokeExpr) binaryOpExpr.getParent()).getMethodName();
                            if ("if".equals(methodName) || "case".equals(methodName) || "case_new".equals(methodName)) {
                                value = ParseUtils.expr2Object(right, "'");
                            }
                        }
                        parameters.add(new KVValue(binaryOpExpr.getLeft().toString(), value));
                    } else {
                        parameters.add(new KVValue("script", makeScriptMethodField(binaryOpExpr, tableAlias)));
                    }
                }
            } else if (object instanceof SQLMethodInvokeExpr) {
                SQLMethodInvokeExpr mExpr = (SQLMethodInvokeExpr) object;
                String methodName = mExpr.getMethodName().toLowerCase();
                if (methodName.equals("script")) {
                    KVValue script = new KVValue("script", makeMethodField(mExpr.getMethodName(), mExpr.getParameters(), null, alias, tableAlias, true));
                    parameters.add(script);
                } else if (methodName.equals("nested") || methodName.equals("reverse_nested")) {
                    NestedType nestedType = new NestedType();

                    if (!nestedType.tryFillFromExpr(object)) {
                        throw new SqlParseException("failed parsing nested expr " + object);
                    }

                    parameters.add(new KVValue("nested", nestedType));
                } else if (methodName.equals("children")) {
                    ChildrenType childrenType = new ChildrenType();

                    if (!childrenType.tryFillFromExpr(object)) {
                        throw new SqlParseException("failed parsing children expr " + object);
                    }

                    parameters.add(new KVValue("children", childrenType));
                } else if (SQLFunctions.buildInFunctions.contains(methodName)) {
                    // 用于聚合查询时支持if、case_new 函数生成新的值
                    if (mExpr.getParent() instanceof SQLAggregateExpr) {
                        KVValue script = new KVValue("script", makeMethodField(mExpr.getMethodName(), mExpr.getParameters(), null, alias, tableAlias, true));
                        parameters.add(script);
                    } else {
                        MethodField mf = makeMethodField(methodName, mExpr.getParameters(), null, null, tableAlias, false);
                        String key = mf.getParams().get(0).toString(), value = mf.getParams().get(1).toString();
                        parameters.add(new KVValue(key, new SQLCharExpr(first && !SQLFunctions.buildInFunctions.contains(finalMethodName) ? String.format("%s;return %s;", value, key) : value)));
                    }
                } else {
                    throw new SqlParseException("only support script/nested/children as inner functions");
                }
            } else if (object instanceof SQLCaseExpr) {
                String scriptCode = new CaseWhenParser((SQLCaseExpr) object).parse();
                parameters.add(new KVValue("script", new SQLCharExpr(scriptCode)));
            } else if (object instanceof SQLCastExpr) {
                CastParser castParser = new CastParser((SQLCastExpr) object);
                String scriptCode = castParser.parse(false);
                parameters.add(new KVValue(castParser.getName(), new SQLCharExpr(scriptCode)));
            } else {
                parameters.add(new KVValue(ParseUtils.removeTableAliasFromField(object, tableAlias)));
            }

        }
        // script字段不会走这个分支
        if (SQLFunctions.buildInFunctions.contains(finalMethodName.toLowerCase())) {
            if (alias == null && first) {
                alias = "field_" + SQLFunctions.random(); // parameter.get(0).value.toString();
            }
            // should check if field and first .
            Tuple<String, String> newFunctions = null;
            try {
                // 构造script时，二元操作符可能是多样的 case_new 语法，需要 binaryOperatorNames 参数
                newFunctions = SQLFunctions.function(finalMethodName, parameters, parameters.get(0).getKey(), first, binaryOperatorName, binaryOperatorNames);
            } catch (Exception e) {
                e.printStackTrace();
            }
            parameters.clear();
            if (!first) {
                // variance
                parameters.add(new KVValue(newFunctions.v1()));
            } else {
                if (newFunctions.v1().toLowerCase().contains("if")) {
                    // 如果有用户指定的别名，则不使用自动生成的别名
                    if (!StringUtils.isEmpty(alias) && !alias.startsWith("field_")) {
                        parameters.add(new KVValue(alias));
                    } else {
                        parameters.add(new KVValue(newFunctions.v1()));
                    }
                } else {
                    parameters.add(new KVValue(alias));
                }
            }

            parameters.add(new KVValue(newFunctions.v2()));
            finalMethodName = "script";
        }
        if (first) {
            List<KVValue> tempParameters = new LinkedList<>();
            for (KVValue temp : parameters) {
                if (temp.getValue() instanceof SQLExpr) {
                    tempParameters.add(new KVValue(temp.getKey(), ParseUtils.expr2Object((SQLExpr) temp.getValue())));
                } else {
                    tempParameters.add(new KVValue(temp.getKey(), temp.getValue()));
                }
            }
            parameters.clear();
            parameters.addAll(tempParameters);
        }

        return new MethodField(finalMethodName, parameters, option == null ? null : option.name(), alias);
    }
}
