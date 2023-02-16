package com.mogudiandian.elasticsearch.orm.core.request.sql.core;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.KVValue;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser.SqlParseException;

import java.util.List;

public class ParseUtils {

    public static String joiner(List<KVValue> list, String operator) {
        if (list == null || list.isEmpty()) {
            return null;
        }

        StringBuilder builder = new StringBuilder(list.get(0).toString());
        for (int i = 1; i < list.size(); i++) {
            builder.append(operator);
            builder.append(list.get(i).toString());
        }

        return builder.toString();
    }

    public static Object removeTableAliasFromField(Object expr, String tableAlias) {
        if (expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr || expr instanceof SQLVariantRefExpr) {
            String name = expr.toString().replace("`", "");
            if (tableAlias != null) {
                String aliasPrefix = tableAlias + ".";
                if (name.startsWith(aliasPrefix)) {
                    String newFieldName = name.replaceFirst(aliasPrefix, "");
                    return new SQLIdentifierExpr(newFieldName);
                }
            }
        }
        return expr;
    }

    public static Object expr2Object(SQLExpr expr) {
        return expr2Object(expr, "");
    }

    public static Object expr2Object(SQLExpr expr, String charWithQuote) {
        Object value = null;
        if (expr instanceof SQLNumericLiteralExpr) {
            value = ((SQLNumericLiteralExpr) expr).getNumber();
        } else if (expr instanceof SQLCharExpr) {
            value = charWithQuote + ((SQLCharExpr) expr).getText() + charWithQuote;
        } else if (expr instanceof SQLIdentifierExpr) {
            value = expr.toString();
        } else if (expr instanceof SQLPropertyExpr) {
            value = expr.toString();
        } else if (expr instanceof SQLVariantRefExpr) {
            value = expr.toString();
        } else if (expr instanceof SQLAllColumnExpr) {
            value = "*";
        } else if (expr instanceof SQLValuableExpr) {
            value = ((SQLValuableExpr) expr).getValue();
        }
        return value;
    }

    public static Object getScriptValue(SQLExpr expr) {
        if (expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr || expr instanceof SQLVariantRefExpr) {
            return "doc['" + expr + "'].value";
        } else if (expr instanceof SQLValuableExpr) {
            return ((SQLValuableExpr) expr).getValue();
        }
        throw new SqlParseException("could not parse sqlBinaryOpExpr need to be identifier/valuable got" + expr.getClass().toString() + " with value:" + expr.toString());
    }

    public static Object getScriptValueWithQuote(SQLExpr expr, String quote) {
        if (expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr || expr instanceof SQLVariantRefExpr) {
            return "doc['" + expr + "'].value";
        } else if (expr instanceof SQLCharExpr) {
            return quote + ((SQLCharExpr) expr).getValue() + quote;
        } else if (expr instanceof SQLIntegerExpr) {
            return ((SQLIntegerExpr) expr).getValue();
        } else if (expr instanceof SQLNumericLiteralExpr) {
            return ((SQLNumericLiteralExpr) expr).getNumber();
        } else if (expr instanceof SQLNullExpr) {
            return expr.toString().toLowerCase();
        } else if (expr instanceof SQLBinaryOpExpr) {
            String left = "doc['" + ((SQLBinaryOpExpr) expr).getLeft().toString() + "'].value";
            String operator = ((SQLBinaryOpExpr) expr).getOperator().getName();
            String right = "doc['" + ((SQLBinaryOpExpr) expr).getRight().toString() + "'].value";
            return left + operator + right;
        }
        throw new SqlParseException("could not parse sqlBinaryOpExpr need to be identifier/valuable got " + expr.getClass().toString() + " with value:" + expr.toString());
    }

    public static String extendedToString(SQLExpr sqlExpr) {
        if (sqlExpr instanceof SQLTextLiteralExpr) {
            return ((SQLTextLiteralExpr) sqlExpr).getText();
        }
        return sqlExpr.toString();
    }

}
