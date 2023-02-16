package com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.ParseUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.query.*;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class Parameter {

    private String analysis;

    private Float boost;

    public String value;

    public Integer slop;

    private Map<String, Float> fieldsBoosts = new HashMap<>();

    private String type;

    private Float tieBreaker;

    private Operator operator;

    private String defaultField;

    private String minimumShouldMatch;

    private Boolean inOrder;

    public String clauses;

    public static Parameter parseParameter(SQLMethodInvokeExpr method) {
        Parameter instance = new Parameter();
        List<SQLExpr> parameters = method.getParameters();
        for (SQLExpr expr : parameters) {
            if (expr instanceof SQLCharExpr) {
                if (instance.value == null) {
                    instance.value = ((SQLCharExpr) expr).getText();
                } else {
                    instance.analysis = ((SQLCharExpr) expr).getText();
                }
            } else if (expr instanceof SQLNumericLiteralExpr) {
                instance.boost = ((SQLNumericLiteralExpr) expr).getNumber().floatValue();
            } else if (expr instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr sqlExpr = (SQLBinaryOpExpr) expr;
                switch (ParseUtils.expr2Object(sqlExpr.getLeft()).toString()) {
                    case "query":
                        instance.value = ParseUtils.expr2Object(sqlExpr.getRight()).toString();
                        break;
                    case "analyzer":
                        instance.analysis = ParseUtils.expr2Object(sqlExpr.getRight()).toString();
                        break;
                    case "boost":
                        instance.boost = Float.parseFloat(ParseUtils.expr2Object(sqlExpr.getRight()).toString());
                        break;
                    case "slop":
                        instance.slop = Integer.parseInt(ParseUtils.expr2Object(sqlExpr.getRight()).toString());
                        break;

                    case "fields":
                        int index;
                        for (String f : Strings.splitStringByCommaToArray(ParseUtils.expr2Object(sqlExpr.getRight()).toString())) {
                            index = f.lastIndexOf('^');
                            if (-1 < index) {
                                instance.fieldsBoosts.put(f.substring(0, index), Float.parseFloat(f.substring(index + 1)));
                            } else {
                                instance.fieldsBoosts.put(f, 1.0F);
                            }
                        }
                        break;
                    case "type":
                        instance.type = ParseUtils.expr2Object(sqlExpr.getRight()).toString();
                        break;
                    case "tie_breaker":
                        instance.tieBreaker = Float.parseFloat(ParseUtils.expr2Object(sqlExpr.getRight()).toString());
                        break;
                    case "ESOperator":
                        instance.operator = Operator.fromString(ParseUtils.expr2Object(sqlExpr.getRight()).toString());
                        break;

                    case "default_field":
                        instance.defaultField = ParseUtils.expr2Object(sqlExpr.getRight()).toString();
                        break;

                    case "in_order":
                        instance.inOrder = Boolean.valueOf(ParseUtils.expr2Object(sqlExpr.getRight()).toString());
                        break;
                    case "clauses":
                        instance.clauses = ParseUtils.expr2Object(sqlExpr.getRight()).toString();
                        break;
                    case "minimum_should_match":
                        instance.minimumShouldMatch = ParseUtils.expr2Object(sqlExpr.getRight()).toString();
                        break;

                    default:
                        break;
                }
            }
        }

        return instance;
    }

    public static ToXContent fullParameter(MatchPhraseQueryBuilder query, Parameter parameter) {
        if (parameter.analysis != null) {
            query.analyzer(parameter.analysis);
        }

        if (parameter.boost != null) {
            query.boost(parameter.boost);
        }

        if (parameter.slop != null) {
            query.slop(parameter.slop);
        }

        return query;
    }

    public static ToXContent fullParameter(MatchQueryBuilder query, Parameter parameter) {
        if (parameter.analysis != null) {
            query.analyzer(parameter.analysis);
        }

        if (parameter.boost != null) {
            query.boost(parameter.boost);
        }

        if (parameter.operator != null) {
            query.operator(parameter.operator);
        }

        if (parameter.minimumShouldMatch != null) {
            query.minimumShouldMatch(parameter.minimumShouldMatch);
        }

        return query;
    }

    public static ToXContent fullParameter(WildcardQueryBuilder query, Parameter parameter) {
        if (parameter.boost != null) {
            query.boost(parameter.boost);
        }
        return query;
    }

    public static ToXContent fullParameter(QueryStringQueryBuilder query, Parameter parameter) {
        if (parameter.analysis != null) {
            query.analyzer(parameter.analysis);
        }

        if (parameter.boost != null) {
            query.boost(parameter.boost);
        }

        if (parameter.slop != null) {
            query.phraseSlop(parameter.slop);
        }

        if (parameter.defaultField != null) {
            query.defaultField(parameter.defaultField);
        }

        if (parameter.tieBreaker != null) {
            query.tieBreaker(parameter.tieBreaker);
        }

        if (parameter.operator != null) {
            query.defaultOperator(parameter.operator);
        }

        if (parameter.type != null) {
            query.type(MultiMatchQueryBuilder.Type.parse(parameter.type.toLowerCase(Locale.ROOT), LoggingDeprecationHandler.INSTANCE));
        }

        if (parameter.minimumShouldMatch != null) {
            query.minimumShouldMatch(parameter.minimumShouldMatch);
        }

        query.fields(parameter.fieldsBoosts);

        return query;
    }

    public static ToXContent fullParameter(MultiMatchQueryBuilder query, Parameter parameter) {
        if (parameter.analysis != null) {
            query.analyzer(parameter.analysis);
        }

        if (parameter.boost != null) {
            query.boost(parameter.boost);
        }

        if (parameter.slop != null) {
            query.slop(parameter.slop);
        }

        if (parameter.type != null) {
            query.type(parameter.type);
        }

        if (parameter.tieBreaker != null) {
            query.tieBreaker(parameter.tieBreaker);
        }

        if (parameter.operator != null) {
            query.operator(parameter.operator);
        }

        if (parameter.minimumShouldMatch != null) {
            query.minimumShouldMatch(parameter.minimumShouldMatch);
        }

        query.fields(parameter.fieldsBoosts);

        return query;
    }

    public static ToXContent fullParameter(SpanNearQueryBuilder query, Parameter parameter) {
        if (parameter.boost != null) {
            query.boost(parameter.boost);
        }

        if (parameter.inOrder != null) {
            query.inOrder(parameter.inOrder);
        }

        return query;
    }

    public static ToXContent fullParameter(MatchPhrasePrefixQueryBuilder query, Parameter parameter) {
        if (parameter.analysis != null) {
            query.analyzer(parameter.analysis);
        }

        if (parameter.boost != null) {
            query.boost(parameter.boost);
        }

        if (parameter.slop != null) {
            query.slop(parameter.slop);
        }

        return query;
    }
}
