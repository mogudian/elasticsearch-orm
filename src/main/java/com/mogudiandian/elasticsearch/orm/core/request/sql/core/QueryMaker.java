package com.mogudiandian.elasticsearch.orm.core.request.sql.core;

import com.alibaba.druid.sql.ast.expr.*;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.Condition;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.Parameter;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser.CaseWhenParser;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser.ScriptFilter;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser.SqlParseException;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser.SubQueryExpression;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.spatial.*;
import com.mogudiandian.elasticsearch.orm.core.util.OrmUtils;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.ESOperator;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.Where;
import com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.Where.CONN;
import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import com.google.common.collect.ImmutableSet;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;
import org.elasticsearch.join.query.JoinQueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchModule;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

public class QueryMaker {

    private static final Set<ESOperator> NOT_ES_OPERATOR_SET = ImmutableSet.of(ESOperator.N, ESOperator.NIN, ESOperator.ISN, ESOperator.NBETWEEN, ESOperator.NLIKE, ESOperator.NIN_TERMS, ESOperator.NTERM, ESOperator.NREGEXP);

    /**
     * 实体类
     */
    private Class<? extends BaseEntity> entityClass;

    public QueryMaker(Class<? extends BaseEntity> entityClass) {
        this.entityClass = entityClass;
    }

    /**
     * 构建过滤条件
     */
    public ToXContent make(Condition condition) {
        String fieldName = condition.getName();
        boolean isSearchField = OrmUtils.isSearchField(entityClass, fieldName);

        // 根据实体属性的字段名进行replace
        fieldName = OrmUtils.getEntityFieldName(entityClass, fieldName);
        Object value = condition.getValue();

        ToXContent x;

        if (value instanceof SQLMethodInvokeExpr) {
            x = make(condition, fieldName, (SQLMethodInvokeExpr) value, isSearchField);
        } else if (value instanceof SubQueryExpression) {
            x = make(condition, fieldName, ((SubQueryExpression) value).getValues(), isSearchField);
        } else {
            x = make(condition, fieldName, value, isSearchField);
        }

        return x;
    }

    private ToXContent make(Condition condition, String name, SQLMethodInvokeExpr value, boolean isSearchField) {
        ToXContent bqb;
        Parameter parameter;
        switch (value.getMethodName().toLowerCase()) {
            case "query":
                parameter = Parameter.parseParameter(value);
                QueryStringQueryBuilder queryString = QueryBuilders.queryStringQuery(parameter.value);
                bqb = Parameter.fullParameter(queryString, parameter);
                bqb = fixNot(condition, bqb);
                break;
            case "matchquery":
            case "match_query":
                parameter = Parameter.parseParameter(value);
                MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(name, parameter.value);
                bqb = Parameter.fullParameter(matchQuery, parameter);
                bqb = fixNot(condition, bqb);
                break;
            case "score":
            case "scorequery":
            case "score_query":
                float boost = Float.parseFloat(value.getParameters().get(1).toString());
                Condition subCondition = new Condition(condition.getConn(), name, null, condition.getEsOperator(), value.getParameters().get(0), null);
                bqb = QueryBuilders.constantScoreQuery((QueryBuilder) make(subCondition)).boost(boost);
                break;
            case "wildcardquery":
            case "wildcard_query":
                parameter = Parameter.parseParameter(value);
                WildcardQueryBuilder wildcardQuery = QueryBuilders.wildcardQuery(name, parameter.value);
                bqb = Parameter.fullParameter(wildcardQuery, parameter);
                break;

            case "matchphrasequery":
            case "match_phrase":
            case "matchphrase":
                parameter = Parameter.parseParameter(value);
                MatchPhraseQueryBuilder matchPhraseQuery = QueryBuilders.matchPhraseQuery(name, parameter.value);
                bqb = Parameter.fullParameter(matchPhraseQuery, parameter);
                break;

            case "multimatchquery":
            case "multi_match":
            case "multimatch":
                parameter = Parameter.parseParameter(value);
                MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery(parameter.value);
                bqb = Parameter.fullParameter(multiMatchQuery, parameter);
                break;

            case "spannearquery":
            case "span_near":
            case "spannear":
                parameter = Parameter.parseParameter(value);

                // parse clauses
                List<SpanQueryBuilder> clauses = new ArrayList<>();
                try (XContentParser parser = JsonXContent.jsonXContent.createParser(new NamedXContentRegistry(new SearchModule(Settings.EMPTY, true, Collections.emptyList()).getNamedXContents()), LoggingDeprecationHandler.INSTANCE, parameter.clauses)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        QueryBuilder query = SpanNearQueryBuilder.parseInnerQueryBuilder(parser);
                        if (!(query instanceof SpanQueryBuilder)) {
                            throw new ParsingException(parser.getTokenLocation(), "spanNear [clauses] must be of type span query");
                        }
                        clauses.add((SpanQueryBuilder) query);
                    }
                } catch (IOException e) {
                    throw new SqlParseException("could not parse clauses: " + e.getMessage());
                }

                SpanNearQueryBuilder spanNearQuery = QueryBuilders.spanNearQuery(clauses.get(0), Optional.ofNullable(parameter.slop).orElse(SpanNearQueryBuilder.DEFAULT_SLOP));
                for (int i = 1; i < clauses.size(); ++i) {
                    spanNearQuery.addClause(clauses.get(i));
                }

                bqb = Parameter.fullParameter(spanNearQuery, parameter);
                break;

            case "matchphraseprefix":
            case "matchphraseprefixquery":
            case "match_phrase_prefix":
                parameter = Parameter.parseParameter(value);
                MatchPhrasePrefixQueryBuilder phrasePrefixQuery = QueryBuilders.matchPhrasePrefixQuery(name, parameter.value);
                bqb = Parameter.fullParameter(phrasePrefixQuery, parameter);
                break;

            default:
                throw new SqlParseException("it did not support this query method " + value.getMethodName());
        }

        return bqb;
    }

    private ToXContent make(Condition condition, String name, Object value, boolean isSearchField) {
        ToXContent x;
        switch (condition.getEsOperator()) {
            case ISN:
            case IS:
            case N:
            case EQ:
                if (value == null || value instanceof SQLIdentifierExpr) {
                    if (value == null || ((SQLIdentifierExpr) value).getName().equalsIgnoreCase("missing")) {
                        x = QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(name));
                    } else {
                        throw new SqlParseException(String.format("Cannot recoginze Sql identifer %s", ((SQLIdentifierExpr) value).getName()));
                    }
                } else {
                    // TODO maybe use term filter when not analayzed field avalaible to make exact matching?
                    // using matchPhrase to achieve equallity.
                    // matchPhrase still have some disatvantegs, f.e search for 'word' will match 'some word'
                    x = QueryBuilders.matchPhraseQuery(name, value);
                }
                break;
            case LIKE:
            case NLIKE:
                // TODO isSearchField
                if (isSearchField) {
                    x = QueryBuilders.matchPhraseQuery(name, value);
                } else {
                    String queryStr = ((String) value);
                    queryStr = queryStr.replace('%', '*').replace('_', '?');
                    queryStr = queryStr.replace("&PERCENT", "%").replace("&UNDERSCORE", "_");
                    x = QueryBuilders.wildcardQuery(name, queryStr);
                }
                break;
            case REGEXP:
            case NREGEXP:
                Object[] values = (Object[]) value;
                RegexpQueryBuilder regexpQuery = QueryBuilders.regexpQuery(name, values[0].toString());
                if (1 < values.length) {
                    String[] flags = values[1].toString().split("\\|");
                    RegexpFlag[] regexpFlags = new RegexpFlag[flags.length];
                    for (int i = 0; i < flags.length; ++i) {
                        regexpFlags[i] = RegexpFlag.valueOf(flags[i]);
                    }
                    regexpQuery.flags(regexpFlags);
                }
                if (2 < values.length) {
                    regexpQuery.maxDeterminizedStates(Integer.parseInt(values[2].toString()));
                }
                x = regexpQuery;
                break;
            case GT:
                x = QueryBuilders.rangeQuery(name).gt(value);
                break;
            case GTE:
                x = QueryBuilders.rangeQuery(name).gte(value);
                break;
            case LT:
                x = QueryBuilders.rangeQuery(name).lt(value);
                break;
            case LTE:
                x = QueryBuilders.rangeQuery(name).lte(value);
                break;
            case NIN:
            case IN:
                if (condition.getNameExpr() instanceof SQLCaseExpr) {
                    /*
                     * 调用CaseWhenParser解析将Condition的nameExpr属性对象解析为script query
                     * 参考了SqlParser.findSelect()方法，看它是如何解析select中的case when字段的
                     */
                    String scriptCode = new CaseWhenParser((SQLCaseExpr) condition.getNameExpr()).parseCaseWhenInWhere((Object[]) value);
                    /*
                     * 参考QueryAction.handleScriptField() 将上文得到的scriptCode封装为es的Script对象，
                     * 但又不是完全相同，因为DefaultQueryAction.handleScriptField()是处理select子句中的case when查询，对应es的script_field查询，
                     * 而此处是处理where子句中的case when查询，对应的是es的script query，具体要看官网文档，搜索关键字是"script query"
                     * 搜索结果如下：
                     * 1、文档 https://www.elastic.co/guide/en/elasticsearch/reference/6.1/query-dsl-script-query.html
                     * 2、java api https://www.elastic.co/guide/en/elasticsearch/client/java-api/6.1/java-specialized-queries.html
                     */
                    x = QueryBuilders.scriptQuery(new Script(scriptCode));

                } else {
                    values = (Object[]) value;
                    MatchPhraseQueryBuilder[] matchQueries = new MatchPhraseQueryBuilder[values.length];
                    for (int i = 0; i < values.length; i++) {
                        matchQueries[i] = QueryBuilders.matchPhraseQuery(name, values[i]);
                    }

                    BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
                    for (MatchPhraseQueryBuilder matchQuery : matchQueries) {
                        boolQuery.should(matchQuery);
                    }
                    x = boolQuery;
                }

                break;
            case BETWEEN:
            case NBETWEEN:
                x = QueryBuilders.rangeQuery(name).gte(((Object[]) value)[0]).lte(((Object[]) value)[1]);
                break;
            case GEO_INTERSECTS:
                String wkt = condition.getValue().toString();
                try {
                    ShapeBuilder shapeBuilder = getShapeBuilderFromString(wkt);
                    x = QueryBuilders.geoShapeQuery(name, shapeBuilder);
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new SqlParseException("couldn't create shapeBuilder from wkt: " + wkt);
                }
                break;
            case GEO_BOUNDING_BOX:
                BoundingBoxFilterParams boxFilterParams = (BoundingBoxFilterParams) condition.getValue();
                Point topLeft = boxFilterParams.getTopLeft();
                Point bottomRight = boxFilterParams.getBottomRight();
                x = QueryBuilders.geoBoundingBoxQuery(name).setCorners(topLeft.getLat(), topLeft.getLon(), bottomRight.getLat(), bottomRight.getLon());
                break;
            case GEO_DISTANCE:
                DistanceFilterParams distanceFilterParams = (DistanceFilterParams) condition.getValue();
                Point fromPoint = distanceFilterParams.getFrom();
                String distance = trimApostrophes(distanceFilterParams.getDistance());
                x = QueryBuilders.geoDistanceQuery(name).distance(distance).point(fromPoint.getLat(), fromPoint.getLon());
                break;
            case GEO_POLYGON:
                PolygonFilterParams polygonFilterParams = (PolygonFilterParams) condition.getValue();
                ArrayList<GeoPoint> geoPoints = new ArrayList<>();
                for (Point p : polygonFilterParams.getPolygon()) {
                    geoPoints.add(new GeoPoint(p.getLat(), p.getLon()));
                }
                x = QueryBuilders.geoPolygonQuery(name, geoPoints);
                break;
            case NIN_TERMS:
            case IN_TERMS:
                Object[] termValues = (Object[]) value;
                if (termValues.length == 1 && termValues[0] instanceof SubQueryExpression) {
                    termValues = ((SubQueryExpression) termValues[0]).getValues();
                }
                Object[] termValuesObjects = new Object[termValues.length];
                for (int i = 0; i < termValues.length; i++) {
                    termValuesObjects[i] = parseTermValue(termValues[i]);
                }
                x = QueryBuilders.termsQuery(name, termValuesObjects);
                break;
            case NTERM:
            case TERM:
                Object term = ((Object[]) value)[0];
                x = QueryBuilders.termQuery(name, parseTermValue(term));
                break;
            case IDS_QUERY:
                Object[] idsParameters = (Object[]) value;
                String[] ids;
                if (idsParameters.length == 2 && idsParameters[1] instanceof SubQueryExpression) {
                    Object[] idsFromSubQuery = ((SubQueryExpression) idsParameters[1]).getValues();
                    ids = arrayOfObjectsToStringArray(idsFromSubQuery, 0, idsFromSubQuery.length - 1);
                } else {
                    ids = arrayOfObjectsToStringArray(idsParameters, 1, idsParameters.length - 1);
                }
                x = QueryBuilders.idsQuery().addIds(ids);
                break;
            case NNESTED_COMPLEX:
            case NESTED_COMPLEX:
                if (!(value instanceof Where)) {
                    throw new SqlParseException("unsupported nested condition");
                }

                Where whereNested = (Where) value;
                BoolQueryBuilder nestedFilter = explain(whereNested);

                x = QueryBuilders.nestedQuery(name, nestedFilter, ScoreMode.None);
                break;
            case CHILDREN_COMPLEX:
                if (!(value instanceof Where)) {
                    throw new SqlParseException("unsupported nested condition");
                }

                Where whereChildren = (Where) value;
                BoolQueryBuilder childrenFilter = explain(whereChildren);
                // TODO pass score mode
                x = JoinQueryBuilders.hasChildQuery(name, childrenFilter, ScoreMode.None);
                break;
            case SCRIPT:
                ScriptFilter scriptFilter = (ScriptFilter) value;
                Map<String, Object> params = new HashMap<>();
                if (scriptFilter.containsParameters()) {
                    params = scriptFilter.getArgs();
                }
                x = QueryBuilders.scriptQuery(new Script(scriptFilter.getScriptType(), Script.DEFAULT_SCRIPT_LANG, scriptFilter.getScript(), params));
                break;
            default:
                throw new SqlParseException("not define type " + name);
        }

        return fixNot(condition, x);
    }

    private String[] arrayOfObjectsToStringArray(Object[] values, int from, int to) {
        String[] strings = new String[to - from + 1];
        int counter = 0;
        for (int i = from; i <= to; i++) {
            strings[counter] = values[i].toString();
            counter++;
        }
        return strings;
    }

    private ShapeBuilder getShapeBuilderFromString(String str) {
        String json;
        if (str.contains("{")) {
            json = fixJsonFromElastic(str);
        } else {
            json = WktToGeoJsonConverter.toGeoJson(trimApostrophes(str));
        }

        return getShapeBuilderFromJson(json);
    }

    /*
     * elastic sends {coordinates=[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]], type=Polygon}
     * proper form is {"coordinates":[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]], "type":"Polygon"}
     *  */
    private String fixJsonFromElastic(String elasticJson) {
        String properJson = elasticJson.replaceAll("=", ":");
        properJson = properJson.replaceAll("(type)(:)([a-zA-Z]+)", "\"type\":\"$3\"");
        properJson = properJson.replaceAll("coordinates", "\"coordinates\"");
        return properJson;
    }

    private ShapeBuilder getShapeBuilderFromJson(String json) {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, json)) {
            parser.nextToken();
            return ShapeParser.parse(parser);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String trimApostrophes(String str) {
        return str.substring(1, str.length() - 1);
    }

    private ToXContent fixNot(Condition condition, ToXContent bqb) {
        if (NOT_ES_OPERATOR_SET.contains(condition.getEsOperator())) {
            bqb = QueryBuilders.boolQuery().mustNot((QueryBuilder) bqb);
        }
        return bqb;
    }

    private Object parseTermValue(Object termValue) {
        if (termValue instanceof SQLNumericLiteralExpr) {
            termValue = ((SQLNumericLiteralExpr) termValue).getNumber();
            if (termValue instanceof BigDecimal || termValue instanceof Double) {
                termValue = ((Number) termValue).doubleValue();
            } else if (termValue instanceof Float) {
                termValue = ((Number) termValue).floatValue();
            } else if (termValue instanceof BigInteger || termValue instanceof Long) {
                termValue = ((Number) termValue).longValue();
            } else if (termValue instanceof Integer) {
                termValue = ((Number) termValue).intValue();
            } else if (termValue instanceof Short) {
                termValue = ((Number) termValue).shortValue();
            } else if (termValue instanceof Byte) {
                termValue = ((Number) termValue).byteValue();
            }
        } else if (termValue instanceof SQLBooleanExpr) {
            termValue = ((SQLBooleanExpr) termValue).getValue();
        } else {
            termValue = termValue.toString();
        }

        return termValue;
    }

    /**
     * 将where条件构建成query
     */
    public BoolQueryBuilder explain(Where where) {
        return explain(where, true);
    }

    public BoolQueryBuilder explain(Where where, boolean isFilter) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        // 一直取，取到最深的那个where
        // 暂时只遇到了该sql：select a,b,c as my_c from tbl where a = 1，会走这个分支
        // 就是where子句中只有一个条件的情况下会走该分支
        // 还有层层嵌套的情况，一直get到底
        while (where.getWheres().size() == 1) {
            where = where.getWheres().getFirst();
        }

        // where.getWheres().size()的长度等于0 或者 大于1
        explainWhere(boolQuery, where);

        // 使用filter，不需要计算_score
        if (isFilter) {
            return QueryBuilders.boolQuery().filter(boolQuery);
        }
        return boolQuery;
    }

    private void explainWhere(BoolQueryBuilder boolQuery, Where where) {
        if (where instanceof Condition) {
            // 重点方法 就是这里解析最细粒度的where条件
            addSubQuery(boolQuery, where, (QueryBuilder) make((Condition) where));
        } else {
            /*
             * select a,b,c as my_c from tbl where a = 1 or b = 2 and (c = 3 or d = 4) or e > 1
             * 上面这条sql中的“b = 2 and (c = 3 or d = 4)”这部分会走该分支，
             * 因为“b = 2 and (c = 3 or d = 4)”被封装为Where类型的对象，而不是Condition对象
             */
            BoolQueryBuilder subQuery = QueryBuilders.boolQuery();

            // 将subQuery对象纳入到boolQuery中，即boolQuery是上一级，subQuery是下一级
            addSubQuery(boolQuery, where, subQuery);
            for (Where subWhere : where.getWheres()) {
                // 然后又将subWhere对象纳入到subQuery对象中，通过递归就能层层解析出这个Where条件了：“b = 2 and (c = 3 or d = 4)”
                explainWhere(subQuery, subWhere);
            }
        }
    }

    /**
     * 增加嵌套子条件
     */
    private void addSubQuery(BoolQueryBuilder boolQuery, Where where, QueryBuilder subQuery) {
        if (where instanceof Condition) {
            Condition condition = (Condition) where;

            if (condition.isNested()) {
                boolean isNestedQuery = subQuery instanceof NestedQueryBuilder;
                InnerHitBuilder ihb = null;
                if (condition.getInnerHits() != null) {
                    try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, condition.getInnerHits())) {
                        ihb = InnerHitBuilder.fromXContent(parser);
                    } catch (IOException e) {
                        throw new IllegalArgumentException("couldn't parse inner_hits: " + e.getMessage(), e);
                    }
                }

                if ("missing".equalsIgnoreCase(String.valueOf(condition.getValue())) && (condition.getEsOperator() == ESOperator.IS || condition.getEsOperator() == ESOperator.EQ)) {
                    NestedQueryBuilder q = isNestedQuery ? (NestedQueryBuilder) subQuery : QueryBuilders.nestedQuery(condition.getNestedPath(), QueryBuilders.boolQuery().mustNot(subQuery), ScoreMode.None);
                    if (ihb != null) {
                        q.innerHit(ihb);
                    }
                    boolQuery.mustNot(q);
                    return;
                }

                // support not nested
                if (condition.getEsOperator() == ESOperator.NNESTED_COMPLEX) {
                    if (ihb != null) {
                        ((NestedQueryBuilder) subQuery).innerHit(ihb);
                    }
                    boolQuery.mustNot(subQuery);
                    return;
                }

                if (!isNestedQuery) {
                    subQuery = QueryBuilders.nestedQuery(condition.getNestedPath(), subQuery, ScoreMode.None);
                }
                if (ihb != null) {
                    ((NestedQueryBuilder) subQuery).innerHit(ihb);
                }
            } else if (condition.isChildren()) {
                subQuery = JoinQueryBuilders.hasChildQuery(condition.getChildType(), subQuery, ScoreMode.None);
            }
        }

        // 将subQuery对象纳入到boolQuery中，即boolQuery是上一级，subQuery是下一级
        if (where.getConn() == CONN.AND) {
            boolQuery.must(subQuery);
        } else {
            boolQuery.should(subQuery);
        }
    }
}
