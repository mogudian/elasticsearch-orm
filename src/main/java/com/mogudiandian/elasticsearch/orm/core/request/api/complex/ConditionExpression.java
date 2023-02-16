package com.mogudiandian.elasticsearch.orm.core.request.api.complex;

import com.mogudiandian.elasticsearch.orm.core.util.OrmUtils;
import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.util.Assert;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * 条件表达式
 * @author sunbo
 */
public final class ConditionExpression extends Expression {

    /**
     * 属性名
     */
    private String fieldName;

    /**
     * 操作符
     */
    private Operator operator;

    /**
     * 值
     */
    private Object[] values;

    public ConditionExpression(String fieldName, Operator operator, Object... values) {
        this.fieldName = fieldName;
        this.operator = operator;
        this.values = values;
        Assert.isTrue(values != null, "values must be nonnull");
        operator.check(values);
    }

    @Override
    public QueryBuilder toQueryBuilder() {
        QueryBuilder queryBuilder;

        boolean isSearchField = OrmUtils.isSearchField(entityClass, this.fieldName);

        Class<? extends BaseEntity> nestedFieldType = OrmUtils.getNestedFieldType(entityClass, this.fieldName);

        // 根据实体属性的字段名进行replace
        String fieldName = OrmUtils.getEntityFieldName(entityClass, this.fieldName);

        switch (operator) {
            case TERM:
            case EQUAL:
                queryBuilder = QueryBuilders.termQuery(fieldName, values[0]);
                break;
            case TERMS:
            case IN:
                if (values.length == 1 && values[0] instanceof Collection) {
                    queryBuilder = QueryBuilders.termsQuery(fieldName, (Collection<?>) values[0]);
                } else {
                    queryBuilder = QueryBuilders.termsQuery(fieldName, values);
                }
                break;
            case WILDCARD:
            case LIKE:
                if (isSearchField) {
                    queryBuilder = QueryBuilders.matchPhraseQuery(fieldName, values[0]);
                } else {
                    queryBuilder = QueryBuilders.wildcardQuery(fieldName, "*" + values[0] + "*");
                }
                break;
            case RANGE:
            case BETWEEN:
                if (values.length == 2) {
                    queryBuilder = QueryBuilders.rangeQuery(fieldName).from(values[0]).includeLower(true).to(values[1]).includeUpper(true);
                } else {
                    queryBuilder = QueryBuilders.rangeQuery(fieldName).from(values[0]).includeLower((Boolean) values[1]).to(values[2]).includeUpper((Boolean) values[3]);
                }
                // TODO 也可以是3和5(日期做format查询)
                break;
            case GT:
                queryBuilder = QueryBuilders.rangeQuery(fieldName).from(values[0]).includeLower(false);
                break;
            case GTE:
                queryBuilder = QueryBuilders.rangeQuery(fieldName).from(values[0]).includeLower(true);
                break;
            case LT:
                queryBuilder = QueryBuilders.rangeQuery(fieldName).to(values[0]).includeLower(false);
                break;
            case LTE:
                queryBuilder = QueryBuilders.rangeQuery(fieldName).to(values[0]).includeLower(true);
                break;
            case EXISTS:
                queryBuilder = QueryBuilders.existsQuery(fieldName);
                break;
            case NOT_EXISTS:
                queryBuilder = QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(fieldName));
                break;
            case NESTED:
                if (nestedFieldType == null || !BaseEntity.class.isAssignableFrom(nestedFieldType)) {
                    throw new RuntimeException("Nested field type must be assigned from BaseEntity");
                }

                BoolQueryBuilder nestedQuery = QueryBuilders.boolQuery();

                Object object = values[0];
                Stream<Object> stream;
                if (object.getClass().isArray()) {
                    stream = IntStream.range(0, Array.getLength(object))
                                      .mapToObj(x -> Array.get(object, x));
                } else if (object instanceof List) {
                    stream = ((List<Object>) object).stream();
                } else {
                    stream = Arrays.stream(values);
                }
                stream.filter(x -> x instanceof Expression)
                      .map(x -> (Expression) x)
                      .peek(x -> {
                          x.setEntityClass(nestedFieldType);
                          if (x instanceof ConditionExpression) {
                              ((ConditionExpression) x).fieldName = fieldName + "." + ((ConditionExpression) x).fieldName;
                          }
                      })
                      .map(Expression::toQueryBuilder)
                      .forEach(nestedQuery::must);
                queryBuilder = QueryBuilders.nestedQuery(fieldName, nestedQuery, ScoreMode.None);
                break;
            default:
                throw new RuntimeException("Not Implemented");
        }
        return queryBuilder;
    }
}