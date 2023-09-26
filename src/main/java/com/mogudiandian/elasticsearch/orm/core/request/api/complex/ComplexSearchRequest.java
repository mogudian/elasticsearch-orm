package com.mogudiandian.elasticsearch.orm.core.request.api.complex;

import com.mogudiandian.elasticsearch.orm.core.request.api.ApiSearchRequest;
import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.LinkedList;
import java.util.List;

/**
 * 复杂搜索条件
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
public class ComplexSearchRequest extends ApiSearchRequest {

    /**
     * 表达式
     */
    private List<Expression> expressions;

    public ComplexSearchRequest(Class<? extends BaseEntity> entityClass) {
        super(entityClass);
    }

    public ComplexSearchRequest appendExpression(Expression expression) {
        if (expressions == null) {
            expressions = new LinkedList<>();
        }
        expression.setEntityClass(entityClass);
        expressions.add(expression);
        return this;
    }

    public ComplexSearchRequest removeExpression(Expression expression) {
        if (expressions != null) {
            expressions.remove(expression);
        }
        return this;
    }

    public ComplexSearchRequest clearExpressions() {
        if (expressions != null) {
            expressions.clear();
        }
        return this;
    }

    @Override
    public QueryBuilder toQueryBuilder() {
        return new LogicExpression(Logic.MUST, expressions).toQueryBuilder();
    }

}