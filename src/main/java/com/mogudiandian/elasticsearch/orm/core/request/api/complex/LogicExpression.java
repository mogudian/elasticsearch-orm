package com.mogudiandian.elasticsearch.orm.core.request.api.complex;

import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import lombok.Getter;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 逻辑表达式
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
@Getter
public final class LogicExpression extends Expression {

    /**
     * 逻辑符
     */
    private Logic logic;

    /**
     * 逻辑下的所有表达式 用这个便可以嵌套
     */
    private List<Expression> expressions;

    public LogicExpression(Logic logic, List<Expression> expressions) {
        this.logic = logic;
        this.expressions = expressions;
    }

    public LogicExpression(Logic logic, Expression... expressions) {
        this(logic, Arrays.stream(expressions).collect(Collectors.toList()));
    }

    @Override
    public void setEntityClass(Class<? extends BaseEntity> entityClass) {
        super.setEntityClass(entityClass);
        if (expressions != null) {
            expressions.forEach(x -> x.setEntityClass(entityClass));
        }
    }

    @Override
    public QueryBuilder toQueryBuilder() {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if (expressions != null && !expressions.isEmpty()) {
            // TODO 根据NESTED分组
            switch (logic) {
                case MUST:
                case AND:
                    expressions.stream()
                               .map(Expression::toQueryBuilder)
                               .forEach(boolQueryBuilder::must);
                    break;
                case SHOULD:
                case OR:
                    expressions.stream()
                               .map(Expression::toQueryBuilder)
                               .forEach(boolQueryBuilder::should);
                    break;
                case MUST_NOT:
                case NOT:
                    expressions.stream()
                               .map(Expression::toQueryBuilder)
                               .forEach(boolQueryBuilder::mustNot);
                    break;
                default:
                    throw new RuntimeException("Not Implemented");
            }
        }
        return boolQueryBuilder;
    }

}