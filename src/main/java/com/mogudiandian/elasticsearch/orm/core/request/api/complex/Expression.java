package com.mogudiandian.elasticsearch.orm.core.request.api.complex;

import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import lombok.Setter;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * 表达式
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
public abstract class Expression {

    /**
     * 实体类
     */
    @Setter
    protected Class<? extends BaseEntity> entityClass;

    /**
     * 将表达式转换为query
     *
     * @return query
     */
    public abstract QueryBuilder toQueryBuilder();

}