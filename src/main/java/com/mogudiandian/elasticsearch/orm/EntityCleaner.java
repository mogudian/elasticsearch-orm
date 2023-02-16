package com.mogudiandian.elasticsearch.orm;

import com.mogudiandian.elasticsearch.orm.core.BaseEntity;

/**
 * 实体数据清理器
 * @author sunbo
 */
public interface EntityCleaner {

    /**
     * 根据实体类进行清理
     * @param entityClass 实体类
     */
    void clean(Class<? extends BaseEntity> entityClass);

}
