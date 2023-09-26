package com.mogudiandian.elasticsearch.orm.core;

import java.util.List;
import java.util.Map;

/**
 * ES实体映射
 * @param <E> 实体类型
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
public abstract class EntityMapping<E extends BaseEntity> {

    protected Class<E> entityClass;

    protected EntityMapping(Class<E> entityClass) {
        this.entityClass = entityClass;
    }

    /**
     * 根据实体类型返回ES中模糊搜索的字段
     * @return 字段列表
     */
    public abstract List<String> searchFields();

    /**
     * 根据实体类型返回ES中生命周期的字段
     * @return 字段列表
     */
    public abstract String lifecycleField();

    /**
     * 从实体到ES文档的映射
     * @param entity ES的实体
     * @return ES文档
     */
    public abstract Map<String, Object> convert(E entity);

    /**
     * 从ES文档到实体的映射
     * @param map ES文档
     * @return ES的实体
     */
    public abstract E convert(Map<String, Object> map);
}
