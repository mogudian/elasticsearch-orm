package com.mogudiandian.elasticsearch.orm.core;

import com.mogudiandian.elasticsearch.orm.core.util.OrmUtils;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * Entity映射查找器
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
@Slf4j
public final class EntityMappingFinder {

    /**
     * 缓存
     */
    private static final Map<Class<? extends BaseEntity>, EntityMapping<? extends BaseEntity>> cache = new HashMap<>();

    /**
     * 获取实体类型对应的mapping 走缓存
     * @param entityClass 实体类型
     * @param <E> 实体类型
     * @return mapping
     */
    public static <E extends BaseEntity> EntityMapping<E> find(Class<E> entityClass) {
        EntityMapping<E> mapping = (EntityMapping<E>) cache.get(entityClass);
        if (mapping == null) {
            mapping = find0(entityClass);
            cache.put(entityClass, mapping);
        }
        return mapping;
    }

    /**
     * 反射获取实体类型对应的mapping
     * @param entityClass 实体类型
     * @param <E> 实体类型
     * @return mapping
     */
    private static <E extends BaseEntity> EntityMapping<E> find0(Class<E> entityClass) {
        Class<EntityMapping<E>> entityMapping = OrmUtils.getEntityMapping(entityClass);
        if (entityMapping != null) {
            try {
                return entityMapping.getConstructor(Class.class).newInstance(entityClass);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                log.error("Instantiate mapping class {} error, caused ", entityMapping, e);
            }
        }
        return new DefaultEntityMapping<>(entityClass);
    }

}
