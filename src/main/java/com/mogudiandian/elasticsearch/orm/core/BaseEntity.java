package com.mogudiandian.elasticsearch.orm.core;

/**
 * ES基础实体
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
public interface BaseEntity {

    String ID = "id", CLAZZ = "class";

    /**
     * 实体ID
     */
    String entityId();

}
