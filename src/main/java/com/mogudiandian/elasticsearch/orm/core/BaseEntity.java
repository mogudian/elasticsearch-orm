package com.mogudiandian.elasticsearch.orm.core;

/**
 * ES基础实体
 * @author sunbo
 */
public interface BaseEntity {

    String ID = "id", CLAZZ = "class";

    /**
     * 实体ID
     */
    String entityId();

}
