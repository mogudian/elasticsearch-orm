package com.mogudiandian.elasticsearch.orm.core.annotation;

import java.lang.annotation.*;

/**
 * 用来指定实体对应的索引名称
 * @author sunbo
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Index {
    String value();
}