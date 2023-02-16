package com.mogudiandian.elasticsearch.orm.core.annotation;

import java.lang.annotation.*;

/**
 * 需要嵌套的字段
 * @author sunbo
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface NestedField {
}
