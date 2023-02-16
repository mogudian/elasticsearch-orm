package com.mogudiandian.elasticsearch.orm.core.request.api.annotated.es;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * ES的wildcard
 * @author sunbo
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Wildcard {
    /**
     * 属性名 不指定则使用当前的属性名
     */
    String name() default "";

    /**
     * 是否左侧通配
     */
    boolean leftWildcard() default true;

    /**
     * 是否右侧通配
     */
    boolean rightWildcard() default true;
}
