package com.mogudiandian.elasticsearch.orm.core.request.api.annotated.es;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * ES的term
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Term {

    /**
     * 属性名 不指定则使用当前的属性名
     */
    String name() default "";
}
