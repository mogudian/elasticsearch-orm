package com.mogudiandian.elasticsearch.orm.core.request.api.annotated.es;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * ES的range
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Range {

    /**
     * 属性名 不指定则使用当前的属性名
     */
    String name() default "";

    /**
     * >
     */
    boolean lt() default false;

    /**
     * <
     */
    boolean gt() default false;

    /**
     * >=
     */
    boolean includeLower() default true;

    /**
     * <=
     */
    boolean includeUpper() default true;

}
