package com.mogudiandian.elasticsearch.orm.core.annotation;

import java.lang.annotation.*;
import java.util.concurrent.TimeUnit;

/**
 * 需要控制生命周期的字段
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface LifecycleField {

    /**
     * 是否用当前时间
     * @return 如果没有业务时间 可以使用当前时间来进行区分
     */
    boolean useCurrent() default false;

    /**
     * 值的时间单位 当属性类型是Long/Integer时才需要设置
     * @return 时间单位 默认毫秒
     */
    TimeUnit timeUnit() default TimeUnit.MILLISECONDS;

    /**
     * 保留的时间单位
     * @return 时间单位 默认天
     */
    TimeUnit durationUnit() default TimeUnit.DAYS;

    /**
     * 保留的时长
     * @return 时长
     */
    long value();

    /**
     * 如果被注解的时间类型是Date 需要指定Date精确到天(yyyy-MM-dd)还是到秒(yyyy-MM-dd HH:mm:ss)
     * @return 时间格式
     */
    // String format() default "yyyy-MM-dd HH:mm:ss";

}