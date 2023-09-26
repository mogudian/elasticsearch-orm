package com.mogudiandian.elasticsearch.orm.core.util;

import com.mogudiandian.elasticsearch.orm.core.EntityProperty;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;


/**
 * 生命周期的工具类
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
public final class LifecycleUtils {

    private LifecycleUtils() {
    }

    /**
     * 获取属性的值 用于插入ES前将实体转换为文档
     * @param obj 实例
     * @param property 属性
     * @return 属性的值 类型为long表示的毫秒数
     */
    public static Object getPropertyValue(Object obj, EntityProperty property) {
        if (obj == null || property == null || !property.isForLifecycle()) {
            throw new IllegalStateException();
        }

        Object o = property.invokeGetter(obj);
        if (o == null) {
            if (property.getLifecycleField().useCurrent()) {
                return System.currentTimeMillis();
            }
            return null;
        }

        Class<?> type = property.getType();
        if (type == Long.class) {
            return property.getLifecycleField().timeUnit().toMillis((Long) o);
        } else if (type == Integer.class) {
            return property.getLifecycleField().timeUnit().toMillis(((Integer) o).longValue());
        } else {
            // String format = property.getLifecycleField().format();
            if (Date.class.isAssignableFrom(type)) {
                Date date = (Date) o;
                return date.getTime();
            } else if (Calendar.class.isAssignableFrom(type)) {
                Calendar calendar = (Calendar) o;
                return calendar.getTimeInMillis();
            } else if (LocalDateTime.class.isAssignableFrom(type)) {
                LocalDateTime dateTime = (LocalDateTime) o;
                return dateTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
            } else if (LocalDate.class.isAssignableFrom(type)) {
                LocalDate date = (LocalDate) o;
                return date.atStartOfDay().toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
            }
        }

        throw new RuntimeException("Not support lifecycle field for type " + type);
    }

    /**
     * 设置属性值 用于从ES读取后将文档转换为实体
     * @param obj 实例
     * @param property 属性
     * @param value 要设置的值(毫秒数)
     */
    public static void setPropertyValue(Object obj, EntityProperty property, Long value) {
        if (obj == null ) {
            throw new IllegalStateException();
        }

        property.invokeSetter(obj, revertPropertyValue(property, value));
    }

    /**
     * 还原属性值 用于从ES读取后将文档转换为实体
     * @param property 属性
     * @param value 要还原的值(毫秒数)
     */
    public static Object revertPropertyValue(EntityProperty property, Long value) {
        if (property == null || !property.isForLifecycle()) {
            throw new IllegalStateException();
        }

        if (value == null) {
            return null;
        }

        Class<?> type = property.getType();
        if (type == Long.class) {
            return TimeUnit.MILLISECONDS.convert(value, property.getLifecycleField().timeUnit());
        } else if (type == Integer.class) {
            return (int) TimeUnit.MILLISECONDS.convert(value, property.getLifecycleField().timeUnit());
        } else {
            if (Date.class.isAssignableFrom(type)) {
                return new Date(value);
            } else if (Calendar.class.isAssignableFrom(type)) {
                Calendar calendar = Calendar.getInstance();
                calendar.setTimeInMillis(value);
                return calendar;
            } else if (LocalDateTime.class.isAssignableFrom(type)) {
                return Instant.ofEpochMilli(value).atZone(ZoneOffset.ofHours(8)).toLocalDateTime();
            } else if (LocalDate.class.isAssignableFrom(type)) {
                return Instant.ofEpochMilli(value).atZone(ZoneOffset.ofHours(8)).toLocalDate();
            } else {
                throw new RuntimeException("Not support lifecycle field for type " + type);
            }
        }
    }

}
