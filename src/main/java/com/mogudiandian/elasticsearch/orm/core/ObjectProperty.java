package com.mogudiandian.elasticsearch.orm.core;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * 对象的属性
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
public class ObjectProperty {

    /**
     * 字段
     */
    protected Field field;

    /**
     * getter
     */
    protected Method getter;

    /**
     * setter
     */
    protected Method setter;

    public ObjectProperty(Field field) {
        this.field = field;
        try {
            PropertyDescriptor propertyDescriptor = new PropertyDescriptor(field.getName(), field.getDeclaringClass());
            this.getter = propertyDescriptor.getReadMethod();
            this.setter = propertyDescriptor.getWriteMethod();
        } catch (IntrospectionException e) {
            throw new RuntimeException("field " + field + " has no getter or setter", e);
        }
    }

    /**
     * 获取属性所在的类
     * @return 属性所在的类
     */
    public Class<?> getDeclaringClass() {
        return field.getDeclaringClass();
    }

    /**
     * 获取属性名
     * @return 属性名
     */
    public String getName() {
        return field.getName();
    }

    /**
     * 获取属性类型
     * @return 属性类型
     */
    public Class<?> getType() {
        return field.getType();
    }

    /**
     * 调用属性的getter
     * @param obj 实例
     * @return 属性值
     */
    public Object invokeGetter(Object obj) {
        try {
            return getter.invoke(obj);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 调用属性的setter
     * @param obj 实例
     * @param value 要设置的值
     */
    public void invokeSetter(Object obj, Object value) {
        try {
            setter.invoke(obj, value);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

}
