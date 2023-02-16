package com.mogudiandian.elasticsearch.orm.core.request.api.annotated;

import com.mogudiandian.elasticsearch.orm.core.ObjectProperty;
import com.mogudiandian.elasticsearch.orm.core.request.api.annotated.es.*;
import lombok.Getter;

import java.lang.reflect.Field;

/**
 * 查询模板的属性
 * @author sunbo
 */
@Getter
public class TemplateProperty extends ObjectProperty {

    /**
     * 属性的Range注解
     */
    private Range range;

    /**
     * 属性的Term注解
     */
    private Term term;

    /**
     * 属性的Terms注解
     */
    private Terms terms;

    /**
     * 属性的Wildcard注解
     */
    private Wildcard wildcard;

    /**
     * 属性的Nested注解
     */
    private Nested nested;

    public TemplateProperty(Field field) {
        super(field);

        this.range = field.getAnnotation(Range.class);
        this.term = field.getAnnotation(Term.class);
        this.terms = field.getAnnotation(Terms.class);
        this.wildcard = field.getAnnotation(Wildcard.class);
        this.nested = field.getAnnotation(Nested.class);
    }

}
