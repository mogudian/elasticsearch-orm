package com.mogudiandian.elasticsearch.orm.core;

import com.mogudiandian.elasticsearch.orm.core.annotation.LifecycleField;
import com.mogudiandian.elasticsearch.orm.core.annotation.NestedField;
import com.mogudiandian.elasticsearch.orm.core.annotation.SearchField;
import org.springframework.core.ResolvableType;

import java.lang.reflect.Field;
import java.util.Collection;

/**
 * 实体的属性
 * @author sunbo
 */
public class EntityProperty extends ObjectProperty {

    /**
     * 特征值
     */
    public static final int NO_FEATURE = 0, FEATURE_FOR_SEARCH = 1, FEATURE_FOR_LIFECYCLE = 2, FEATURE_FOR_NESTED = 4;

    /**
     * 表示搜索字段的注解
     */
    private SearchField searchField;

    /**
     * 是否是搜索字段
     */
    private boolean forSearch;

    /**
     * 表示生命周期的注解
     */
    private LifecycleField lifecycleField;

    /**
     * 是否是生命周期字段
     */
    private boolean forLifecycle;

    /**
     * 表示嵌套字段的注解
     */
    private NestedField nestedField;

    /**
     * 是否是嵌套字段
     */
    private boolean forNested;

    /**
     * 是否是嵌套字段
     */
    private Class<? extends BaseEntity> nestedFieldType;

    public EntityProperty(Field field) {
        super(field);

        this.searchField = field.getAnnotation(SearchField.class);
        this.forSearch = (this.searchField != null);
        this.lifecycleField = field.getAnnotation(LifecycleField.class);
        this.forLifecycle = (this.lifecycleField != null);
        this.nestedField = field.getAnnotation(NestedField.class);
        this.forNested = (this.nestedField != null);
        if (this.forNested) {
            Class<?> fieldType = field.getType();
            if (BaseEntity.class.isAssignableFrom(fieldType)) {
                this.nestedFieldType = (Class<? extends BaseEntity>) field.getType();
            } else if (fieldType.isArray()) {
                if (BaseEntity.class.isAssignableFrom(fieldType.getComponentType())) {
                    this.nestedFieldType = (Class<? extends BaseEntity>) fieldType.getComponentType();
                }
            } else if (Collection.class.isAssignableFrom(fieldType)) {
                ResolvableType resolvableType = ResolvableType.forField(field);
                Class<?> genericType = resolvableType.getGeneric(0).resolve();
                if (BaseEntity.class.isAssignableFrom(genericType)) {
                    this.nestedFieldType = (Class<? extends BaseEntity>) genericType;
                }
            }
        }
    }

    /**
     * 属性是否需要被搜索
     * @return 是否需要被搜索
     */
    public boolean isForSearch() {
        return forSearch;
    }

    public SearchField getSearchField() {
        return searchField;
    }

    /**
     * 属性是否表示生命周期
     * @return
     */
    public boolean isForLifecycle() {
        return forLifecycle;
    }

    public LifecycleField getLifecycleField() {
        return lifecycleField;
    }

    public NestedField getNestedField() {
        return nestedField;
    }

    public boolean isForNested() {
        return forNested;
    }

    public Class<? extends BaseEntity> getNestedFieldType() {
        return nestedFieldType;
    }

    /**
     * 获取属性特征
     * @return 所有特征值求和
     */
    public int getFeatures() {
        int feature = 0;
        if (isForSearch()) {
            feature |= FEATURE_FOR_SEARCH;
        }
        if (isForLifecycle()) {
            feature |= FEATURE_FOR_LIFECYCLE;
        }
        if (isForNested()) {
            feature |= FEATURE_FOR_NESTED;
        }
        return feature;
    }

}
