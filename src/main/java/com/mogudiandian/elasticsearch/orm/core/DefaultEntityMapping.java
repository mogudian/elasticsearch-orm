package com.mogudiandian.elasticsearch.orm.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mogudiandian.elasticsearch.orm.core.util.LifecycleUtils;
import com.mogudiandian.elasticsearch.orm.core.util.OrmUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 默认的ES实体映射
 * @param <E> 实体类型
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
public class DefaultEntityMapping<E extends BaseEntity> extends EntityMapping<E> {

    public DefaultEntityMapping(Class<E> entityClass) {
        super(entityClass);
    }

    @Override
    public List<String> searchFields() {
        return OrmUtils.getEntityProperties(super.entityClass)
                       .stream()
                       .filter(EntityProperty::isForSearch)
                       .map(EntityProperty::getName)
                       .map(OrmUtils::encodeSearchFieldName)
                       .collect(Collectors.toList());

    }

    @Override
    public String lifecycleField() {
        return OrmUtils.getEntityProperties(super.entityClass)
                       .stream()
                       .filter(EntityProperty::isForLifecycle)
                       .map(EntityProperty::getName)
                       .map(OrmUtils::encodeLifecycleFieldName)
                       .findFirst()
                       .orElse(null);
    }

    @Override
    public Map<String, Object> convert(E entity) {
        Map<String, Object> map = new HashMap<>();

        map.put(BaseEntity.ID, entity.entityId());
        map.put(BaseEntity.CLAZZ, entity.getClass().getName());

        // 获取实体所有属性并按照特征分组
        Map<Integer, List<EntityProperty>> featureProperties = OrmUtils.getEntityProperties(super.entityClass)
                                                                       .stream()
                                                                       .collect(Collectors.groupingBy(EntityProperty::getFeatures));

        // 无特征
        Optional.ofNullable(featureProperties.get(EntityProperty.NO_FEATURE))
                .ifPresent(list -> list.forEach(x -> map.put(x.getName(), x.invokeGetter(entity))));

        // 将嵌套字段名称encode
        Optional.ofNullable(featureProperties.get(EntityProperty.FEATURE_FOR_NESTED))
                .ifPresent(list -> list.forEach(x -> {
                    Object value = x.invokeGetter(entity);
                    if (value != null) {
                        map.put(OrmUtils.encodeNestedFieldName(x.getName()), JSON.toJSON(value));
                    }
                }));

        // 将搜索字段名称encode
        Optional.ofNullable(featureProperties.get(EntityProperty.FEATURE_FOR_SEARCH))
                .ifPresent(list -> list.forEach(x -> {
                    Object value = x.invokeGetter(entity);
                    if (value instanceof String && x.getSearchField().removeHtmlTags()) {
                        // TODO 后续引用joshua-util再换成原始的
                        // value = HTMLUtils.removeTags((String) value);
                        value = ((String) value).replaceAll("<.+?>", "");
                    }
                    map.put(OrmUtils.encodeSearchFieldName(x.getName()), value);
                }));

        // 将生命周期字段名称encode 将生命周期字段Date/Long的时间进行统一格式后设置
        Optional.ofNullable(featureProperties.get(EntityProperty.FEATURE_FOR_LIFECYCLE))
                .ifPresent(list -> list.forEach(x -> map.put(OrmUtils.encodeLifecycleFieldName(x.getName()), LifecycleUtils.getPropertyValue(entity, x))));

        return map;
    }

    @Override
    public E convert(Map<String, Object> map) {
        JSONObject jsonObject = new JSONObject(map);

        List<EntityProperty> entityProperties = OrmUtils.getEntityProperties(super.entityClass);

        // 搜索字段
        entityProperties.stream()
                        .filter(EntityProperty::isForSearch)
                        .forEach(x -> {
                            Object value = jsonObject.remove(OrmUtils.encodeSearchFieldName(x.getName()));
                            if (value != null) {
                                jsonObject.put(x.getName(), value);
                            }
                        });

        // 生命周期字段
        entityProperties.stream()
                        .filter(EntityProperty::isForLifecycle)
                        .forEach(x -> {
                            Long value = jsonObject.getLong(OrmUtils.encodeLifecycleFieldName(x.getName()));
                            if (value != null) {
                                // 将统一的毫秒时间转换成字段的Date/Long
                                jsonObject.put(x.getName(), LifecycleUtils.revertPropertyValue(x, value));
                                jsonObject.remove(OrmUtils.encodeLifecycleFieldName(x.getName()));
                            }
                        });

        // 嵌套字段
        entityProperties.stream()
                        .filter(EntityProperty::isForNested)
                        .forEach(x -> {
                            Object value = jsonObject.remove(OrmUtils.encodeNestedFieldName(x.getName()));
                            if (value != null) {
                                jsonObject.put(x.getName(), JSON.toJSON(value));
                            }
                        });

        return jsonObject.toJavaObject(super.entityClass);
    }

}
