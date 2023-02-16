package com.mogudiandian.elasticsearch.orm.core.util;

import com.mogudiandian.elasticsearch.orm.core.EntityMapping;
import com.mogudiandian.elasticsearch.orm.core.EntityProperty;
import com.mogudiandian.elasticsearch.orm.core.annotation.Index;
import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.ResolvableType;
import org.springframework.core.type.filter.AssignableTypeFilter;

import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ES实体工具
 * @author sunbo
 */
@Slf4j
public final class OrmUtils {

    /**
     * ES中的模糊搜索字段名前缀
     */
    private static final String SEARCH_FIELD_PREFIX = "searchFor";

    /**
     * ES中的生命周期字段名前缀
     */
    private static final String LIFECYCLE_FIELD_PREFIX = "lifecycleFor";

    /**
     * ES中的嵌套字段名前缀
     */
    private static final String NESTED_FIELD_PREFIX = "nestedFor";

    /**
     * entity class -> index name
     */
    private static Map<Class<? extends BaseEntity>, String> entityClasses = new HashMap<>();

    /**
     * BaseEntity子类中的所有属性
     */
    private static Map<Class<? extends BaseEntity>, List<EntityProperty>> entityPropertiesMap = new HashMap<>();

    /**
     * BaseEntity子类中的所有属性对应的字段名
     */
    private static Map<Class<? extends BaseEntity>, Map<String, String>> entityPropertyFieldNames = new HashMap<>();

    /**
     * BaseEntity子类中的所有属性对应的属性
     */
    private static Map<Class<? extends BaseEntity>, Map<String, EntityProperty>> entityPropertyFields = new HashMap<>();

    /**
     * 所有的mapping
     */
    private static Map<Class<? extends BaseEntity>, Class<? extends EntityMapping<? extends BaseEntity>>> entityMappings = new HashMap<>();

    /**
     * 初始化方法，必须提前预热
     */
    public static void init(String basePackage) {
        if (StringUtils.isBlank(basePackage)) {
            throw new RuntimeException("base-package can not be blank");
        }

        // 用spring扫描子类
        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AssignableTypeFilter(BaseEntity.class));
        provider.addIncludeFilter(new AssignableTypeFilter(EntityMapping.class));

        Set<BeanDefinition> components = provider.findCandidateComponents(basePackage);
        try {
            // 遍历子类 获取类型和属性
            for (BeanDefinition component : components) {
                Class<?> clazz = Class.forName(component.getBeanClassName());

                // entity的子类
                if (BaseEntity.class.isAssignableFrom(clazz) && clazz != BaseEntity.class) {
                    Class<? extends BaseEntity> entityClass = (Class<? extends BaseEntity>) clazz;

                    // 处理实体类
                    Index index = clazz.getAnnotation(Index.class);
                    if (index != null) {
                        // 实体类型对应索引名
                        entityClasses.put(entityClass, index.value());
                    } else {
                        log.warn("Entity class {} has no @Index annotation, ignored", clazz);
                        // 去掉是为了兼容@Nested的情况
                        // continue;
                    }

                    // 实体的数据
                    List<EntityProperty> propertyList = Arrays.stream(FieldUtils.getAllFields(clazz))
                                                              .filter(x -> !Modifier.isStatic(x.getModifiers()))
                                                              .map(EntityProperty::new)
                                                              .collect(Collectors.toList());

                    // 只能有一个生命周期的字段
                    if (propertyList.stream().filter(EntityProperty::isForLifecycle).count() > 1) {
                        throw new RuntimeException("Class " + component.getBeanClassName() + " has more than 1 lifecycle fields");
                    }

                    // 实体类型对应属性
                    entityPropertiesMap.put(entityClass, propertyList);

                    entityPropertyFieldNames.put(entityClass, new HashMap<>());
                    entityPropertyFields.put(entityClass, new HashMap<>());

                    for (EntityProperty entityProperty : propertyList) {
                        String propertyName = entityProperty.getName(), fieldName;
                        if (entityProperty.isForSearch()) {
                            fieldName = encodeSearchFieldName(propertyName);
                        } else if (entityProperty.isForLifecycle()) {
                            fieldName = encodeLifecycleFieldName(propertyName);
                        } else if (entityProperty.isForNested()) {
                            fieldName = encodeNestedFieldName(propertyName);
                        } else {
                            fieldName = propertyName;
                        }
                        entityPropertyFieldNames.get(entityClass).put(propertyName, fieldName);
                        entityPropertyFields.get(entityClass).put(propertyName, entityProperty);
                    }
                } else if (EntityMapping.class.isAssignableFrom(clazz) && clazz != EntityMapping.class) {
                    // 处理实体类映射
                    ResolvableType resolvableType = ResolvableType.forClass(clazz);
                    if (!resolvableType.hasGenerics()) {
                        log.warn("Entity Mapping class {} has no <? extends BaseEntity> generic type, ignored", clazz);
                        continue;
                    }
                    // 获取泛型
                    Class<?> genericClass = resolvableType.getGeneric(0).resolve();
                    // 忽略 DefaultElasticEntityMapping
                    if (genericClass != BaseEntity.class) {
                        entityMappings.put((Class<? extends BaseEntity>) genericClass, (Class<? extends EntityMapping<? extends BaseEntity>>) clazz);
                    }
                }
            }
            log.info("{} initialized...", OrmUtils.class);
        } catch (ClassNotFoundException e) {
            log.error("{} init throws ", OrmUtils.class, e);
        }
    }

    /**
     * 实体类型是否可接受
     * @param entityClass 实体类型
     * @return 可接受返回true
     */
    public static boolean isAcceptable(Class<BaseEntity> entityClass) {
        return entityClasses.get(entityClass) != null;
    }

    /**
     * 根据实体的类型获取索引名
     * @param entityClass 实体类型
     * @return 索引名
     */
    public static String getIndexName(Class<? extends BaseEntity> entityClass) {
        if (entityClass == null) {
            throw new NullPointerException();
        }
        String indexName = entityClasses.get(entityClass);
        if (indexName == null) {
            // TODO 后续加一个IndexNameResolver
            throw new RuntimeException("No index found for type " + entityClass);
        }
        return indexName;
    }

    /**
     * 获取类中的所有属性 不包含继承的
     * @param entityClass 实体类
     * @return 所有属性信息
     */
    public static List<EntityProperty> getEntityProperties(Class<? extends BaseEntity> entityClass) {
        return new ArrayList<>(entityPropertiesMap.get(entityClass));
    }

    /**
     * 获取实体属性对应的字段名
     * @param entityClass 实体类
     * @param propertyName 属性名
     * @return 字段名
     */
    public static String getEntityFieldName(Class<? extends BaseEntity> entityClass, String propertyName) {
        return entityPropertyFieldNames.get(entityClass).getOrDefault(propertyName, propertyName);
    }

    /**
     * 属性是否是搜索字段
     * @param entityClass 实体类
     * @param propertyName 属性名
     * @return 字段名
     */
    public static boolean isSearchField(Class<? extends BaseEntity> entityClass, String propertyName) {
        return Optional.ofNullable(entityPropertyFields.get(entityClass))
                       .map(x -> x.get(propertyName))
                       .map(EntityProperty::isForSearch)
                       .orElse(false);
    }

    /**
     * 获取属性的嵌套类型
     * @param entityClass 实体类
     * @param propertyName 属性名
     * @return 字段名
     */
    public static Class<? extends BaseEntity> getNestedFieldType(Class<? extends BaseEntity> entityClass, String propertyName) {
        return Optional.ofNullable(entityPropertyFields.get(entityClass))
                       .map(x -> x.get(propertyName))
                       .map(EntityProperty::getNestedFieldType)
                       .orElse(null);
    }

    /**
     * 根据实体类型获取映射
     * @param entityClass 实体类型
     * @param <E> 实体类型
     * @return 映射
     */
    public static <E extends BaseEntity> Class<EntityMapping<E>> getEntityMapping(Class<?> entityClass) {
        return (Class<EntityMapping<E>>) entityMappings.get(entityClass);
    }

    /**
     * 将类中的属性名encode为ES中的模糊搜索字段名
     * xyz -> searchForXyz
     * @param name 类的属性名
     * @return 字段名
     */
    public static String encodeSearchFieldName(String name) {
        return SEARCH_FIELD_PREFIX + StringUtils.capitalize(name);
    }

    /**
     * 将ES中的模糊搜索字段名解析回类中的属性名
     * searchForXyz -> xyz
     * @param encodedName ES的字段名
     * @return 属性名
     */
    public static String decodeSearchFieldName(String encodedName) {
        if (!encodedName.startsWith(SEARCH_FIELD_PREFIX)) {
            return encodedName;
        }
        return StringUtils.uncapitalize(encodedName.substring(SEARCH_FIELD_PREFIX.length()));
    }

    /**
     * 将类中的属性名encode为ES中的生命周期字段名
     * xyz -> lifecycleForXyz
     * @param name 类的属性名
     * @return 字段名
     */
    public static String encodeLifecycleFieldName(String name) {
        return LIFECYCLE_FIELD_PREFIX + StringUtils.capitalize(name);
    }

    /**
     * 将ES中的生命周期字段名解析回类中的属性名
     * lifecycleForXyz -> xyz
     * @param encodedName ES的字段名
     * @return 属性名
     */
    public static String decodeLifecycleFieldName(String encodedName) {
        if (!encodedName.startsWith(LIFECYCLE_FIELD_PREFIX)) {
            return encodedName;
        }
        return StringUtils.uncapitalize(encodedName.substring(LIFECYCLE_FIELD_PREFIX.length()));
    }

    /**
     * 将类中的属性名encode为ES中的嵌套字段名
     * xyz -> nestedForXyz
     * @param name 类的属性名
     * @return 字段名
     */
    public static String encodeNestedFieldName(String name) {
        return NESTED_FIELD_PREFIX + StringUtils.capitalize(name);
    }

    /**
     * 获取所有的实体类型
     * @return 所有的实体类型集合
     */
    public static Set<Class<? extends BaseEntity>> getAllEntityClasses() {
        return new HashSet<>(entityClasses.keySet());
    }
}
