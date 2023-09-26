package com.mogudiandian.elasticsearch.orm.core.request.api.annotated;

import com.mogudiandian.elasticsearch.orm.core.request.api.annotated.es.Terms;
import com.mogudiandian.elasticsearch.orm.core.request.api.annotated.es.Wildcard;
import com.mogudiandian.elasticsearch.orm.core.util.OrmUtils;
import com.mogudiandian.elasticsearch.orm.core.request.api.ApiSearchRequest;
import com.mogudiandian.elasticsearch.orm.core.request.api.annotated.es.Range;
import com.mogudiandian.elasticsearch.orm.core.request.api.annotated.es.Term;
import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.elasticsearch.index.query.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 复杂搜索条件
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
@Slf4j
public class AnnotatedSearchRequest extends ApiSearchRequest {

    private static ConcurrentHashMap<Class<SearchTemplate>, Map<String, TemplateProperty>> templateClassProperties = new ConcurrentHashMap<>();

    /**
     * 搜索类
     */
    private SearchTemplate searchTemplate;

    public AnnotatedSearchRequest(Class<? extends BaseEntity> entityClass, SearchTemplate searchTemplate) {
        super(entityClass);
        this.searchTemplate = searchTemplate;
    }

    private Map<String, TemplateProperty> getTemplateProperties0(Class<SearchTemplate> clazz) {
        Field[] fields = Arrays.stream(FieldUtils.getAllFields(clazz))
                               .filter(x -> !Modifier.isStatic(x.getModifiers()))
                               .toArray(Field[]::new);
        Map<String, TemplateProperty> map = new HashMap<>(fields.length, 1);
        for (Field field : fields) {
            try {
                map.put(field.getName(), new TemplateProperty(field));
            } catch (Exception e) {
                log.warn("get field metadata failure, field is: {}, exception is: ", field, e);
            }
        }
        return map;
    }

    private Map<String, TemplateProperty> getTemplateProperties(Class<SearchTemplate> clazz) {
        return templateClassProperties.computeIfAbsent(clazz, k -> getTemplateProperties0(clazz));
    }

    @Override
    public QueryBuilder toQueryBuilder() {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        Map<String, TemplateProperty> objectProperties = getTemplateProperties((Class<SearchTemplate>) searchTemplate.getClass());
        // TODO 根据.号分组

        for (Map.Entry<String, TemplateProperty> entry : objectProperties.entrySet()) {
            String fieldName = entry.getKey();
            boolean isSearchField = OrmUtils.isSearchField(entityClass, fieldName);

            TemplateProperty property = entry.getValue();
            Object value = property.invokeGetter(searchTemplate);
            if (value != null) {
                Wildcard wildcard = property.getWildcard();
                if (wildcard != null) {
                    if (isSearchField) {
                        boolQueryBuilder.must(QueryBuilders.matchPhraseQuery(fieldName, value));
                    } else {
                        if (StringUtils.isNotBlank(wildcard.name())) {
                            fieldName = wildcard.name();
                        }
                        // 根据实体属性的字段名进行replace
                        fieldName = OrmUtils.getEntityFieldName(entityClass, fieldName);
                        if (wildcard.leftWildcard()) {
                            value = "*" + value;
                        }
                        if (wildcard.rightWildcard()) {
                            value = value + "*";
                        }
                        WildcardQueryBuilder query = QueryBuilders.wildcardQuery(fieldName, (String) value);
                        boolQueryBuilder.must(query);
                    }
                    continue;
                }
                Term term = property.getTerm();
                if (term != null) {
                    if (StringUtils.isNotBlank(term.name())) {
                        fieldName = term.name();
                    }
                    // 根据实体属性的字段名进行replace
                    fieldName = OrmUtils.getEntityFieldName(entityClass, fieldName);
                    boolQueryBuilder.must(QueryBuilders.termsQuery(fieldName, value));
                    continue;
                }
                Range range = property.getRange();
                if (range != null) {
                    if (StringUtils.isNotBlank(range.name())) {
                        fieldName = range.name();
                    }
                    // 根据实体属性的字段名进行replace
                    fieldName = OrmUtils.getEntityFieldName(entityClass, fieldName);
                    RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(fieldName)
                                                                       .includeLower(range.includeLower())
                                                                       .includeUpper(range.includeUpper());
                    if (range.lt()) {
                        rangeQueryBuilder.lt(value);
                    }
                    if (range.gt()) {
                        rangeQueryBuilder.gt(value);
                    }
                    boolQueryBuilder.must(rangeQueryBuilder);
                    continue;
                }
                Terms terms = property.getTerms();
                if (terms != null) {
                    if (StringUtils.isNotBlank(terms.name())) {
                        fieldName = terms.name();
                    }
                    // 根据实体属性的字段名进行replace
                    fieldName = OrmUtils.getEntityFieldName(entityClass, fieldName);
                    if (value.getClass().isArray()) {
                        boolQueryBuilder.must(QueryBuilders.termsQuery(fieldName, value));
                    } else {
                        boolQueryBuilder.must(QueryBuilders.termsQuery(fieldName, (Collection<?>) value));
                    }
                    continue;
                }
            }
        }

        return boolQueryBuilder;
    }

}