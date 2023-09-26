package com.mogudiandian.elasticsearch.orm.core.request;

import com.mogudiandian.elasticsearch.orm.core.EntityMapping;
import com.mogudiandian.elasticsearch.orm.core.request.api.HighlightConfig;
import com.mogudiandian.elasticsearch.orm.core.request.api.PaginationConfig;
import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.time.FastDateFormat;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * 搜索请求条件
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
public abstract class SearchRequest {

    /**
     * 要查询的实体class
     */
    @Getter
    protected Class<? extends BaseEntity> entityClass;

    /**
     * 索引名称
     */
    @Getter
    @Setter
    protected String indexName;

    /**
     * 默认的时间格式
     */
    protected FastDateFormat defaultDateFormat;

    /**
     * 是否开启调试
     */
    @Getter
    protected boolean debug;

    /**
     * 按照实体类来构造
     * @param entityClass 实体类
     */
    public SearchRequest(Class<? extends BaseEntity> entityClass) {
        this.entityClass = entityClass;
    }

    public FastDateFormat getDefaultDateFormat() {
        return defaultDateFormat;
    }

    public void setDefaultDateFormat(String format) {
        this.defaultDateFormat = FastDateFormat.getInstance(format);
    }

    public void enableDebug() {
        debug = true;
    }

    /**
     * 生成搜索条件
     * @param entityMapping 实体映射
     * @param <I> ID类型
     * @param <E> 实体类型
     * @return 搜索条件
     */
    public abstract <E extends BaseEntity> SearchSourceBuilder toSearchSourceBuilder(EntityMapping<E> entityMapping);

    /**
     * 获取分页设置
     */
    public abstract PaginationConfig getPagination();

    /**
     * 是否需要高亮
     * @return 有关键词 && 有高亮设置
     */
    public abstract HighlightConfig getHighlight();
}
