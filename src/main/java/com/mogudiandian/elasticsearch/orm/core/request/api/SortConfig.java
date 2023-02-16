package com.mogudiandian.elasticsearch.orm.core.request.api;

import lombok.Getter;
import lombok.Setter;

/**
 * 排序条件
 * @author sunbo
 */
@Getter
@Setter
public class SortConfig {

    /**
     * 排序字段 最好用枚举
     */
    private String field;

    /**
     * 是否降序
     */
    private boolean isDescending;

    public SortConfig(String field) {
        this(field, false);
    }

    public SortConfig(String field, boolean isDescending) {
        this.field = field;
        this.isDescending = isDescending;
    }
}
