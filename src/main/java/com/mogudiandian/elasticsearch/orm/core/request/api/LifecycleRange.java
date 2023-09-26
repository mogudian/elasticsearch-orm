package com.mogudiandian.elasticsearch.orm.core.request.api;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

/**
 * 生命周期范围
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
@Getter
@Setter
public class LifecycleRange {

    /**
     * 开始时间(包含)
     */
    private Date start;

    /**
     * 结束时间(不包含)
     */
    private Date end;

    public LifecycleRange(Date start, Date end) {
        this.start = start;
        this.end = end;
    }
}
