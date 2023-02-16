package com.mogudiandian.elasticsearch.orm.core.response;

import com.mogudiandian.elasticsearch.orm.core.BaseEntity;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * 每一个匹配的记录
 * @param <E> 实体类型
 * @author sunbo
 */
@Getter
@Setter
public class MatchRecord<E extends BaseEntity> {

    /**
     * 实体
     */
    private E entity;

    /**
     *
     */
    private Float score;

    private Map<String, List<String>> highlights;

    public MatchRecord(E entity, Float score) {
        this.entity = entity;
        this.score = score;
    }

    public MatchRecord setHighlights(Map<String, List<String>> highlights) {
        this.highlights = highlights;
        return this;
    }
}
