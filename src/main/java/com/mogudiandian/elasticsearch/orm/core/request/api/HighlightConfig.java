package com.mogudiandian.elasticsearch.orm.core.request.api;

import lombok.Setter;

import java.util.Optional;

/**
 * 高亮的配置
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
@Setter
public class HighlightConfig {

    /**
     * 高亮前置标签
     */
    private String preTag;

    /**
     * 高亮后置标签
     */
    private String postTag;

    /**
     * 需要高亮的词
     */
    private String keyword;

    public HighlightConfig(String preTag, String postTag) {
        this(preTag, postTag, null);
    }

    public HighlightConfig(String preTag, String postTag, String keyword) {
        this.preTag = preTag;
        this.postTag = postTag;
        this.keyword = keyword;
    }

    public boolean isHighlight() {
        return (preTag != null && preTag.isEmpty()) || (postTag != null && postTag.isEmpty());
    }

    public String getPreTag() {
        return Optional.ofNullable(preTag).orElse("");
    }

    public String getPostTag() {
        return Optional.ofNullable(postTag).orElse("");
    }

    public HighlightConfig setKeyword(String keyword) {
        this.keyword = keyword;
        return this;
    }
}
