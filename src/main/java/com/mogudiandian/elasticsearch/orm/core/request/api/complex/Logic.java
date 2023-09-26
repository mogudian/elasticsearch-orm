package com.mogudiandian.elasticsearch.orm.core.request.api.complex;

/**
 * 逻辑符 包括ES的和SQL的
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
public enum Logic {

    // ES的
    MUST, SHOULD, MUST_NOT,

    // SQL的
    AND, OR, NOT;

}
