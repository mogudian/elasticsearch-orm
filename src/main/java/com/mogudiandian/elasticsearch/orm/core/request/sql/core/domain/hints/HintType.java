package com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.hints;

/**
 * Created by Eliran on 29/8/2015.
 */
public enum HintType {
    IGNORE_UNAVAILABLE,
    ROUTINGS,
    SHARD_SIZE,
    HIGHLIGHT,
    MINUS_FETCH_AND_RESULT_LIMITS,
    MINUS_USE_TERMS_OPTIMIZATION,
    COLLAPSE,
    POST_FILTER,
    STATS,
    CONFLICTS,
    PREFERENCE,
    TRACK_TOTAL_HITS,
    TIMEOUT,
    INDICES_OPTIONS,
    MIN_SCORE
}
