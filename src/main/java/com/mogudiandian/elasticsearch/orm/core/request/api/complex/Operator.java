package com.mogudiandian.elasticsearch.orm.core.request.api.complex;

import org.springframework.util.Assert;

/**
 * 操作符 包括ES的和SQL的
 *
 * @author Joshua Sun
 * @since 1.0.0
 */
public enum Operator {

    /**
     * ES的term
     */
    TERM() {
        @Override
        void check(Object... values) {
            Assert.isTrue(values.length == 1, "term/equal values must be only one");
        }
    },
    EQUAL() {
        @Override
        void check(Object... values) {
            TERM.check(values);
        }
    },

    TERMS() {
        @Override
        void check(Object... values) {
            Assert.isTrue(values.length > 0, "terms/in values must be not empty");
        }
    },
    IN() {
        @Override
        void check(Object... values) {
            TERMS.check(values);
        }
    }

    , LIKE() {
        @Override
        void check(Object... values) {
            Assert.isTrue(values.length == 1 && values[0] instanceof String, "like values must be only one string");
        }
    },
    WILDCARD() {
        @Override
        void check(Object... values) {
            LIKE.check(values);
        }
    },

    RANGE() {
        @Override
        void check(Object... values) {
            Assert.isTrue(values.length == 2 || values.length == 4, "range/between values count must be 2 or 4");
            if (values.length == 2) {
                Assert.isTrue(values[1] instanceof Boolean && values[3] instanceof Boolean, "range/between values must contain include flag");
            }
        }
    },
    BETWEEN() {
        @Override
        void check(Object... values) {
            RANGE.check(values);
        }
    },

    // 通用的
    GT() {
        @Override
        void check(Object... values) {
            Assert.isTrue(values.length == 1, "gt values must be only one");
        }
    },
    GTE() {
        @Override
        void check(Object... values) {
            Assert.isTrue(values.length == 1, "gte values must be only one");
        }
    },
    LT() {
        @Override
        void check(Object... values) {
            Assert.isTrue(values.length == 1, "lt values must be only one");
        }
    },
    LTE() {
        @Override
        void check(Object... values) {
            Assert.isTrue(values.length == 1, "lte values must be only one");
        }
    },

    EXISTS() {
        @Override
        void check(Object... values) {
            Assert.isTrue(values.length == 0, "exists values must be empty");
        }
    },

    NOT_EXISTS() {
        @Override
        void check(Object... values) {
            Assert.isTrue(values.length == 0, "not exists values must be empty");
        }
    },

    NESTED() {
        @Override
        void check(Object... values) {
            Assert.isTrue(values.length >= 1, "nested values must be not empty");
        }
    };

    void check(Object... values) {
    }

}
