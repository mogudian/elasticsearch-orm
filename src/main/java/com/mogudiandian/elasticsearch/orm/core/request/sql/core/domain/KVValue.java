package com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain;

import java.util.AbstractMap;

public class KVValue extends AbstractMap.SimpleEntry<String, Object> implements Cloneable {

    public KVValue(Object value) {
        super(null, value);
    }

    public KVValue(String key, Object value) {
        super(key != null ? key.replace("'", "") : null, value);
    }

    @Override
    public String toString() {
        // 存在只有value没有key的情况
        if (getKey() == null) {
            return getValue().toString();
        } else {
            return getKey() + "=" + getValue();
        }
    }
}
