package com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser;

/**
 * SQL解析异常
 *
 * @author sunbo
 */
public class SqlParseException extends RuntimeException {

    public SqlParseException(String message) {
        super(message);
    }

}
