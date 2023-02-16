package com.mogudiandian.elasticsearch.orm.core.request.sql;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.time.FastDateFormat;

import java.util.Date;

/**
 * SQL
 * 支持的类型为
 * where 支持复杂(嵌套)查询 不支持子查询
 * order by
 * limit
 * 例如
 * where a = b and c = ? and (e = f or (g = h and i = j))
 * where a = b and c = ? order by x asc, y desc
 * where a = b limit ?, ?
 * where a = b and c = ? order by x asc, y desc limit x, y
 *
 * @author sunbo
 */
@Getter
@Setter
public class SQL {

    /**
     * SQL 从where写起
     */
    private String sql;

    /**
     * 参数
     */
    private Object[] parameters;

    /**
     * 默认的时间格式
     */
    private FastDateFormat defaultDateFormat;

    public SQL(String sql) {
        this.sql = sql;
    }

    public SQL(String sql, Object... parameters) {
        this.sql = sql;
        this.parameters = parameters;
    }

    public SQL setDefaultDateFormat(FastDateFormat defaultDateFormat) {
        this.defaultDateFormat = defaultDateFormat;
        return this;
    }

    @Override
    public String toString() {
        return replacePlaceholders();
    }

    /**
     * 替换?占位符
     *
     * @return 替换后的字符串
     */
    private String replacePlaceholders() {
        StringBuilder builder = new StringBuilder(sql.length());
        char lastChar = '\0';
        boolean inQuote = false, inConvert = false;
        int paramIndex = 0;
        // sql: a='i\'m fine? _\\_' and b>?    params: [5]    ==>>    a=\'i'm fine? _\_' and b>5
        //  ||||||    ||  ||        ||
        //  012345    67  89       10 11
        //      lastChar    currentChar    current_inQuote    current_inConvert    current_text_string_buffer    new_inQuote    new_inConvert  new_text_string_buffer
        // 0      a              =              false               false          a                                false            false     a=
        // 1      =              \              false               false          a=                               false            false     a=\
        // 2      \              '
        // 3      '              i
        // 4      i              \
        // 5      \              '
        // 6      e              ?
        // 7      ?              '
        // 8      b              >
        // 9      >              ?

        // 暂时不支持a='ok?'这种的 这种也会替换 如果有这种需求 使用a=? + ['ok?']来解决 同时如果参数的字符串中包含' 可能不支持
        for (int i = 0, len = sql.length(); i < len; i++) {
            char ch = sql.charAt(i);
            if (ch == '?') {
                builder.append(getParameterTextValue(parameters[paramIndex++]));
            } else {
                builder.append(ch);
            }
            lastChar = ch;
        }
        return builder.toString();
    }

    /**
     * 获取参数的文本
     *
     * @param parameter 参数
     * @return 字符串需要用''引起来
     */
    private String getParameterTextValue(Object parameter) {
        if (parameter == null) {
            return "'null'";
        }
        Class<?> cls = parameter.getClass();
        if (cls == Byte.class || cls == Short.class || cls == Integer.class || cls == Long.class || cls == Float.class || cls == Double.class || cls == Boolean.class) {
            return parameter.toString();
        }
        if (Date.class.isAssignableFrom(cls)) {
            return "'" + defaultDateFormat.format((Date) parameter) + "'";
        }
        return "'" + parameter.toString() + "'";
    }
}
