package com.mogudiandian.elasticsearch.orm.core.request.sql.core.domain.hints;


import com.mogudiandian.elasticsearch.orm.core.request.sql.core.parser.SqlParseException;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.yaml.YamlXContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Eliran on 5/9/2015.
 */
public class HintFactory {

    public static Hint getHintFromString(String hintAsString) {
        if (hintAsString.startsWith("! SHARD_SIZE")) {
            String[] numbers = getParamsFromHint(hintAsString, "! SHARD_SIZE");
            List<Object> params = new ArrayList<>(numbers.length);
            for (String number : numbers) {
                if (number.equals("null") || number.equals("infinity")) {
                    params.add(null);
                } else {
                    params.add(Integer.parseInt(number));
                }
            }
            return new Hint(HintType.SHARD_SIZE, params.toArray());
        }

        if (hintAsString.startsWith("! IGNORE_UNAVAILABLE")) {
            return new Hint(HintType.IGNORE_UNAVAILABLE, null);
        }

        if (hintAsString.startsWith("! ROUTINGS")) {
            String[] routings = getParamsFromHint(hintAsString, "! ROUTINGS");
            return new Hint(HintType.ROUTINGS, routings);
        }
        if (hintAsString.startsWith("! HIGHLIGHT")) {
            String[] heighlights = getParamsFromHint(hintAsString, "! HIGHLIGHT");
            List<Object> hintParams = new ArrayList<>();
            hintParams.add(heighlights[0]);
            if (heighlights.length > 1) {
                StringBuilder builder = new StringBuilder();
                for (int i = 1; i < heighlights.length; i++) {
                    if (i != 1) {
                        builder.append("\n");
                    }
                    builder.append(heighlights[i]);
                }
                String heighlightParam = builder.toString();
                YAMLFactory yamlFactory = new YAMLFactory();
                YAMLParser yamlParser;
                try {
                    yamlParser = yamlFactory.createParser(heighlightParam.toCharArray());
                    YamlXContentParser yamlXContentParser = new YamlXContentParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, yamlParser);
                    Map<String, Object> map = yamlXContentParser.map();
                    hintParams.add(map);
                } catch (IOException e) {
                    throw new SqlParseException("could not parse heighlight hint: " + e.getMessage());
                }
            }
            return new Hint(HintType.HIGHLIGHT, hintParams.toArray());
        }
        if (hintAsString.startsWith("! MINUS_SCROLL_FETCH_AND_RESULT_LIMITS")) {
            Integer[] params = parseParamsAsInts(hintAsString, "! MINUS_SCROLL_FETCH_AND_RESULT_LIMITS");
            if (params.length > 3) {
                throw new SqlParseException("MINUS_FETCH_AND_RESULT_LIMITS should have 3 int params (maxFromFirst,maxFromSecond,hitsPerScrollShard)");
            }
            Integer[] paramsWithDefaults = new Integer[3];
            int defaultMaxFetchFromTable = 100000;
            int defaultFetchOnScroll = 1000;
            paramsWithDefaults[0] = defaultMaxFetchFromTable;
            paramsWithDefaults[1] = defaultMaxFetchFromTable;
            paramsWithDefaults[2] = defaultFetchOnScroll;

            System.arraycopy(params, 0, paramsWithDefaults, 0, params.length);

            return new Hint(HintType.MINUS_FETCH_AND_RESULT_LIMITS, paramsWithDefaults);
        }
        if (hintAsString.startsWith("! MINUS_USE_TERMS_OPTIMIZATION")) {
            String[] param = getParamsFromHint(hintAsString, "! MINUS_USE_TERMS_OPTIMIZATION");
            boolean shouldLowerStringOnTerms = false;
            if (param != null) {
                if (param.length != 1) {
                    throw new SqlParseException("MINUS_USE_TERMS_OPTIMIZATION should have none or one boolean param: false/true ");
                }
                try {
                    shouldLowerStringOnTerms = Boolean.parseBoolean(param[0].toLowerCase());
                } catch (Exception e) {
                    throw new SqlParseException("MINUS_USE_TERMS_OPTIMIZATION should have none or one boolean param: false/true , got:" + param[0]);
                }
            }
            return new Hint(HintType.MINUS_USE_TERMS_OPTIMIZATION, new Object[]{shouldLowerStringOnTerms});
        }
        if (hintAsString.startsWith("! COLLAPSE")) {
            String collapse = getParamFromHint(hintAsString, "! COLLAPSE");
            return new Hint(HintType.COLLAPSE, new String[]{collapse});
        }
        if (hintAsString.startsWith("! POST_FILTER")) {
            String postFilter = getParamFromHint(hintAsString, "! POST_FILTER");
            return new Hint(HintType.POST_FILTER, new String[]{postFilter});
        }
        if (hintAsString.startsWith("! STATS")) {
            String[] statsGroups = getParamsFromHint(hintAsString, "! STATS");
            return new Hint(HintType.STATS, statsGroups);
        }
        if (hintAsString.startsWith("! CONFLICTS")) {
            String conflictsParam = getParamFromHint(hintAsString, "! CONFLICTS");
            return new Hint(HintType.CONFLICTS, new String[]{conflictsParam});
        }
        if (hintAsString.startsWith("! PREFERENCE")) {
            String preferenceParam = getParamFromHint(hintAsString, "! PREFERENCE");
            return new Hint(HintType.PREFERENCE, new String[]{preferenceParam});
        }
        if (hintAsString.startsWith("! TRACK_TOTAL_HITS")) {
            String trackTotalTitsParam = getParamFromHint(hintAsString, "! TRACK_TOTAL_HITS");
            return new Hint(HintType.TRACK_TOTAL_HITS, new String[]{trackTotalTitsParam});
        }
        if (hintAsString.startsWith("! TIMEOUT")) {
            String timeoutParam = getParamFromHint(hintAsString, "! TIMEOUT");
            return new Hint(HintType.TIMEOUT, new String[]{timeoutParam});
        }
        if (hintAsString.startsWith("! INDICES_OPTIONS")) {
            String indicesOptions = getParamFromHint(hintAsString, "! INDICES_OPTIONS");
            if (!indicesOptions.startsWith("{")) {
                indicesOptions = "{" + indicesOptions;
            }
            if (!indicesOptions.endsWith("}")) {
                indicesOptions = indicesOptions + "}";
            }
            return new Hint(HintType.INDICES_OPTIONS, new Object[]{indicesOptions});
        }
        if (hintAsString.startsWith("! MIN_SCORE")) {
            String minScoreParam = getParamFromHint(hintAsString, "! MIN_SCORE");
            return new Hint(HintType.MIN_SCORE, new String[]{minScoreParam});
        }

        return null;
    }

    private static String getParamFromHint(String hint, String prefix) {
        if (!hint.contains("(")) {
            return null;
        }
        return hint.replace(prefix, "").replaceAll("\\s*\\(\\s*", "").replaceAll("\\s*\\,\\s*", ",").replaceAll("\\s*\\)\\s*", "");
    }

    private static String[] getParamsFromHint(String hint, String prefix) {
        String param = getParamFromHint(hint, prefix);
        return param != null ? param.split(",") : null;
    }

    private static Integer[] parseParamsAsInts(String hintAsString, String startWith) {
        String[] number = getParamsFromHint(hintAsString, startWith);
        if (number == null) {
            return new Integer[0];
        }

        Integer[] params = new Integer[number.length];
        for (int i = 0; i < params.length; i++) {
            params[i] = Integer.parseInt(number[i]);
        }
        return params;
    }


}
