package com.wb.util;

import com.wb.bean.ColLine;

import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 解析工具类
 */
public final class ParseUtil {
    private static final Map<Integer, String> HARD_CODE_SCRIPT_MAP = new HashMap<Integer, String>();
    private static final String SPLIT_DOT = ".";
    private static final String SPLIT_COMMA = ",";

    private static final Map<String, Boolean> REGEX_MULTI_VAR_VALUE = new HashMap<String, Boolean>();

    static {
        REGEX_MULTI_VAR_VALUE.put("\\s*=\\s*\"([\\s\\S]*?)\"", false);
        REGEX_MULTI_VAR_VALUE.put("\\s*=\\s*\'([\\s\\S]*?)\'", false);
        REGEX_MULTI_VAR_VALUE.put("\\s*=\\s*(\\w*)", false);
        REGEX_MULTI_VAR_VALUE.put("\\s*=\\s*`([\\s\\S]*?)`", true);

        HARD_CODE_SCRIPT_MAP.put(400, "^\\s*hive\\d?.*?-e\\s*\"([\\s\\S]*)\"");
    }

    /**
     * @param table fact.t1
     * @return [fact, t1]
     */
    public static String[] parseDBTable(String table) {
        return table.split("\\" + SPLIT_DOT);
    }

    public static String collectionToString(Collection<String> coll) {
        return collectionToString(coll, SPLIT_COMMA, true);
    }

    public static String collectionToString(Collection<String> coll, String split, boolean isCheck) {
        StringBuilder sb = new StringBuilder();
        if (Check.notEmpty(coll)) {
            for (String string : coll) {
                boolean flag = !isCheck || Check.notEmpty(string);
                if (flag) {
                    sb.append(string).append(split);
                }
            }
            if (sb.length() > 0) {
                sb.setLength(sb.length() - split.length());
            }
        }
        return sb.toString();
    }

    public static String uniqMerge(String s1, String s2) {
        Set<String> set = new HashSet<String>();
        set.add(s1);
        set.add(s2);
        return collectionToString(set);
    }


    private static String regex(String regex, String content, int group) {
        List<String> regexList = regexList(regex, content, group);
        return Check.isEmpty(regexList) ? "" : regexList.get(0);
    }

    private static List<String> regexList(String regex, String content, int group) {
        Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);
        Matcher matcher = pattern.matcher(content);
        List<String> list = new ArrayList<String>();
        while (matcher.find()) {
            list.add(matcher.group(group));
        }
        return list;
    }

    public static Map<String, String> cloneAliaMap(Map<String, String> map) {
        Map<String, String> map2 = new HashMap<String, String>(map.size());
        for (Entry<String, String> entry : map.entrySet()) {
            map2.put(entry.getKey(), entry.getValue());
        }
        return map2;
    }

    public static Map<String, List<ColLine>> cloneSubQueryMap(Map<String, List<ColLine>> map) {
        Map<String, List<ColLine>> map2 = new HashMap<String, List<ColLine>>(map.size());
        for (Entry<String, List<ColLine>> entry : map.entrySet()) {
            List<ColLine> value = entry.getValue();
            List<ColLine> list = new ArrayList<ColLine>(value.size());
            for (ColLine colLine : value) {
                list.add(cloneColLine(colLine));
            }
            map2.put(entry.getKey(), value);
        }
        return map2;
    }


    public static ColLine cloneColLine(ColLine col) {
        return new ColLine(col.getToNameParse(), col.getColCondition(),
                cloneSet(col.getFromNameSet()), cloneSet(col.getConditionSet()),
                col.getToTableName(), col.getToName());
    }


    public static Set<String> cloneSet(Set<String> set) {
        Set<String> set2 = new HashSet<String>(set.size());
        for (String string : set) {
            set2.add(string);
        }
        return set2;
    }

    public static List<ColLine> cloneList(List<ColLine> list) {
        List<ColLine> list2 = new ArrayList<ColLine>(list.size());
        for (ColLine col : list) {
            list2.add(cloneColLine(col));
        }
        return list2;
    }


    private ParseUtil() {
    }
}
