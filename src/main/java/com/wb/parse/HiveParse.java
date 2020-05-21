package com.wb.parse;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;


/**
 * @author wenbao
 * @version 1.0.0
 * @Description 目的：获取AST中的表，列，以及对其所做的操作，如SELECT,INSERT
 * 重点：获取SELECT操作中的表和列的相关操作。其他操作这判断到表级别。
 * 实现思路：对AST深度优先遍历，遇到操作的token则判断当前的操作，
 * 遇到TOK_TAB或TOK_TABREF则判断出当前操作的表，遇到子句则压栈当前处理，处理子句,子句处理完，栈弹出。
 * @createTime 11:45 2020/5/18
 */
public class HiveParse {

    private static final String UNKNOWN = "UNKNOWN";
    private Map<String, String> alias = new HashMap<String, String>();
    private Map<String, String> cols = new LinkedHashMap<>();
    private Map<String, String> colAlias = new TreeMap<String, String>();
    private Set<String> tables = new HashSet<>();
    private Stack<String> tableNameStack = new Stack<>();
    private Stack<Operation> operationStack = new Stack<Operation>();
    private AtomicInteger tableLevel = new AtomicInteger(0);
    /**
     * 定义及处理不清晰，修改为query或from节点对应的table集合或许好点。目前正在查询处理的表可能不止一个。
     */
    private String nowQueryTable = "";
    private Operation operation;
    private boolean joinClause = false;

    private enum Operation {
        SELECT, INSERT
    }

    public Set<String> parseIntegral(ASTNode ast) {

        //当前查询所对应到的表集合
        Set<String> set = new HashSet<>();
        prepareToParseCurrentNodeAndChildes(ast);
        set.addAll(parseChildNodes(ast));
        set.addAll(parseCurrentNode(ast, set));
        endParseCurrentNode(ast);
        return set;
    }

    private void endParseCurrentNode(ASTNode ast) {
        if (ast.getToken() != null) {
            //join 从句结束，跳出join
            switch (ast.getToken().getType()) {
                case HiveParser.TOK_RIGHTOUTERJOIN:
                case HiveParser.TOK_LEFTOUTERJOIN:
                case HiveParser.TOK_JOIN:
                    joinClause = false;
                    break;
                case HiveParser.TOK_QUERY:
                    break;
                case HiveParser.TOK_INSERT:
                case HiveParser.TOK_SELECT:
                    nowQueryTable = tableNameStack.pop();
                    operation = operationStack.pop();
                    break;
                default:
            }
        }
    }

    private Set<String> parseCurrentNode(ASTNode ast, Set<String> set) {
        if (ast.getToken() != null) {
            switch (ast.getToken().getType()) {
                case HiveParser.TOK_TABLE_PARTITION:
                    if (ast.getChildCount() != 2) {
                        String table = BaseSemanticAnalyzer
                                .getUnescapedName((ASTNode) ast.getChild(0));
                        if (operation == Operation.SELECT) {
                            nowQueryTable = table;
                        }
                        tables.add(table + "\t" + operation);
                    }
                    break;

                case HiveParser.TOK_TAB:
                    // outputTable
                    String tableTab = BaseSemanticAnalyzer
                            .getUnescapedName((ASTNode) ast.getChild(0));
                    if (operation == Operation.SELECT) {
                        nowQueryTable = tableTab;
                    }
                    tables.add(tableTab + "\t" + operation);
                    break;
                case HiveParser.TOK_TABREF:
                    // inputTable
                    ASTNode tabTree = (ASTNode) ast.getChild(0);
                    String tableName = (tabTree.getChildCount() == 1) ? BaseSemanticAnalyzer
                            .getUnescapedName((ASTNode) tabTree.getChild(0))
                            : BaseSemanticAnalyzer
                            .getUnescapedName((ASTNode) tabTree.getChild(0))
                            + "." + tabTree.getChild(1);
                    if (operation == Operation.SELECT) {
                        if (joinClause && !"".equals(nowQueryTable)) {
                            nowQueryTable += "&" + tableName;
                        } else {
                            nowQueryTable = tableName;
                        }
                        set.add(tableName);
                    }
                    tables.add(tableName + "\t" + operation);
                    if (ast.getChild(1) != null) {
                        String alia = ast.getChild(1).getText().toLowerCase();
                        //sql6 p别名在tabref只对应为一个表的别名。
                        alias.put(alia, tableName);
                    }
                    break;
                case HiveParser.TOK_TABLE_OR_COL:
                    if (ast.getParent().getType() != HiveParser.DOT) {
                        String col = ast.getChild(0).getText().toLowerCase();
                        if (alias.get(col) == null
                                && colAlias.get(nowQueryTable + "." + col) == null) {
                            //sql23
                            if (nowQueryTable.indexOf("&") > 0) {
                                cols.put(UNKNOWN + "." + col, "");
                            } else {
                                cols.put(nowQueryTable + "." + col, "");
                            }
                        }
                    }
                    break;
                case HiveParser.TOK_SUBQUERY:
                    if (ast.getChildCount() == 2) {
                        String tableAlias = unescapeIdentifier(ast.getChild(1)
                                .getText());
                        StringBuilder aliaReal = new StringBuilder();
                        for (String table : set) {
                            aliaReal.append(table).append("&");
                        }
                        if (aliaReal.length() != 0) {
                            aliaReal = new StringBuilder(aliaReal.substring(0, aliaReal.length() - 1));
                        }
                        alias.put(tableAlias, aliaReal.toString());
                    }
                    break;

                case HiveParser.TOK_SELEXPR:
                    if (ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
                        String column = ast.getChild(0).getChild(0).getText()
                                .toLowerCase();
                        if (nowQueryTable.indexOf("&") > 0) {
                            cols.put(UNKNOWN + "." + column, tableLevel.toString());
                        } else if (colAlias.get(nowQueryTable + "." + column) == null) {
                            cols.put(nowQueryTable + "." + column, tableLevel.toString());
                        }
                    } else if (ast.getChild(1) != null) {
                        String columnAlia = ast.getChild(1).getText().toLowerCase();
                        colAlias.put(nowQueryTable + "." + columnAlia, "");
                    }
                    break;
                case HiveParser.DOT:
                    if (ast.getType() == HiveParser.DOT) {
                        if (ast.getChildCount() == 2) {
                            if (ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                                    && ast.getChild(0).getChildCount() == 1
                                    && ast.getChild(1).getType() == HiveParser.Identifier) {
                                String alia = BaseSemanticAnalyzer
                                        .unescapeIdentifier(ast.getChild(0)
                                                .getChild(0).getText()
                                                .toLowerCase());
                                String column = BaseSemanticAnalyzer
                                        .unescapeIdentifier(ast.getChild(1)
                                                .getText().toLowerCase());
                                String realTable = null;
                                if (!tables.contains(alia + "\t" + operation)
                                        && alias.get(alia) == null) {
                                    // [b SELECT, a
                                    // SELECT]
                                    alias.put(alia, nowQueryTable);
                                }
                                if (tables.contains(alia + "\t" + operation)) {
                                    realTable = alia;
                                } else if (alias.get(alia) != null) {
                                    realTable = alias.get(alia);
                                }
                                if (realTable == null || realTable.length() == 0 || realTable.indexOf("&") > 0) {
                                    realTable = UNKNOWN;
                                }
                                cols.put(realTable + "." + column, tableLevel.toString());

                            }
                        }
                    }
                    break;
                case HiveParser.TOK_ALTERTABLE_ADDPARTS:
                case HiveParser.TOK_ALTERTABLE_RENAME:
                case HiveParser.TOK_ALTERTABLE_ADDCOLS:
                    ASTNode alterTableName = (ASTNode) ast.getChild(0);
                    tables.add(alterTableName.getText() + "\t" + operation);
                    break;
                default:
            }
        }
        return set;
    }

    private Set<String> parseChildNodes(ASTNode ast) {
        Set<String> set = new HashSet<>();
        int numCh = ast.getChildCount();
        if (numCh > 0) {
            for (int num = 0; num < numCh; num++) {
                ASTNode child = (ASTNode) ast.getChild(num);
                set.addAll(parseIntegral(child));
            }
        }
        return set;
    }

    private void prepareToParseCurrentNodeAndChildes(ASTNode ast) {
        if (ast.getToken() != null) {
            //join 从句开始
            switch (ast.getToken().getType()) {
                case HiveParser.TOK_RIGHTOUTERJOIN:
                case HiveParser.TOK_LEFTOUTERJOIN:
                case HiveParser.TOK_JOIN:
                    joinClause = true;
                    break;
                case HiveParser.TOK_QUERY:
                    tableNameStack.push(nowQueryTable);
                    operationStack.push(operation);
                    nowQueryTable = "";
                    operation = Operation.SELECT;
                    break;
                case HiveParser.TOK_INSERT:
                    tableNameStack.push(nowQueryTable);
                    operationStack.push(operation);
                    operation = Operation.INSERT;
                    break;
                case HiveParser.TOK_SELECT:
                    tableNameStack.push(nowQueryTable);
                    operationStack.push(operation);
                    operation = Operation.SELECT;
                    tableLevel.incrementAndGet();
                    break;
                default:
            }
        }
    }

    public static String unescapeIdentifier(String val) {
        if (val == null) {
            return null;
        }
        if (val.charAt(0) == '`' && val.charAt(val.length() - 1) == '`') {
            val = val.substring(1, val.length() - 1);
        }
        return val;
    }

    private void output(Map<String, String> map) {
        java.util.Iterator<String> it = map.keySet().iterator();
        while (it.hasNext()) {
            String key = it.next();
            System.out.println(key + "\t" + map.get(key));
        }
    }

    public void parse(ASTNode ast) {
        parseIntegral(ast);
        System.out.println("***************表***************");
        for (String table : tables) {
            System.out.println(table);
        }
        System.out.println("***************列***************");
        output(cols);
        System.out.println("***************别名***************");
        output(alias);
    }

    public static void main(String[] args) throws IOException, ParseException,
            SemanticException {
        //不支持 select * select涉及到hive原数据支持
        ParseDriver pd = new ParseDriver();
        String sql1 = "INSERT OVERWRITE TABLE u_data_new  Select id from zpc1";
        String sql2 = "Select name,ip from zpc2 bieming where age > 10 and area in (select area from city)";
        String sql3 = "INSERT OVERWRITE TABLE u_data_new Select d.name,d.ip from (select name,ip from zpc3 where age > 10 and area in (select area from city)) d";
        String sql5 = "insert overwrite table tmp1 PARTITION (partitionkey='2008-08-15') select * from tmp";
        String sql7 = "SELECT id, value FROM (SELECT id, value FROM p1 UNION ALL  SELECT 4 AS id, 5 AS value FROM p1 limit 1) u";
        String sql8 = "select dd from(select id+1 dd from zpc) d";
        String sql9 = "select dd+1 from(select id+1 dd from zpc) d";
        String sql12 = "select * from tablename where unix_timestamp(cz_time) > unix_timestamp('2050-12-31 15:32:28')";
        String sql16 = "select statis_date,time_interval,gds_cd,gds_nm,sale_cnt,discount_amt,discount_rate,price,etl_time,pay_amt from o2ostore.tdm_gds_monitor_rt where time_interval = from_unixtime(unix_timestamp(concat(regexp_replace(from_unixtime(unix_timestamp('201506181700', 'yyyyMMddHHmm')+ 84600 ,  'yyyy-MM-dd HH:mm'),'-| |:',''),'00'),'yyyyMMddHHmmss'),'yyyy-MM-dd HH:mm:ss')";
        String sql13 = "INSERT OVERWRITE TABLE u_data_new SELECT TRANSFORM (userid, movieid, rating, unixtime) USING 'python weekday_mapper.py' AS (userid, movieid, rating, weekday) FROM u_data";
        String sql14 = "SELECT a.* FROM a JOIN b ON (a.id = b.id AND a.department = b.department)";
        String sql22 = "INSERT OVERWRITE TABLE u_data_new select login.uid from login day_login left outer join (select uid from regusers where dt='20130101') day_regusers on day_login.uid=day_regusers.uid where day_login.dt='20130101' and day_regusers.uid is null";
        String sql23 = "select name from (select * from zpc left outer join def) d";

        System.out.println(sql23);
        HiveParse hp = new HiveParse();
        ASTNode ast = pd.parse(sql23);
        System.out.println(ast.toStringTree());
        hp.parse(ast);
    }

}