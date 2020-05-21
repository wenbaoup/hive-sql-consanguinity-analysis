package com.wb.parse;

import com.wb.bean.Block;
import com.wb.bean.ColLine;
import com.wb.bean.QueryTree;
import com.wb.bean.SQLResult;
import com.wb.exception.SQLParseException;
import com.wb.util.Check;
import com.wb.util.NumberUtil;
import com.wb.util.ParseUtil;
import com.wb.util.UnSupportedException;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import java.util.*;
import java.util.Map.Entry;

/**
 * hive sql解析类
 * 目的：实现HQL的语句解析，分析出输入输出表、字段和相应的处理条件。为字段级别的数据血缘提供基础。
 * 重点：获取SELECT操作中的表和列的相关操作。其他操作这判断到字段级别。
 * 实现思路：对AST深度优先遍历，遇到操作的token则判断当前的操作，遇到子句则压栈当前处理，处理子句。子句处理完，栈弹出。
 * 处理字句的过程中，遇到子查询就保存当前子查询的信息，判断与其父查询的关系，最终形成树形结构；
 * 遇到字段或者条件处理则记录当前的字段和条件信息、组成Block，嵌套调用。
 * 关键点解析
 * 1、遇到TOK_TAB或TOK_TABREF则判断出当前操作的表
 * 2、压栈判断是否是join，判断join条件
 * 3、定义数据结构Block,遇到在where\select\join时获得其下相应的字段和条件，组成Block
 * 4、定义数据结构ColLine,遇到TOK_SUBQUERY保存当前的子查询信息，供父查询使用
 * 5、定义数据结构ColLine,遇到TOK_UNION结束时，合并并截断当前的列信息
 * 6、遇到select *　或者未明确指出的字段，查询元数据进行辅助分析
 * 7、解析结果进行相关校验
 * 试用范围：
 * 1、支持标准SQL
 * 2、不支持transform using script
 */
public class LineParser {

    private static final String SPLIT_DOT = ".";
    private static final String SPLIT_COMMA = ",";
    private static final String SPLIT_AND = "&";
    private static final String TOK_EOF = "<EOF>";
    private static final String CON_WHERE = "WHERE:";
    private static final String TOK_TMP_FILE = "TOK_TMP_FILE";
    /**
     * table    column
     */
    private Map<String, List<String>> dbMap = new HashMap<String, List<String>>();
    /**
     * 子查询树形关系保存
     */
    private List<QueryTree> queryTreeList = new ArrayList<>();

    private Stack<Set<String>> conditionsStack = new Stack<Set<String>>();
    private Stack<List<ColLine>> colsStack = new Stack<List<ColLine>>();

    private Map<String, List<ColLine>> resultQueryMap = new HashMap<String, List<ColLine>>();
    /**
     * where or join 条件缓存
     */
    private Set<String> conditions = new HashSet<String>();
    /**
     * 一个子查询内的列缓存
     */
    private List<ColLine> cols = new ArrayList<>();

    private Stack<Boolean> joinStack = new Stack<Boolean>();
    private Stack<ASTNode> joinOnStack = new Stack<ASTNode>();

    private Map<String, QueryTree> queryMap = new HashMap<>();
    private boolean joinClause = false;
    private ASTNode joinOn = null;
    /**
     * hive的默认库
     */
    private String nowQueryDB = "default";
    private boolean isCreateTable = false;

    /**
     * 结果
     */
    private List<SQLResult> resultList = new ArrayList<>();
    private List<ColLine> colLines = new ArrayList<>();
    private Set<String> outputTables = new HashSet<String>();
    private Set<String> inputTables = new HashSet<String>();

    private void parseIntegral(ASTNode ast) {
        prepareToParseCurrentNodeAndChildes(ast);
        parseChildNodes(ast);
        parseCurrentNode(ast);
        endParseCurrentNode(ast);
    }

    /**
     * 解析当前节点
     *
     * @param ast
     * @return
     */
    private void parseCurrentNode(ASTNode ast) {
        if (ast.getToken() != null) {
            switch (ast.getToken().getType()) {
                case HiveParser.TOK_CREATETABLE:
                    //outputtable
                    isCreateTable = true;
                    String tableOut = fillDB(BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0)));
                    outputTables.add(tableOut);
                    break;
                case HiveParser.TOK_TAB:
                    // outputTable
                    String tableTab = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                    String tableOut2 = fillDB(tableTab);
                    outputTables.add(tableOut2);
                    break;
                case HiveParser.TOK_TABREF:
                    // inputTable
                    ASTNode tabTree = (ASTNode) ast.getChild(0);
                    String tableInFull = fillDB((tabTree.getChildCount() == 1) ?
                            BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
                            : BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
                            + SPLIT_DOT + BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(1))
                    );
                    String tableIn = tableInFull.substring(tableInFull.indexOf(SPLIT_DOT) + 1);
                    inputTables.add(tableInFull);
                    queryMap.clear();
                    String alia;
                    if (ast.getChild(1) != null) {
                        //(TOK_TABREF (TOK_TABNAME detail usersequence_client) c)
                        alia = ast.getChild(1).getText().toLowerCase();
                        QueryTree qt = new QueryTree();
                        qt.setCurrent(alia);
                        qt.getTableSet().add(tableInFull);
                        QueryTree pTree = getSubQueryParent(ast);
                        qt.setpId(pTree.getpId());
                        qt.setParent(pTree.getParent());
                        queryTreeList.add(qt);
                        if (joinClause && ast.getParent() == joinOn) {
                            // TOK_SUBQUERY join TOK_TABREF ,此处的TOK_SUBQUERY信息不应该清楚
                            //当前的查询范围
                            for (QueryTree entry : queryTreeList) {
                                if (qt.getParent().equals(entry.getParent())) {
                                    queryMap.put(entry.getCurrent(), entry);
                                }
                            }
                        } else {
                            queryMap.put(qt.getCurrent(), qt);
                        }
                    } else {
                        alia = tableIn.toLowerCase();
                        QueryTree qt = new QueryTree();
                        qt.setCurrent(alia);
                        qt.getTableSet().add(tableInFull);
                        QueryTree pTree = getSubQueryParent(ast);
                        qt.setpId(pTree.getpId());
                        qt.setParent(pTree.getParent());
                        queryTreeList.add(qt);

                        if (joinClause && ast.getParent() == joinOn) {
                            for (QueryTree entry : queryTreeList) {
                                if (qt.getParent().equals(entry.getParent())) {
                                    queryMap.put(entry.getCurrent(), entry);
                                }
                            }
                        } else {
                            queryMap.put(qt.getCurrent(), qt);
                            //此处检查查询 select app.t1.c1,t1.c1 from t1 的情况
                            queryMap.put(tableInFull.toLowerCase(), qt);
                        }
                    }
                    break;
                case HiveParser.TOK_SUBQUERY:
                    if (ast.getChildCount() == 2) {
                        String tableAlias = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(1).getText());
                        QueryTree qt = new QueryTree();
                        qt.setCurrent(tableAlias.toLowerCase());
                        qt.setColLineList(generateColLineList(cols, conditions));
                        QueryTree pTree = getSubQueryParent(ast);
                        qt.setId(generateTreeId(ast));
                        qt.setpId(pTree.getpId());
                        qt.setParent(pTree.getParent());
                        qt.setChildList(getSubQueryChildes(qt.getId()));
                        if (Check.notEmpty(qt.getChildList())) {
                            for (QueryTree cqt : qt.getChildList()) {
                                qt.getTableSet().addAll(cqt.getTableSet());
                                // 移除子节点信息
                                queryTreeList.remove(cqt);
                            }
                        }
                        queryTreeList.add(qt);
                        cols.clear();

                        queryMap.clear();
                        for (QueryTree queryTree : queryTreeList) {
                            //当前子查询才保存
                            if (qt.getParent().equals(queryTree.getParent())) {
                                queryMap.put(queryTree.getCurrent(), queryTree);
                            }
                        }
                    }
                    break;
                case HiveParser.TOK_SELEXPR:
                    //输入输出字段的处理
                    /**
                     * (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE))
                     * 	(TOK_SELECT (TOK_SELEXPR TOK_ALLCOLREF))
                     *
                     * (TOK_DESTINATION (TOK_DIR TOK_TMP_FILE))
                     *   	(TOK_SELECT
                     *			(TOK_SELEXPR (. (TOK_TABLE_OR_COL p) datekey) datekey)
                     *			(TOK_SELEXPR (TOK_TABLE_OR_COL datekey))
                     *     	(TOK_SELEXPR (TOK_FUNCTIONDI count (. (TOK_TABLE_OR_COL base) userid)) buyer_count))
                     *     	(TOK_SELEXPR (TOK_FUNCTION when (> (. (TOK_TABLE_OR_COL base) userid) 5) (. (TOK_TABLE_OR_COL base) clienttype) (> (. (TOK_TABLE_OR_COL base) userid) 1) (+ (. (TOK_TABLE_OR_COL base) datekey) 5) (+ (. (TOK_TABLE_OR_COL base) clienttype) 1)) bbbaaa)
                     */
                    //解析需要插入的表
                    Tree tokInsert = ast.getParent().getParent();
                    Tree child = tokInsert.getChild(0).getChild(0);
                    String tName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) child.getChild(0));
                    String destTable = TOK_TMP_FILE.equals(tName) ? TOK_TMP_FILE : fillDB(tName);

                    //select a.*,* from t1 和 select * from (select c1 as a,c2 from t1) t 的情况
                    if (ast.getChild(0).getType() == HiveParser.TOK_ALLCOLREF) {
                        String tableOrAlias = "";
                        if (ast.getChild(0).getChild(0) != null) {
                            tableOrAlias = ast.getChild(0).getChild(0).getChild(0).getText();
                        }
                        String[] result = getTableAndAlia(tableOrAlias);
                        String aliaString = result[1];

                        boolean isSub = false;
                        //处理嵌套select * 的情况
                        if (Check.notEmpty(aliaString)) {
                            //迭代循环的时候查询
                            for (String string : aliaString.split(SPLIT_AND)) {
                                QueryTree qt = queryMap.get(string.toLowerCase());
                                if (null != qt) {
                                    List<ColLine> colLineList = qt.getColLineList();
                                    if (Check.notEmpty(colLineList)) {
                                        isSub = true;
                                        cols.addAll(colLineList);
                                    }
                                }
                            }
                        }
                        if (!isSub) {
                            //处理直接select * 的情况
                            String nowTable = result[0];
                            //fact.test&test2
                            String[] tableArr = nowTable.split(SPLIT_AND);
                            for (String tables : tableArr) {
                                String[] split = tables.split("\\.");
                                if (split.length > 2) {
                                    throw new SQLParseException("parse table:" + nowTable);
                                }
                            }
                        }
                    } else {
                        Block bk = getBlockIntegral((ASTNode) ast.getChild(0));
                        String toNameParse = getToNameParse(ast, bk);
                        Set<String> fromNameSet = filterData(bk.getColSet());
                        ColLine cl = new ColLine(toNameParse, bk.getCondition(), fromNameSet, new LinkedHashSet<>(), destTable, "");
                        cols.add(cl);
                    }
                    break;
                case HiveParser.TOK_WHERE:
                    //3、过滤条件的处理select类
                    conditions.add(CON_WHERE + getBlockIntegral((ASTNode) ast.getChild(0)).getCondition());
                    break;
                default:
                    /**
                     * (or
                     *   (> (. (TOK_TABLE_OR_COL p) orderid) (. (TOK_TABLE_OR_COL c) orderid))
                     *   (and (= (. (TOK_TABLE_OR_COL p) a) (. (TOK_TABLE_OR_COL c) b))
                     *        (= (. (TOK_TABLE_OR_COL p) aaa) (. (TOK_TABLE_OR_COL c) bbb))))
                     */
                    //1、过滤条件的处理join类
                    if (joinOn != null && joinOn.getTokenStartIndex() == ast.getTokenStartIndex()
                            && joinOn.getTokenStopIndex() == ast.getTokenStopIndex()) {
                        ASTNode astCon = (ASTNode) ast.getChild(2);
                        conditions.add(ast.getText().substring(4) + ":" + getBlockIntegral(astCon).getCondition());
                        break;
                    }
            }
        }
    }

    /**
     * 查找当前节点的父子查询节点
     *
     * @param ast
     */
    private QueryTree getSubQueryParent(Tree ast) {
        Tree astTree = ast;
        QueryTree qt = new QueryTree();
        while (!(astTree = astTree.getParent()).isNil()) {
            if (astTree.getType() == HiveParser.TOK_SUBQUERY) {
                qt.setpId(generateTreeId(astTree));
                qt.setParent(BaseSemanticAnalyzer.getUnescapedName((ASTNode) astTree.getChild(1)));
                return qt;
            }
        }
        qt.setpId(-1);
        qt.setParent("NIL");
        return qt;
    }

    private int generateTreeId(Tree tree) {
        return tree.getTokenStartIndex() + tree.getTokenStopIndex();
    }


    /**
     * 查找当前节点的子子查询节点（索引）
     *
     * @param id
     */
    private List<QueryTree> getSubQueryChildes(int id) {
        List<QueryTree> list = new ArrayList<>();
        for (QueryTree qt : queryTreeList) {
            if (id == qt.getpId()) {
                list.add(qt);
            }
        }
        return list;
    }

    /**
     * 获得要解析的名称
     *
     * @param ast
     * @param bk
     * @return
     */
    private String getToNameParse(ASTNode ast, Block bk) {
        String alia = "";
        Tree child = ast.getChild(0);
        //有别名 ip as alia
        if (ast.getChild(1) != null) {
            alia = ast.getChild(1).getText();
            //没有别名 a.ip
        } else if (child.getType() == HiveParser.DOT
                && child.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                && child.getChild(0).getChildCount() == 1
                && child.getChild(1).getType() == HiveParser.Identifier) {
            alia = BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(1).getText());
            //没有别名 ip
        } else if (child.getType() == HiveParser.TOK_TABLE_OR_COL
                && child.getChildCount() == 1
                && child.getChild(0).getType() == HiveParser.Identifier) {
            alia = BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(0).getText());
        }
        return alia;
    }

    /**
     * 获得解析的块，主要应用在WHERE、JOIN和SELECT端
     * 如： <p>where a=1
     * <p>t1 join t2 on t1.col1=t2.col1 and t1.col2=123
     * <p>select count(distinct col1) from t1
     *
     * @param ast
     * @return
     */
    private Block getBlockIntegral(ASTNode ast) {
        if (ast.getType() == HiveParser.KW_OR
                || ast.getType() == HiveParser.KW_AND) {
            Block bk1 = getBlockIntegral((ASTNode) ast.getChild(0));
            Block bk2 = getBlockIntegral((ASTNode) ast.getChild(1));
            bk1.getColSet().addAll(bk2.getColSet());
            bk1.setCondition("(" + bk1.getCondition() + " " + ast.getText() + " " + bk2.getCondition() + ")");
            return bk1;
            //判断条件  > < like in
        } else if (ast.getType() == HiveParser.NOTEQUAL
                || ast.getType() == HiveParser.EQUAL
                || ast.getType() == HiveParser.LESSTHAN
                || ast.getType() == HiveParser.LESSTHANOREQUALTO
                || ast.getType() == HiveParser.GREATERTHAN
                || ast.getType() == HiveParser.GREATERTHANOREQUALTO
                || ast.getType() == HiveParser.KW_LIKE
                || ast.getType() == HiveParser.DIVIDE
                || ast.getType() == HiveParser.PLUS
                || ast.getType() == HiveParser.MINUS
                || ast.getType() == HiveParser.STAR
                || ast.getType() == HiveParser.MOD
                || ast.getType() == HiveParser.AMPERSAND
                || ast.getType() == HiveParser.TILDE
                || ast.getType() == HiveParser.BITWISEOR
                || ast.getType() == HiveParser.BITWISEXOR) {
            Block bk1 = getBlockIntegral((ASTNode) ast.getChild(0));
            // -1
            if (ast.getChild(1) == null) {
                bk1.setCondition(ast.getText() + bk1.getCondition());
            } else {
                Block bk2 = getBlockIntegral((ASTNode) ast.getChild(1));
                bk1.getColSet().addAll(bk2.getColSet());
                bk1.setCondition(bk1.getCondition() + " " + ast.getText() + " " + bk2.getCondition());
            }
            return bk1;
        } else if (ast.getType() == HiveParser.TOK_FUNCTIONDI) {
            Block col = getBlockIntegral((ASTNode) ast.getChild(1));
            String condition = ast.getChild(0).getText();
            col.setCondition(condition + "(distinct (" + col.getCondition() + "))");
            return col;
        } else if (ast.getType() == HiveParser.TOK_FUNCTION) {
            String fun = ast.getChild(0).getText();
            Block col = ast.getChild(1) == null ? new Block() : getBlockIntegral((ASTNode) ast.getChild(1));
            if ("when".equalsIgnoreCase(fun)) {
                col.setCondition(getWhenCondition(ast));
                Set<Block> processChildes = processChilds(ast, 1);
                col.getColSet().addAll(bkToCols(processChildes));
                return col;
            } else if ("IN".equalsIgnoreCase(fun)) {
                col.setCondition(col.getCondition() + " in (" + blockCondToString(processChilds(ast, 2)) + ")");
                return col;
                //isnull isnotnull
            } else if ("TOK_ISNOTNULL".equalsIgnoreCase(fun)
                    || "TOK_ISNULL".equalsIgnoreCase(fun)) {
                col.setCondition(col.getCondition() + " " + fun.toLowerCase().substring(4));
                return col;
            } else if ("BETWEEN".equalsIgnoreCase(fun)) {
                col.setCondition(getBlockIntegral((ASTNode) ast.getChild(2)).getCondition()
                        + " between " + getBlockIntegral((ASTNode) ast.getChild(3)).getCondition()
                        + " and " + getBlockIntegral((ASTNode) ast.getChild(4)).getCondition());
                return col;
            }
            Set<Block> processChilds = processChilds(ast, 1);
            col.getColSet().addAll(bkToCols(processChilds));
            col.setCondition(fun + "(" + blockCondToString(processChilds) + ")");
            return col;
            //map,array
        } else if (ast.getType() == HiveParser.LSQUARE) {
            Block column = getBlockIntegral((ASTNode) ast.getChild(0));
            Block key = getBlockIntegral((ASTNode) ast.getChild(1));
            column.setCondition(column.getCondition() + "[" + key.getCondition() + "]");
            return column;
        } else {
            return parseBlock(ast);
        }
    }


    private Set<String> bkToCols(Set<Block> processChilds) {
        Set<String> set = new LinkedHashSet<String>(processChilds.size());
        for (Block colLine : processChilds) {
            if (Check.notEmpty(colLine.getColSet())) {
                set.addAll(colLine.getColSet());
            }
        }
        return set;
    }

    private String blockCondToString(Set<Block> processChilds) {
        StringBuilder sb = new StringBuilder();
        for (Block colLine : processChilds) {
            sb.append(colLine.getCondition()).append(SPLIT_COMMA);
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    /**
     * 解析when条件
     *
     * @param ast
     * @return case when c1>100 then col1 when c1>0 col2 else col3 end
     */
    private String getWhenCondition(ASTNode ast) {
        int cnt = ast.getChildCount();
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i < cnt; i++) {
            String condition = getBlockIntegral((ASTNode) ast.getChild(i)).getCondition();
            if (i == 1) {
                sb.append("(case when ").append(condition);
                //else
            } else if (i == cnt - 1) {
                sb.append(" else ").append(condition).append(" end)");
                //then
            } else if (i % 2 == 0) {
                sb.append(" then ").append(condition);
            } else {
                sb.append(" when ").append(condition);
            }
        }
        return sb.toString();
    }


    /**
     * 保存subQuery查询别名和字段信息
     *
     * @param sqlIndex
     */
    private void putResultQueryMap(int sqlIndex) {
        List<ColLine> list = generateColLineList(cols, conditions);
        //没有重名的情况就不用标记
        String key = sqlIndex == 0 ? LineParser.TOK_EOF : LineParser.TOK_EOF + sqlIndex;
        resultQueryMap.put(key, list);
    }

    private List<ColLine> generateColLineList(List<ColLine> cols, Set<String> conditions) {
        List<ColLine> list = new ArrayList<ColLine>();
        for (ColLine entry : cols) {
            entry.getConditionSet().addAll(conditions);
            list.add(ParseUtil.cloneColLine(entry));
        }
        return list;
    }

    /**
     * 判断正常列，
     * 正常：a as col, a
     * 异常：1 ，'a' //数字、字符等作为列名
     */
    private boolean notNormalCol(String column) {
        return Check.isEmpty(column) || NumberUtil.isNumeric(column)
                || (column.startsWith("\"") && column.endsWith("\""))
                || (column.startsWith("\'") && column.endsWith("\'"));
    }

    /**
     * 从指定索引位置开始解析子树
     *
     * @param ast
     * @param startIndex 开始索引
     * @return
     */
    private Set<Block> processChilds(ASTNode ast, int startIndex) {
        int cnt = ast.getChildCount();
        Set<Block> set = new LinkedHashSet<>();
        for (int i = startIndex; i < cnt; i++) {
            Block bk = getBlockIntegral((ASTNode) ast.getChild(i));
            if (Check.notEmpty(bk.getCondition()) || Check.notEmpty(bk.getColSet())) {
                set.add(bk);
            }
        }
        return set;
    }


    /**
     * 解析获得列名或者字符数字等和条件
     *
     * @param ast
     * @return
     */
    private Block parseBlock(ASTNode ast) {
        if (ast.getType() == HiveParser.DOT
                && ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                && ast.getChild(0).getChildCount() == 1
                && ast.getChild(1).getType() == HiveParser.Identifier) {
            String column = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(1).getText());
            String alia = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getChild(0).getText());
            return getBlock(column, alia);
        } else if (ast.getType() == HiveParser.TOK_TABLE_OR_COL
                && ast.getChildCount() == 1
                && ast.getChild(0).getType() == HiveParser.Identifier) {
            String column = ast.getChild(0).getText();
            return getBlock(column, null);
        } else if (ast.getType() == HiveParser.Number
                || ast.getType() == HiveParser.StringLiteral
                || ast.getType() == HiveParser.Identifier) {
            Block bk = new Block();
            bk.setCondition(ast.getText());
            bk.getColSet().add(ast.getText());
            return bk;
        }
        return new Block();
    }


    /**
     * 根据列名和别名获得块信息
     *
     * @param column
     * @param alia
     * @return
     */
    private Block getBlock(String column, String alia) {
        String[] result = getTableAndAlia(alia);
        String tableArray = result[0];
        String aliaString = result[1];
//迭代循环的时候查询
        for (String string : aliaString.split(SPLIT_AND)) {
            QueryTree qt = queryMap.get(string.toLowerCase());
            if (Check.notEmpty(column)) {
                for (ColLine colLine : qt.getColLineList()) {
                    if (column.equalsIgnoreCase(colLine.getToNameParse())) {
                        Block bk = new Block();
                        bk.setCondition(colLine.getColCondition());
                        bk.setColSet(ParseUtil.cloneSet(colLine.getFromNameSet()));
                        return bk;
                    }
                }
            }
        }

        Block bk = new Block();
        bk.setCondition(tableArray + SPLIT_DOT + column);
        bk.getColSet().add(tableArray + SPLIT_DOT + column);
        return bk;
    }

    /**
     * 过滤掉无用的列：如col1,123,'2013',col2 ==>> col1,col2
     *
     * @param colSet
     * @return
     */
    private Set<String> filterData(Set<String> colSet) {
        Set<String> set = new LinkedHashSet<String>();
        for (String string : colSet) {
            if (!notNormalCol(string)) {
                set.add(string);
            }
        }
        return set;
    }


    /**
     * 解析所有子节点
     *
     * @param ast
     * @return
     */
    private void parseChildNodes(ASTNode ast) {
        int numCh = ast.getChildCount();
        if (numCh > 0) {
            for (int num = 0; num < numCh; num++) {
                ASTNode child = (ASTNode) ast.getChild(num);
                parseIntegral(child);
            }
        }
    }

    /**
     * 准备解析当前节点
     *
     * @param ast
     */
    private void prepareToParseCurrentNodeAndChildes(ASTNode ast) {
        if (ast.getToken() != null) {
            switch (ast.getToken().getType()) {
                case HiveParser.TOK_SWITCHDATABASE:
                    System.out.println("nowQueryDB changed " + nowQueryDB + " to " + ast.getChild(0).getText());
                    nowQueryDB = ast.getChild(0).getText();
                    break;
                case HiveParser.TOK_TRANSFORM:
                    throw new UnSupportedException("no support transform using clause");
                case HiveParser.TOK_RIGHTOUTERJOIN:
                case HiveParser.TOK_LEFTOUTERJOIN:
                case HiveParser.TOK_JOIN:
                case HiveParser.TOK_LEFTSEMIJOIN:
                case HiveParser.TOK_MAPJOIN:
                case HiveParser.TOK_FULLOUTERJOIN:
                case HiveParser.TOK_UNIQUEJOIN:
                    joinStack.push(joinClause);
                    joinClause = true;
                    joinOnStack.push(joinOn);
                    joinOn = ast;
                    break;
                default:
            }
        }
    }


    /**
     * 结束解析当前节点
     *
     * @param ast
     */
    private void endParseCurrentNode(ASTNode ast) {
        if (ast.getToken() != null) {
            Tree parent = ast.getParent();
            //join 从句结束，跳出join
            switch (ast.getToken().getType()) {
                case HiveParser.TOK_RIGHTOUTERJOIN:
                case HiveParser.TOK_LEFTOUTERJOIN:
                case HiveParser.TOK_JOIN:
                case HiveParser.TOK_LEFTSEMIJOIN:
                case HiveParser.TOK_MAPJOIN:
                case HiveParser.TOK_FULLOUTERJOIN:
                case HiveParser.TOK_UNIQUEJOIN:
                    joinClause = joinStack.pop();
                    joinOn = joinOnStack.pop();
                    break;

                case HiveParser.TOK_QUERY:
                    //union的子节点
                    processUnionStack(ast, parent);
                case HiveParser.TOK_INSERT:
                case HiveParser.TOK_SELECT:
                    break;
                case HiveParser.TOK_UNION:
                    //合并union字段信息
                    mergeUnionCols();
                    //union的子节点
                    processUnionStack(ast, parent);
                    break;
                default:
            }
        }
    }

    private void mergeUnionCols() {
        validateUnion(cols);
        int size = cols.size();
        int colNum = size / 2;
        List<ColLine> list = new ArrayList<ColLine>(colNum);
        //合并字段
        for (int i = 0; i < colNum; i++) {
            ColLine col = cols.get(i);
            for (int j = i + colNum; j < size; j = j + colNum) {
                ColLine col2 = cols.get(j);
                list.add(col2);
                if (notNormalCol(col.getToNameParse()) && !notNormalCol(col2.getToNameParse())) {
                    col.setToNameParse(col2.getToNameParse());
                }
                col.getFromNameSet().addAll(col2.getFromNameSet());

                col.setColCondition(col.getColCondition() + SPLIT_AND + col2.getColCondition());

                Set<String> conditionSet = ParseUtil.cloneSet(col.getConditionSet());
                conditionSet.addAll(col2.getConditionSet());
                conditionSet.addAll(conditions);
                col.getConditionSet().addAll(conditionSet);
            }
        }
        //移除已经合并的数据
        cols.removeAll(list);
    }

    private void processUnionStack(ASTNode ast, Tree parent) {
        boolean isNeedAdd = parent.getType() == HiveParser.TOK_UNION;
        if (isNeedAdd) {
            //有弟节点(是第一节点)
            if (parent.getChild(0) == ast && parent.getChild(1) != null) {
                //压栈
                conditionsStack.push(ParseUtil.cloneSet(conditions));
                conditions.clear();
                colsStack.push(ParseUtil.cloneList(cols));
                cols.clear();
            } else {  //无弟节点(是第二节点)
                //出栈
                if (!conditionsStack.isEmpty()) {
                    conditions.addAll(conditionsStack.pop());
                }
                if (!colsStack.isEmpty()) {
                    cols.addAll(0, colsStack.pop());
                }
            }
        }
    }

    private void parseAST(ASTNode ast) {
        parseIntegral(ast);
    }

    public List<SQLResult> parse(String sqlAll) throws Exception {
        if (Check.isEmpty(sqlAll)) {
            return resultList;
        }
        //清空最终结果集
        startParseAll();
        int i = 0;
        //当前是第几个sql
        for (String sql : sqlAll.split("(?<!\\\\);")) {
            ParseDriver pd = new ParseDriver();
            String trim = sql.toLowerCase().trim();
            if (trim.startsWith("set") || trim.startsWith("add") || Check.isEmpty(trim)) {
                continue;
            }
            ASTNode ast = pd.parse(sql);
            prepareParse();
            parseAST(ast);
            endParse(++i);
        }
        return resultList;
    }

    /**
     * 清空上次处理的结果
     */
    private void startParseAll() {
        resultList.clear();
    }

    private void prepareParse() {
        isCreateTable = false;
        dbMap.clear();

        colLines.clear();
        outputTables.clear();
        inputTables.clear();

        queryMap.clear();
        queryTreeList.clear();
//where or join 条件缓存
        conditionsStack.clear();
        //一个子查询内的列缓存
        colsStack.clear();

        resultQueryMap.clear();
        //where or join 条件缓存
        conditions.clear();
        //一个子查询内的列缓存
        cols.clear();

        joinStack.clear();
        joinOnStack.clear();

        joinClause = false;
        joinOn = null;
    }

    /**
     * 所有解析完毕之后的后期处理
     */
    private void endParse(int sqlIndex) {
        putResultQueryMap(sqlIndex);
        setColLineList();
    }

    /***
     * 设置输出表的字段对应关系
     */
    private void setColLineList() {
        Map<String, List<ColLine>> map = new HashMap<String, List<ColLine>>();
        for (Entry<String, List<ColLine>> entry : resultQueryMap.entrySet()) {
            if (entry.getKey().startsWith(TOK_EOF)) {
                List<ColLine> value = entry.getValue();
                for (ColLine colLine : value) {
                    List<ColLine> list = map.get(colLine.getToTableName());
                    if (Check.isEmpty(list)) {
                        list = new ArrayList<>();
                        map.put(colLine.getToTableName(), list);
                    }
                    list.add(colLine);
                }
            }
        }

        for (Entry<String, List<ColLine>> entry : map.entrySet()) {
            String table = entry.getKey();
            List<ColLine> pList = entry.getValue();
            List<String> dList = dbMap.get(table);
            int metaSize = Check.isEmpty(dList) ? 0 : dList.size();
            //按顺序插入对应的字段
            for (int i = 0; i < pList.size(); i++) {
                ColLine clp = pList.get(i);
                String colName = null;
                if (i < metaSize) {
                    colName = table + SPLIT_DOT + dList.get(i);
                }
                if (isCreateTable && TOK_TMP_FILE.equals(table)) {
                    for (String string : outputTables) {
                        table = string;
                    }
                }
                ColLine colLine = new ColLine(clp.getToNameParse(), clp.getColCondition(),
                        clp.getFromNameSet(), clp.getConditionSet(), table, colName);
                colLines.add(colLine);
            }
        }

        if (Check.notEmpty(colLines)) {
            SQLResult sr = new SQLResult();
            sr.setColLineList(ParseUtil.cloneList(colLines));
            sr.setInputTables(ParseUtil.cloneSet(inputTables));
            sr.setOutputTables(ParseUtil.cloneSet(outputTables));
            resultList.add(sr);
        }
    }


    /**
     * 补全db信息
     * table1 ==>> db1.table1
     * db1.table1 ==>> db1.table1
     * db2.t1&t2 ==>> db2.t1&db1.t2
     *
     * @param nowTable
     */
    private String fillDB(String nowTable) {
        if (Check.isEmpty(nowTable)) {
            return nowTable;
        }
        StringBuilder sb = new StringBuilder();
        //fact.test&test2&test3
        String[] tableArr = nowTable.split(SPLIT_AND);
        for (String tables : tableArr) {
            String[] split = tables.split("\\" + SPLIT_DOT);
            if (split.length > 2) {
                System.out.println(tables);
                throw new SQLParseException("parse table:" + nowTable);
            }
            String db = split.length == 2 ? split[0] : nowQueryDB;
            String table = split.length == 2 ? split[1] : split[0];
            sb.append(db).append(SPLIT_DOT).append(table).append(SPLIT_AND);
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }


    /**
     * 根据别名查询表明
     *
     * @param alia
     * @return
     */
    private String[] getTableAndAlia(String alia) {
        String aliaString = Check.notEmpty(alia) ? alia :
                ParseUtil.collectionToString(queryMap.keySet(), SPLIT_AND, true);
        String[] result = {"", aliaString};
        Set<String> tableSet = new HashSet<String>();
        if (Check.notEmpty(aliaString)) {
            String[] split = aliaString.split(SPLIT_AND);
            for (String string : split) {
                //别名又分单独起的别名 和 表名，即 select a.col,table_name.col from table_name a
                if (inputTables.contains(string) || inputTables.contains(fillDB(string))) {
                    tableSet.add(fillDB(string));
                } else if (queryMap.containsKey(string.toLowerCase())) {
                    tableSet.addAll(queryMap.get(string.toLowerCase()).getTableSet());
                }
            }
            result[0] = ParseUtil.collectionToString(tableSet, SPLIT_AND, true);
            result[1] = aliaString;
        }
        return result;
    }

    /**
     * 校验union
     *
     * @param list
     */
    private void validateUnion(List<ColLine> list) {
        int size = list.size();
        if (size % 2 == 1) {
            throw new SQLParseException("union column number are different, size=" + size);
        }
        int colNum = size / 2;
        checkUnion(list, 0, colNum);
        checkUnion(list, colNum, size);
    }

    private void checkUnion(List<ColLine> list, int start, int end) {
        String tmp = null;
        //合并字段
        for (int i = start; i < end; i++) {
            ColLine col = list.get(i);
            if (Check.isEmpty(tmp)) {
                tmp = col.getToTableName();
            } else if (!tmp.equals(col.getToTableName())) {
                throw new SQLParseException("union column number/types are different,table1=" + tmp + ",table2=" + col.getToTableName());
            }
        }
    }

}
