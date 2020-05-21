package com.wb.parse;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * @author wenbao
 * @version 1.0.0
 * @Description 解析table 之间的血缘关系
 * @createTime 14:44 2020/5/18
 */
public class HiveTableLineageInfo implements NodeProcessor {

    /**
     * Stores input tables in sql.
     */
    private TreeSet<String> inputTableSet = new TreeSet<>();
    /**
     * Stores output tables in sql.
     */
    private TreeSet<String> outputTableSet = new TreeSet<>();

    /**
     * @return java.util.TreeSet
     */
    private TreeSet getInputTableSet() {
        return inputTableSet;
    }

    /**
     * @return java.util.TreeSet
     */
    private TreeSet getOutputTableSet() {
        return outputTableSet;
    }

    /**
     * Implements the process method for the NodeProcessor interface.
     */
    @Override
    public Object process(Node nd, Stack stack, NodeProcessorCtx procCtx,
                          Object... nodeOutputs) throws SemanticException {
        ASTNode pt = (ASTNode) nd;

        switch (pt.getToken().getType()) {

            case HiveParser.TOK_CREATETABLE:
                outputTableSet.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0)));
                break;
            case HiveParser.TOK_TAB:
                outputTableSet.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0)));
                break;

            case HiveParser.TOK_TABREF:
                ASTNode tabTree = (ASTNode) pt.getChild(0);
                String tableName = (tabTree.getChildCount() == 1) ?
                        BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) :
                        BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "." + tabTree.getChild(1);
                inputTableSet.add(tableName);
                break;
            default:
        }
        return null;
    }

    /**
     * parses given query and gets the lineage info.
     *
     * @param query
     * @throws ParseException
     */
    public void getLineageInfo(String query) throws ParseException, SemanticException {
        /*
         * Get the AST tree
         */
        ParseDriver pd = new ParseDriver();
        ASTNode tree = pd.parse(query);

        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }

        /*
         * initialize Event Processor and dispatcher.
         */
        inputTableSet.clear();
        outputTableSet.clear();

        // create a walker which walks the tree in a DFS manner while maintaining
        // the operator stack. The dispatcher
        // generates the plan from the operator tree
        Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();

        // The dispatcher fires the processor corresponding to the closest matching
        // rule and passes the context along
        Dispatcher dispatcher = new DefaultRuleDispatcher(this, rules, null);
        GraphWalker ogw = new DefaultGraphWalker(dispatcher);

        // Create a list of topop nodes
        List<Node> topNodes = new ArrayList<Node>();
        topNodes.add(tree);
        ogw.startWalking(topNodes, null);
    }

    public static void main(String[] args) throws IOException, ParseException,
            SemanticException {

        //默认所有的临时表都以temp开头
        //所有sql都是从页面copy得到 需要做空格之类的转换  异常ParseException line 3:0 character ’ ’ not supported here
        //1.所有SQL必须去掉开头空格   2.如果还有异常  用idea里面的空格替换原有sql中的空格
        String str1 = "with temp_0 as (\n" +
                "select substring(pay_time,1,10) dt,user_id,sum(b.xxbi_consume+b.giftcard_consume) pay_fee\n" +
                "from order_all a inner join xxbi_trade b on a.order_id = b.order_id \n" +
                "where substring(pay_time,1,10)>='2016-04-17' and substring(pay_time,1,10)<='2016-04-30'\n" +
                "group by substring(pay_time,1,10),user_id\n" +
                "union all\n" +
                "select substring(a.pay_time,1,10) dt,a.uid user_id,sum(b.pay_fee) pay_fee\n" +
                "from unified_orders a inner join payments b on a.order_id=b.order_id\n" +
                "where pay_type in ('WXAPP','WXWAP')\n" +
                "and substring(b.pay_time,1,10)>='2016-04-17' and substring(b.pay_time,1,10)<='2016-04-30'\n" +
                "group by substring(a.pay_time,1,10),a.uid\n" +
                "),temp_1 as (\n" +
                "select distinct substring(pay_time,1,10) dt from order_all where substring(pay_time,1,10)>='2016-04-17' and substring(pay_time,1,10)<='2016-04-30'\n" +
                "),temp_2 as (\n" +
                "select dt,user_id,sum(pay_fee) pay_fee from temp_0 group by dt,user_id\n" +
                "),temp_3 as (\n" +
                "select temp_2.*,temp_1.dt zdt from temp_1 join temp_2 on 1=1\n" +
                ")\n" +
                "select dt,\n" +
                "sum(case when (datediff(dt,zdt)=0) then pay_fee end) pay,\n" +
                "count(distinct case when (datediff(dt,zdt)=0) then user_id end) user_cnt,\n" +
                "sum(case when datediff(dt,zdt)>0 and datediff(dt,zdt)<=7 then pay_fee end ) pay_seven,\n" +
                "count(distinct case when datediff(dt,zdt)>0 and datediff(dt,zdt)<=7 then user_id end) user_seven\n" +
                "from temp_3 \n" +
                "where dt >= '2016-04-24'and dt<= '2016-04-30' \n" +
                "group by dt\n";

        String str2 = "INSERT OVERWRITE TABLE u_data_new SELECT TRANSFORM (userid, movieid, rating, unixtime) USING 'python weekday_mapper.py' AS (userid, movieid, rating, weekday) FROM u_data";

        String str3 = "select * from (select id,devid,job_time from tb_in_base) a";

        String str4 = "SELECT\n" +
                "a.fieldA,\n" +
                "rightTable.fieldB,\n" +
                "rightTable.fieldC\n" +
                "FROM\n" +
                "(\n" +
                "SELECT\n" +
                "leftTable.fieldA\n" +
                "FROM\n" +
                "leftTable\n" +
                "WHERE\n" +
                "leftTable.ds >= '2018-09-22'\n" +
                "AND leftTable.ds <= '2018-10-21'\n" +
                "AND NOT EXISTS (\n" +
                "SELECT\n" +
                "1\n" +
                "FROM\n" +
                "otherTable\n" +
                "WHERE\n" +
                "otherTable.ds >= '2018-09-22'\n" +
                "AND otherTable.ds <= '2018-10-21'\n" +
                "AND leftTable.fieldA = otherTable.fieldA\n" +
                ")\n" +
                ") a\n" +
                "LEFT JOIN rightTable ON (\n" +
                "rightTable.ds = '2018-10-30'\n" +
                "AND a.fieldA = rightTable.fieldA\n" +
                ")\n" +
                "WHERE\n" +
                "rightTable.fieldA IS NOT NULL\n";

        String str5 = "insert\n" +
                "overwrite\n" +
                "table\n" +
                "t_md_soft_wp7_dload\n" +
                "partition(ds=20120820)\n" +
                "select g_f,dload_count,dload_user,tensoft_dload_count,tensoft_dload_user,outsoft_dload_count,outsoft_dload_user\n" +
                "from\n" +
                "(select\n" +
                "temp1.g_f,\n" +
                "temp1.dload_count,\n" +
                "temp1.dload_user,\n" +
                "temp2.tensoft_dload_count,\n" +
                "temp2.tensoft_dload_user,\n" +
                "temp3.outsoft_dload_count,\n" +
                "temp3.outsoft_dload_user\n" +
                "from\n" +
                "(select\n" +
                "g_f,\n" +
                "count(1) as dload_user,\n" +
                "sum(t1.pv) as dload_count\n" +
                "from\n" +
                "(select\n" +
                "g_f,\n" +
                "cookie_id,\n" +
                "count(1) as pv\n" +
                "from\n" +
                "t_od_soft_wp7_dload\n" +
                "where\n" +
                "ds=20120820\n" +
                "group by g_f,cookie_id) t1\n" +
                "group by g_f) temp1 left outer join\n" +
                "(select\n" +
                "g_f, count(1) as tensoft_dload_user,\n" +
                "sum(tt3.login_pv) as tensoft_dload_count\n" +
                "from\n" +
                "(select\n" +
                "g_f,\n" +
                "cookie_id,\n" +
                "count(1) as login_pv\n" +
                "from\n" +
                "t_od_soft_wp7_dload tt1 join t_rd_soft_wp7_app tt2 on tt1.ds=tt2.ds and tt1.ios_soft_id = tt2.appid\n" +
                "where\n" +
                "tt1.ds=20120820 and tt2.is_self_rd = 1\n" +
                "group by g_f,cookie_id) tt3\n" +
                "group by g_f) temp2 on temp1.g_f = temp2.g_f\n" +
                "left outer join\n" +
                "(select\n" +
                "g_f,\n" +
                "count(1) as outsoft_dload_user,\n" +
                "sum(tt6.login_pv) as outsoft_dload_count\n" +
                "from\n" +
                "(select\n" +
                "g_f,\n" +
                "cookie_id,\n" +
                "count(1) as login_pv\n" +
                "from\n" +
                "t_od_soft_wp7_dload tt4 join t_rd_soft_wp7_app tt5 on tt4.ds=tt5.ds and tt4.ios_soft_id = tt5.appid\n" +
                "where\n" +
                "tt4.ds=20120820 and tt5.is_self_rd = 0\n" +
                "group by g_f,cookie_id) tt6\n" +
                "group by g_f) temp3 on temp1.g_f = temp3.g_f\n" +
                "union all\n" +
                "select\n" +
                "temp4.g_f,\n" +
                "temp4.dload_count,\n" +
                "temp4.dload_user,\n" +
                "temp5.tensoft_dload_count,\n" +
                "temp5.tensoft_dload_user,\n" +
                "temp6.outsoft_dload_count,\n" +
                "temp6.outsoft_dload_user\n" +
                "from\n" +
                "(select\n" +
                "cast('-1' as bigint) as g_f,\n" +
                "count(1) as dload_user,\n" +
                "sum(tt7.pv) as dload_count\n" +
                "from\n" +
                "(select\n" +
                "cast('-1' as bigint) as g_f,\n" +
                "cookie_id,\n" +
                "count(1) as pv\n" +
                "from\n" +
                "t_od_soft_wp7_dload\n" +
                "where\n" +
                "ds=20120820\n" +
                "group by g_f,cookie_id) tt7\n" +
                "group by g_f) temp4 left outer join\n" +
                "(select\n" +
                "cast('-1' as bigint) as g_f,\n" +
                "count(1) as tensoft_dload_user,\n" +
                "sum(tt10.login_pv) as tensoft_dload_count\n" +
                "from\n" +
                "(select\n" +
                "cast('-1' as bigint) as g_f,\n" +
                "cookie_id,\n" +
                "count(1) as login_pv\n" +
                "from\n" +
                "t_od_soft_wp7_dload tt8 join t_rd_soft_wp7_app tt9 on tt8.ds=tt9.ds and tt8.ios_soft_id = tt9.appid\n" +
                "where\n" +
                "tt8.ds=20120820 and tt9.is_self_rd = 1\n" +
                "group by g_f,cookie_id) tt10\n" +
                "group by g_f) temp5 on temp4.g_f = temp5.g_f\n" +
                "left outer join\n" +
                "(select\n" +
                "cast('-1' as bigint) as g_f,\n" +
                "count(1) as outsoft_dload_user,\n" +
                "sum(tt13.login_pv) as outsoft_dload_count\n" +
                "from\n" +
                "(select\n" +
                "cast('-1' as bigint) as g_f,\n" +
                "cookie_id,\n" +
                "count(1) as login_pv\n" +
                "from\n" +
                "t_od_soft_wp7_dload tt11 join t_rd_soft_wp7_app tt12 on tt11.ds=tt12.ds and tt11.ios_soft_id = tt12.appid\n" +
                "where\n" +
                "tt11.ds=20120820 and tt12.is_self_rd = 0\n" +
                "group by g_f,cookie_id) tt13\n" +
                "group by g_f) temp6 on temp4.g_f = temp6.g_f ) t";


        String query;
        if (1 == args.length) {
            query = args[0];
        } else {
            query = str5;
        }


        HiveTableLineageInfo lep = new HiveTableLineageInfo();
        lep.getLineageInfo(query);
        //todo 可以通过临时表命名规范来去掉临时表
        System.out.println("Input tables = " + lep.getInputTableSet());
        System.out.println("Output tables = " + lep.getOutputTableSet());
    }
}
