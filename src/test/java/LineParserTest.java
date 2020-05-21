import com.wb.bean.ColLine;
import com.wb.bean.SQLResult;
import com.wb.parse.LineParser;
import junit.framework.TestCase;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class LineParserTest extends TestCase {

    private LineParser parse = null;

    @Override
    protected void setUp() throws Exception {
        parse = new LineParser();
    }

    public void testParseCreateTable() throws Exception {
        Set<String> inputTablesExpected = new HashSet<String>();
        Set<String> outputTablesExpected = new HashSet<String>();
        Set<String> conditions = new HashSet<String>();
        Set<ColLine> lineSetExpected = new HashSet<ColLine>();
        Set<String> outputTablesActual;
        Set<String> inputTablesActual;
        List<ColLine> lineListActualed;
        String sql1 = "create table target_table LOCATION '/data/location' as select a from source_table;" +
                "create table target_table2 LOCATION '/data/location' as select a2 from source_table2;";
        List<SQLResult> srList = parse.parse(sql1);
        inputTablesExpected.add("default.source_table");
        outputTablesExpected.add("default.target_table");
        Set<String> clone1 = clone(conditions);
        Set<String> fromNameSet1 = new LinkedHashSet<String>();
        fromNameSet1.add("default.source_table.a");
        ColLine col1 = new ColLine("a", null, fromNameSet1, clone1, null, null);
        lineSetExpected.add(col1);
        SQLResult sr = srList.get(0);
        outputTablesActual = sr.getOutputTables();
        inputTablesActual = sr.getInputTables();
        lineListActualed = sr.getColLineList();
        assertSetEquals(outputTablesExpected, outputTablesActual);
        assertSetEquals(inputTablesExpected, inputTablesActual);
        assertCoLineSetEqual(lineSetExpected, lineListActualed);

        inputTablesExpected.clear();
        outputTablesExpected.clear();
        lineSetExpected.clear();
        inputTablesExpected.add("default.source_table2");
        outputTablesExpected.add("default.target_table2");
        Set<String> clone2 = clone(conditions);
        Set<String> fromNameSet2 = new LinkedHashSet<String>();
        fromNameSet2.add("default.source_table2.a2");
        ColLine col2 = new ColLine("a2", null, fromNameSet2, clone2, null, null);
        lineSetExpected.add(col2);
        SQLResult sr2 = srList.get(1);
        outputTablesActual = sr2.getOutputTables();
        inputTablesActual = sr2.getInputTables();
        lineListActualed = sr2.getColLineList();
        assertSetEquals(outputTablesExpected, outputTablesActual);
        assertSetEquals(inputTablesExpected, inputTablesActual);
        assertCoLineSetEqual(lineSetExpected, lineListActualed);
    }


    /*
     * 支持解析 select * from table
     */
    public void testParseAllColumn() throws Exception {
        Set<String> inputTablesExpected = new HashSet<String>();
        Set<String> outputTablesExpected = new HashSet<String>();
        Set<String> conditions = new HashSet<String>();
        Set<ColLine> lineSetExpected = new HashSet<ColLine>();
        Set<String> outputTablesActual;
        Set<String> inputTablesActual;
        List<ColLine> lineListActualed;
        String sql1 = "set mapred.job.priority=VERY_HIGH;use app;insert into table dest select statid from " +
                "(select a.*,*,return_benefit_base_foo.* from hand_qq_passenger a join return_benefit_base_foo b on a.statid=b.id where a.channel > 10) base";
        List<SQLResult> srList = parse.parse(sql1);

        inputTablesExpected.add("app.hand_qq_passenger");
        inputTablesExpected.add("app.return_benefit_base_foo");
        outputTablesExpected.add("app.dest");
        conditions.add("WHERE:app.hand_qq_passenger.channel > 10");
        conditions.add("JOIN:app.hand_qq_passenger.statid = app.return_benefit_base_foo.id");

        Set<String> clone1 = clone(conditions);
        Set<String> fromNameSet1 = new LinkedHashSet<String>();
        fromNameSet1.add("app.hand_qq_passenger.statid");
        ColLine col1 = new ColLine("statid", null, fromNameSet1, clone1, null, null);
        lineSetExpected.add(col1);

        SQLResult sr = srList.get(0);
        outputTablesActual = sr.getOutputTables();
        inputTablesActual = sr.getInputTables();
        lineListActualed = sr.getColLineList();
        assertSetEquals(outputTablesExpected, outputTablesActual);
        assertSetEquals(inputTablesExpected, inputTablesActual);
        assertCoLineSetEqual(lineSetExpected, lineListActualed);
        printResult(outputTablesActual, inputTablesActual, lineListActualed);
    }

    /*
     * 支持解析 select * from table
     */
    public void testParseAllColumn2() throws Exception {
        Set<String> inputTablesExpected = new HashSet<String>();
        Set<String> outputTablesExpected = new HashSet<String>();
        Set<String> conditions = new HashSet<String>();
        Set<ColLine> lineSetExpected = new HashSet<ColLine>();
        Set<String> outputTablesActual;
        Set<String> inputTablesActual;
        List<ColLine> lineListActualed;
        String sql1 = "use app;insert into table dest select aaa from " +
                "(select statid as aaa,a.*,* from app.hand_qq_passenger a ) base";
        List<SQLResult> srList = parse.parse(sql1);
        inputTablesExpected.add("app.hand_qq_passenger");
        outputTablesExpected.add("app.dest");

        Set<String> clone1 = clone(conditions);
        Set<String> fromNameSet1 = new LinkedHashSet<String>();
        fromNameSet1.add("app.hand_qq_passenger.statid");
        ColLine col1 = new ColLine("aaa", null, fromNameSet1, clone1, null, null);
        lineSetExpected.add(col1);

        SQLResult sr = srList.get(0);
        outputTablesActual = sr.getOutputTables();
        inputTablesActual = sr.getInputTables();
        lineListActualed = sr.getColLineList();
        assertSetEquals(outputTablesExpected, outputTablesActual);
        assertSetEquals(inputTablesExpected, inputTablesActual);
        assertCoLineSetEqual(lineSetExpected, lineListActualed);
        printResult(outputTablesActual, inputTablesActual, lineListActualed);
    }

    /*
     * 支持解析 where > and in 等
     */
    public void testParseWhere() throws Exception {
        Set<String> inputTablesExpected = new HashSet<String>();
        Set<String> outputTablesExpected = new HashSet<String>();
        Set<String> conditions = new HashSet<String>();
        Set<ColLine> lineSetExpected = new HashSet<ColLine>();
        Set<String> outputTablesActual;
        Set<String> inputTablesActual;
        List<ColLine> lineListConvert;
        String sql1 = "INSERT OVERWRITE table app.dest PARTITION (year='2015',month='10',day='$day') " +
                "select ip,name from test where age > 10 and area in (11,22) or name<>'$V_PARYMD'";
        List<SQLResult> srList = parse.parse(sql1);
        inputTablesExpected.add("default.test");
        outputTablesExpected.add("app.dest");
        conditions.add("WHERE:((default.test.age > 10 and default.test.area in (11,22)) or default.test.name <> '$V_PARYMD')");

        Set<String> clone1 = clone(conditions);
        Set<String> fromNameSet1 = new LinkedHashSet<String>();
        fromNameSet1.add("default.test.ip");
        ColLine col1 = new ColLine("ip", null, fromNameSet1, clone1, null, null);
        Set<String> clone2 = clone(conditions);
        Set<String> fromNameSet2 = new LinkedHashSet<String>();
        fromNameSet2.add("default.test.name");
        ColLine col2 = new ColLine("name", null, fromNameSet2, clone2, null, null);
        lineSetExpected.add(col1);
        lineSetExpected.add(col2);

        SQLResult sr = srList.get(0);
        outputTablesActual = sr.getOutputTables();
        inputTablesActual = sr.getInputTables();
        lineListConvert = sr.getColLineList();
        assertSetEquals(outputTablesExpected, outputTablesActual);
        assertSetEquals(inputTablesExpected, inputTablesActual);
        assertCoLineSetEqual(lineSetExpected, lineListConvert);
        printResult(outputTablesActual, inputTablesActual, lineListConvert);
    }

    /*
     * 支持join
     */
    public void testParseJoin() throws Exception {
        Set<String> inputTablesExpected = new HashSet<String>();
        Set<String> outputTablesExpected = new HashSet<String>();
        Set<String> conditions = new HashSet<String>();
        Set<ColLine> lineSetExpected = new HashSet<ColLine>();
        Set<String> outputTablesActual;
        Set<String> inputTablesActual;
        List<ColLine> lineListActualed;
        String sql = "use app;insert into table dest select nvl(a.name,0) as name_al, b.ip  " +
                "from test a join test1 b on a.ip=b.ip where a.age > 10 and b.area in (11,22) and to_date(b.date) > date_sub('20151001',7)";
        List<SQLResult> srList = parse.parse(sql);
        inputTablesExpected.add("app.test");
        inputTablesExpected.add("app.test1");
        outputTablesExpected.add("app.dest");

        conditions.add("WHERE:((app.test.age > 10 and app.test1.area in (11,22)) and to_date(app.test1.date) > date_sub('20151001',7))");
        conditions.add("JOIN:app.test.ip = app.test1.ip");

        Set<String> clone1 = clone(conditions);
        Set<String> fromNameSet1 = new LinkedHashSet<String>();
        fromNameSet1.add("app.test1.ip");
        ColLine col1 = new ColLine("ip", null, fromNameSet1, clone1, null, null);

        Set<String> clone2 = clone(conditions);
        clone2.add("COLFUN:nvl(app.test.name,0)");
        Set<String> fromNameSet2 = new LinkedHashSet<String>();
        fromNameSet2.add("app.test.name");
        ColLine col2 = new ColLine("name_al", null, fromNameSet2, clone2, null, null);
        lineSetExpected.add(col1);
        lineSetExpected.add(col2);

        SQLResult sr = srList.get(0);
        outputTablesActual = sr.getOutputTables();
        inputTablesActual = sr.getInputTables();
        lineListActualed = sr.getColLineList();
        printResult(outputTablesActual, inputTablesActual, lineListActualed);
        assertSetEquals(outputTablesExpected, outputTablesActual);
        assertSetEquals(inputTablesExpected, inputTablesActual);
        assertCoLineSetEqual(lineSetExpected, lineListActualed);
    }

    /*
     * 支持map,array
     * struct暂时不支持
     */
    public void testParseMap() throws Exception {
        Set<String> inputTablesExpected = new HashSet<String>();
        Set<String> outputTablesExpected = new HashSet<String>();
        Set<String> conditions = new HashSet<String>();
        Set<ColLine> lineSetExpected = new HashSet<ColLine>();
        Set<String> outputTablesActual;
        Set<String> inputTablesActual;
        List<ColLine> lineListActualed;
        String sql = "use dw;insert into table dest select 1+1 as num, params['cid'] as maptest,arr[0] as arrtest,CONCAT(year,month,day) as date " +
                "from test ";
        List<SQLResult> srList = parse.parse(sql);
        inputTablesExpected.add("dw.test");
        outputTablesExpected.add("dw.dest");

        conditions.clear();
        Set<String> clone1 = clone(conditions);
        Set<String> fromNameSet1 = new LinkedHashSet<String>();
        clone1.add("COLFUN:1 + 1");
        ColLine col1 = new ColLine("num", null, fromNameSet1, clone1, null, null);

        Set<String> clone2 = clone(conditions);
        clone2.add("COLFUN:dw.test.params['cid']");
        Set<String> fromNameSet2 = new LinkedHashSet<String>();
        fromNameSet2.add("dw.test.params");
        ColLine col2 = new ColLine("maptest", null, fromNameSet2, clone2, null, null);

        Set<String> clone3 = clone(conditions);
        clone3.add("COLFUN:dw.test.arr[0]");
        Set<String> fromNameSet3 = new LinkedHashSet<String>();
        fromNameSet3.add("dw.test.arr");
        ColLine col3 = new ColLine("arrtest", null, fromNameSet3, clone3, null, null);

        Set<String> clone4 = clone(conditions);
        clone4.add("COLFUN:CONCAT(dw.test.year,dw.test.month,dw.test.day)");
        Set<String> fromNameSet4 = new LinkedHashSet<String>();
        fromNameSet4.add("dw.test.year");
        fromNameSet4.add("dw.test.month");
        fromNameSet4.add("dw.test.day");
        ColLine col4 = new ColLine("date", null, fromNameSet4, clone4, null, null);
        lineSetExpected.add(col1);
        lineSetExpected.add(col2);
        lineSetExpected.add(col3);
        lineSetExpected.add(col4);

        SQLResult sr = srList.get(0);
        outputTablesActual = sr.getOutputTables();
        inputTablesActual = sr.getInputTables();
        lineListActualed = sr.getColLineList();
        assertSetEquals(outputTablesExpected, outputTablesActual);
        assertSetEquals(inputTablesExpected, inputTablesActual);
        assertCoLineSetEqual(lineSetExpected, lineListActualed);
        printResult(outputTablesActual, inputTablesActual, lineListActualed);
    }

    /* *
     * 支持union
     * <p>说明:要求sql具有可读性，没有歧义。如：
     * <p>1、尽量具有相同别名：select 1 as a, b from t1 union select 2,c from t2 =>> select 1 as a, b from t1 union select 2 as a,c as b from t2
     * <p>2、子查询中字段要列出：SELECT u.id, actions.date FROM ( SELECT av.uid AS uid FROM action_video av WHERE av.date = '2010-06-03' UNION ALL SELECT ac.uid AS uid FROM action_comment ac WHERE ac.date = '2008-06-03' ) actions JOIN users u ON (u.id = actions.uid)
     *   =>> SELECT u.id, actions.date FROM ( SELECT av.uid AS uid, av.date as date FROM action_video av WHERE av.date = '2010-06-03' UNION ALL SELECT ac.uid AS uid, ac.date as date FROM action_comment ac WHERE ac.date = '2008-06-03' ) actions JOIN users u ON (u.id = actions.uid)
     * <p>3、不写字段数要一致：select id from t1 union all select id,userName from t2
     * */
    public void testParseUnion() throws Exception {
        Set<String> inputTablesExpected = new HashSet<String>();
        Set<String> outputTablesExpected = new HashSet<String>();
        Set<String> conditions = new HashSet<String>();
        Set<ColLine> lineSetExpected = new HashSet<ColLine>();
        Set<String> outputTablesActual;
        Set<String> inputTablesActual;
        List<ColLine> lineListActualed;
        String sql = "use default;use app;SELECT u.id, actions.date FROM ( " +
                "SELECT av.id AS uid, av.date as date " +
                "FROM action_video av " +
                "WHERE av.date = '2010-06-03' " +
                "UNION ALL " +
                "SELECT ac.uid AS uid, ac.date as date " +
                "FROM fact.action_comment ac " +
                "WHERE ac.date = '2008-06-03' " +
                ") actions JOIN users u ON (u.id = actions.uid)";
        List<SQLResult> srList = parse.parse(sql);
        inputTablesExpected.add("app.users");
        inputTablesExpected.add("app.action_video");
        inputTablesExpected.add("fact.action_comment");
        outputTablesExpected.clear();

        conditions.add("WHERE:app.action_video.date = '2010-06-03'");
        conditions.add("WHERE:fact.action_comment.date = '2008-06-03'");
        conditions.add("JOIN:app.users.id = app.action_video.id&fact.action_comment.uid");

        Set<String> clone1 = clone(conditions);
        Set<String> fromNameSet1 = new LinkedHashSet<String>();
        fromNameSet1.add("app.users.id");
        ColLine col1 = new ColLine("id", null, fromNameSet1, clone1, null, null);
        Set<String> clone2 = clone(conditions);
        Set<String> fromNameSet2 = new LinkedHashSet<String>();
        fromNameSet2.add("app.action_video.date");
        fromNameSet2.add("fact.action_comment.date");
        ColLine col2 = new ColLine("date", null, fromNameSet2, clone2, null, null);
        lineSetExpected.add(col1);
        lineSetExpected.add(col2);

        SQLResult sr = srList.get(0);
        outputTablesActual = sr.getOutputTables();
        inputTablesActual = sr.getInputTables();
        lineListActualed = sr.getColLineList();
        assertSetEquals(outputTablesExpected, outputTablesActual);
        assertSetEquals(inputTablesExpected, inputTablesActual);
        assertCoLineSetEqual(lineSetExpected, lineListActualed);
        printResult(outputTablesActual, inputTablesActual, lineListActualed);
    }

    public void testParseUnion2() throws Exception {
        Set<String> inputTablesExpected = new HashSet<String>();
        Set<String> outputTablesExpected = new HashSet<String>();
        Set<String> conditions = new HashSet<String>();
        Set<ColLine> lineSetExpected = new HashSet<ColLine>();
        Set<String> outputTablesActual;
        Set<String> inputTablesActual;
        List<ColLine> lineListActualed;

        String sql = "INSERT OVERWRITE TABLE target_table " +
                "SELECT name, id, \"Category159\"   FROM source_table_1 " +
                "UNION ALL " +
                "SELECT name, id,category FROM source_table_2 " +
                "UNION ALL " +
                "SELECT name, id, \"Category160\"  FROM source_table_3 where name=123";
        List<SQLResult> srList = parse.parse(sql);
        inputTablesExpected.add("default.source_table_1");
        inputTablesExpected.add("default.source_table_2");
        inputTablesExpected.add("default.source_table_3");
        outputTablesExpected.add("default.target_table");

        conditions.add("WHERE:default.source_table_3.name = 123");

        Set<String> clone1 = clone(conditions);
        Set<String> fromNameSet1 = new LinkedHashSet<String>();
        fromNameSet1.add("default.source_table_1.name");
        fromNameSet1.add("default.source_table_2.name");
        fromNameSet1.add("default.source_table_3.name");
        ColLine col1 = new ColLine("name", null, fromNameSet1, clone1, null, null);
        Set<String> clone2 = clone(conditions);
        Set<String> fromNameSet2 = new LinkedHashSet<String>();
        fromNameSet2.add("default.source_table_1.id");
        fromNameSet2.add("default.source_table_2.id");
        fromNameSet2.add("default.source_table_3.id");
        ColLine col2 = new ColLine("id", null, fromNameSet2, clone2, null, null);
        Set<String> clone3 = clone(conditions);
        clone3.add("COLFUN:\"Category159\"&default.source_table_2.category&\"Category160\"");
        Set<String> fromNameSet3 = new LinkedHashSet<String>();
        fromNameSet3.add("default.source_table_2.category");
        ColLine col3 = new ColLine("category", null, fromNameSet3, clone3, null, null);
        lineSetExpected.add(col1);
        lineSetExpected.add(col2);
        lineSetExpected.add(col3);

        SQLResult sr = srList.get(0);
        outputTablesActual = sr.getOutputTables();
        inputTablesActual = sr.getInputTables();
        lineListActualed = sr.getColLineList();
        assertSetEquals(outputTablesExpected, outputTablesActual);
        assertSetEquals(inputTablesExpected, inputTablesActual);
        assertCoLineSetEqual(lineSetExpected, lineListActualed);
        printResult(outputTablesActual, inputTablesActual, lineListActualed);
    }


    /**
     * 支持解析
     * <p>=,<>,>=,<=,>,<
     * <p>join,where,case when then else end,+,-,*,/,concat,nvl
     * <p>is null, is not null
     * <p>sum,count,max,min,avg,distinct
     * <p>or,and
     * <p> to_date(last_sucgrabord_time ) > date_sub('$data_desc',7)
     *
     * @throws Exception
     */
    public void testParse() throws Exception {
        Set<String> inputTablesExpected = new HashSet<String>();
        Set<String> outputTablesExpected = new HashSet<String>();
        Set<String> conditions = new HashSet<String>();
        Set<ColLine> lineSetExpected = new HashSet<ColLine>();
        Set<String> outputTablesActual;
        Set<String> inputTablesActual;
        List<ColLine> lineListActualed;

        String sql25 = "from(select p.datekey datekey, p.userid	userid, c.clienttype " +
                "from detail.usersequence_client c join fact.orderpayment p on (p.orderid > c.orderid or p.a = c.b) and p.aaa=c.bbb " +
                "full outer join dim.user du on du.userid = p.userid where p.datekey = '20131118' and (du.userid in (111,222) or hash(p.test) like '%123%')) base " +

                "insert overwrite table test.customer_kpi select concat(base.datekey,1,2) as aaa, " +
                "case when base.userid > 5 then base.clienttype when base.userid > 1 then base.datekey+5 else 1-base.clienttype end bbbaaa,count(distinct hash(base.userid)) buyer_count " +
                "where base.userid is not null group by base.datekey, base.clienttype";
        List<SQLResult> srList = parse.parse(sql25);
        inputTablesExpected.add("detail.usersequence_client");
        inputTablesExpected.add("fact.orderpayment");
        inputTablesExpected.add("dim.user");
        outputTablesExpected.add("test.customer_kpi");

        conditions.add("JOIN:((fact.orderpayment.orderid > detail.usersequence_client.orderid or fact.orderpayment.a = detail.usersequence_client.b) and fact.orderpayment.aaa = detail.usersequence_client.bbb)");
        conditions.add("WHERE:(fact.orderpayment.datekey = '20131118' and (dim.user.userid in (111,222) or hash(fact.orderpayment.test) like '%123%'))");
        conditions.add("FULLOUTERJOIN:dim.user.userid = fact.orderpayment.userid");
        conditions.add("WHERE:fact.orderpayment.userid isnotnull");

        Set<String> clone1 = clone(conditions);
        clone1.add("COLFUN:concat(fact.orderpayment.datekey,1,2)");
        Set<String> fromNameSet1 = new LinkedHashSet<String>();
        fromNameSet1.add("fact.orderpayment.datekey");
        ColLine col1 = new ColLine("aaa", null, fromNameSet1, clone1, null, null);
        Set<String> clone2 = clone(conditions);
        clone2.add("COLFUN:(case when fact.orderpayment.userid > 5 then detail.usersequence_client.clienttype when fact.orderpayment.userid > 1 then fact.orderpayment.datekey + 5 else 1 - detail.usersequence_client.clienttype end)");
        Set<String> fromNameSet2 = new LinkedHashSet<String>();
        fromNameSet2.add("fact.orderpayment.userid");
        fromNameSet2.add("fact.orderpayment.datekey");
        fromNameSet2.add("detail.usersequence_client.clienttype");
        ColLine col2 = new ColLine("bbbaaa", null, fromNameSet2, clone2, null, null);
        Set<String> clone3 = clone(conditions);
        clone3.add("COLFUN:count(distinct (hash(fact.orderpayment.userid)))");
        Set<String> fromNameSet3 = new LinkedHashSet<String>();
        fromNameSet3.add("fact.orderpayment.userid");
        ColLine col3 = new ColLine("buyer_count", null, fromNameSet3, clone3, null, null);
        lineSetExpected.add(col1);
        lineSetExpected.add(col2);
        lineSetExpected.add(col3);

        SQLResult sr = srList.get(0);
        outputTablesActual = sr.getOutputTables();
        inputTablesActual = sr.getInputTables();
        lineListActualed = sr.getColLineList();
        assertSetEquals(outputTablesExpected, outputTablesActual);
        assertSetEquals(inputTablesExpected, inputTablesActual);
        assertCoLineSetEqual(lineSetExpected, lineListActualed);
        printResult(outputTablesActual, inputTablesActual, lineListActualed);
    }

    /**
     * 支持方法的循环嵌套查询
     *
     * @throws Exception
     */
    public void testParseFun() throws Exception {
        Set<String> inputTablesExpected = new HashSet<String>();
        Set<String> outputTablesExpected = new HashSet<String>();
        Set<String> conditions = new HashSet<String>();
        Set<ColLine> lineSetExpected = new HashSet<ColLine>();
        Set<String> outputTablesActual;
        Set<String> inputTablesActual;
        List<ColLine> lineListActualed;


        String sql26 = "insert overwrite table test.kd_st_kpi_dri_active_day_city_bi" +
                " select base.city_name,concat(base.last1,'aaa') as aaa,concat(last7,'bbb') as bbb from " +
                " (select " +
                "  b.city_name" +
                " ,b.city_id" +
                " ,nvl(last1_dri_cnt,0) as last1" +
                " ,nvl(last7_dri_cnt,0) as last7" +
                " ,b.pt" +
                " from " +
                " ( " +
                " SELECT" +
                "     city_name ," +
                "     city_id," +
                " 	pt" +
                " FROM" +
                "     dim_city" +
                " WHERE" +
                "     pt='$yesday' AND level=2" +
                " GROUP BY" +
                "     city_name," +
                "     city_id," +
                " 	pt" +
                " ) b" +
                " left outer join " +
                " (" +
                " 	select  " +
                " city_id " +
                " ,pt" +
                " ,count( distinct  case when  to_date(last_sucgrabord_time ) ='$data_desc' then  dri_id end )     last1_dri_cnt  " +
                " ,count( distinct  case when  to_date(last_sucgrabord_time ) >date_sub('$data_desc',7) and   to_date(last_sucgrabord_time) <='$data_desc' then  dri_id end ) last7_dri_cnt" +
                " from dw_dri_wide_sheet " +
                " where pt='$data_desc'" +
                " and last_sucgrabord_time is not null " +
                " group by city_id,pt " +
                " ) a  on a.city_id =  b.city_id) base";
        List<SQLResult> srList = parse.parse(sql26);
        inputTablesExpected.add("default.dw_dri_wide_sheet");
        inputTablesExpected.add("default.dim_city");
        outputTablesExpected.add("test.kd_st_kpi_dri_active_day_city_bi");

        conditions.add("LEFTOUTERJOIN:default.dw_dri_wide_sheet.city_id = default.dim_city.city_id");
        conditions.add("WHERE:(default.dim_city.pt = '$yesday' AND default.dim_city.level = 2)");
        conditions.add("WHERE:(default.dw_dri_wide_sheet.pt = '$data_desc' and default.dw_dri_wide_sheet.last_sucgrabord_time isnotnull)");
        Set<String> clone1 = clone(conditions);
        Set<String> fromNameSet1 = new LinkedHashSet<String>();
        fromNameSet1.add("default.dim_city.city_name");
        ColLine col1 = new ColLine("city_name", null, fromNameSet1, clone1, null, null);
        Set<String> clone2 = clone(conditions);
        clone2.add("COLFUN:concat(nvl(count(distinct ((case when to_date(default.dw_dri_wide_sheet.last_sucgrabord_time) = '$data_desc' else default.dw_dri_wide_sheet.dri_id end))),0),'aaa')");
        Set<String> fromNameSet2 = new LinkedHashSet<String>();
        fromNameSet2.add("default.dw_dri_wide_sheet.last_sucgrabord_time");
        fromNameSet2.add("default.dw_dri_wide_sheet.dri_id");
        ColLine col2 = new ColLine("aaa", null, fromNameSet2, clone2, null, null);
        Set<String> clone3 = clone(conditions);
        clone3.add("COLFUN:concat(nvl(count(distinct ((case when (to_date(default.dw_dri_wide_sheet.last_sucgrabord_time) > date_sub('$data_desc',7) and to_date(default.dw_dri_wide_sheet.last_sucgrabord_time) <= '$data_desc') else default.dw_dri_wide_sheet.dri_id end))),0),'bbb')");
        Set<String> fromNameSet3 = new LinkedHashSet<String>();
        fromNameSet3.add("default.dw_dri_wide_sheet.last_sucgrabord_time");
        fromNameSet3.add("default.dw_dri_wide_sheet.dri_id");
        ColLine col3 = new ColLine("bbb", null, fromNameSet3, clone3, null, null);
        lineSetExpected.add(col1);
        lineSetExpected.add(col2);
        lineSetExpected.add(col3);

        SQLResult sr = srList.get(0);
        outputTablesActual = sr.getOutputTables();
        inputTablesActual = sr.getInputTables();
        lineListActualed = sr.getColLineList();
        assertSetEquals(outputTablesExpected, outputTablesActual);
        assertSetEquals(inputTablesExpected, inputTablesActual);
        assertCoLineSetEqual(lineSetExpected, lineListActualed);
        printResult(outputTablesActual, inputTablesActual, lineListActualed);
    }

    public void testParseFun2() throws Exception {
        Set<String> outputTablesActual;
        Set<String> inputTablesActual;
        List<ColLine> lineListActualed;

        String sql = "INSERT OVERWRITE TABLE APP.WX_PASSENGER_DETAIL PARTITION(YEAR='$V_PARYEAR',MONTH='$V_PARMONTH',DAY='$V_PARDAY2') " +
                "SELECT  " +
                "H.PID, " +
                "H.AREA, " +
                "MAX(H.WP_TYPE) WP_TYPE, " +
                "MAX(CASE WHEN C.PID IS NOT NULL AND (NP_TYPE=1 OR IS_USEWXPAY=1) THEN 1 ELSE 0 END) NP_TYPE, " +
                "MAX(H.IS_USEWXPAY) IS_USEWXPAY " +
                "FROM " +
                "( " +
                "SELECT  " +
                "A.PASSENGERID PID, " +
                "B.AREA AREA, " +
                "(CASE WHEN A.CREATE_TIME<'$V_YESTERDAY' AND B.CHANNEL=1200 AND B.SUCC_FLAG = 1 THEN 1 ELSE 0 END) WP_TYPE, " +
                "0 NP_TYPE, " +
                "(CASE WHEN B.CREATETIME <'$V_YESTERDAY' AND B.SUCC_FLAG = 1 THEN 1 ELSE 0 END) IS_USEWXPAY " +
                "FROM " +
                "( " +
                "SELECT ORDERID, " +
                "STATUS, " +
                "CREATE_TIME, " +
                "PASSENGERID " +
                "FROM PDW.WX_DIDI_TRANSACTION WHERE CONCAT(YEAR,MONTH,DAY)>='$V_PAR7DAYS' AND CONCAT(YEAR,MONTH,DAY)<='$V_PARYESTERDAY' " +
                "AND TRANS_TYPE = 1 AND STATUS = 1 " +
                ") A " +
                "JOIN " +
                "(SELECT ORDERID,  " +
                "AREA, " +
                "PASSENGERID, " +
                "CHANNEL, " +
                "CREATETIME, " +
                "(CASE WHEN (STATUS >=1 AND STATUS<=3 OR (STATUS = 11)) THEN 1 ELSE 0 END) SUCC_FLAG " +
                "FROM PDW.DW_ORDER " +
                "WHERE CONCAT(YEAR,MONTH,DAY)>='$V_PAR7DAYS' AND CONCAT(YEAR,MONTH,DAY)<='$V_PARYESTERDAY' " +
                ") B " +
                "ON(A.ORDERID=B.ORDERID) " +
                "UNION ALL " +
                "SELECT  " +
                " PID, " +
                "AREA, " +
                "WP_TYPE, " +
                "NP_TYPE, " +
                "IS_USEWXPAY " +
                "FROM " +
                "APP.WX_PASSENGER_DETAIL " +
                "WHERE CONCAT(YEAR,MONTH,DAY)='$V_PAR3DAYAGO' " +
                ") H " +
                "LEFT OUTER JOIN  " +
                "( " +
                "SELECT PID " +
                "FROM PDW.STAT_PASSENGER " +
                "WHERE CONCAT(YEAR,MONTH,DAY)='$V_PARYESTERDAY' AND APP_VERSION >='2.6' " +
                " ) C " +
                "ON (H.PID=C.PID) " +
                "GROUP BY H.PID,H.AREA";
        List<SQLResult> srList = parse.parse(sql);

        SQLResult sr = srList.get(0);
        outputTablesActual = sr.getOutputTables();
        inputTablesActual = sr.getInputTables();
        lineListActualed = sr.getColLineList();
        printResult(outputTablesActual, inputTablesActual, lineListActualed);
    }

    public void testParseUnion3() throws Exception {
        Set<String> inputTablesExpected = new HashSet<String>();
        Set<String> outputTablesExpected = new HashSet<String>();
        Set<String> conditions = new HashSet<String>();
        Set<ColLine> lineSetExpected = new HashSet<ColLine>();
        Set<String> outputTablesActual;
        Set<String> inputTablesActual;
        List<ColLine> lineListActualed;

        String sql = "INSERT overwrite TABLE dest " +
                "SELECT order_id FROM gulfstream_ods.g_order " +
                "UNION ALL " +
                "SELECT order_id " +
                "FROM (SELECT orderid order_id,passengerid passenger_id FROM pdw.dw_order) f " +
                "JOIN (SELECT pid FROM pdw.passenger) g  " +
                "ON f.passenger_id = g.pid";
        List<SQLResult> srList = parse.parse(sql);
        inputTablesExpected.add("pdw.passenger");
        inputTablesExpected.add("gulfstream_ods.g_order");
        inputTablesExpected.add("pdw.dw_order");
        outputTablesExpected.add("default.dest");

        conditions.add("JOIN:pdw.dw_order.passengerid = pdw.passenger.pid");

        Set<String> clone1 = clone(conditions);
        Set<String> fromNameSet1 = new LinkedHashSet<String>();
        fromNameSet1.add("pdw.dw_order.orderid");
        fromNameSet1.add("gulfstream_ods.g_order.order_id");
        ColLine col1 = new ColLine("order_id", null, fromNameSet1, clone1, null, null);
        lineSetExpected.add(col1);

        SQLResult sr = srList.get(0);
        outputTablesActual = sr.getOutputTables();
        inputTablesActual = sr.getInputTables();
        lineListActualed = sr.getColLineList();
        assertSetEquals(outputTablesExpected, outputTablesActual);
        assertSetEquals(inputTablesExpected, inputTablesActual);
        assertCoLineSetEqual(lineSetExpected, lineListActualed);
        printResult(outputTablesActual, inputTablesActual, lineListActualed);
    }


    public void testParseDuplicationAlia() throws Exception {
        Set<String> inputTablesExpected = new HashSet<String>();
        Set<String> outputTablesExpected = new HashSet<String>();
        Set<String> conditions = new HashSet<String>();
        Set<ColLine> lineSetExpected = new HashSet<ColLine>();
        Set<String> outputTablesActual;
        Set<String> inputTablesActual;
        List<ColLine> lineListActualed;
        //a,b,c别名重复
        String sql = "insert overwrite table beatles_gulf_driver "
                + "select "
                + "c.driver_phone, "
                + "c.driver_id "
                + "from "
                + "( "
                + "		select "
                + "		b.driver_phone, "
                + "		a.driver_id "
                + "		from "
                + "		( "
                + "			select  "
                + " 		driver_id "
                + "			from gulfstream_dw.dw_m_driver_strategy  "
                + "			where concat(year,month,day) between '$V_7DAY_AGO' and '$V_YESTERDAY' and strive_count > 0 "
                + "			group by driver_id "
                + "		)a "
                + "		join "
                + "		( "
                + "			select  "
                + "			driver_id, "
                + "			driver_phone  "
                + " 		from gulfstream_dw.dw_v_driver_base  "
                + "			where concat(year,month,day) = '$V_YESTERDAY' "
                + "		)b "
                + "		on a.driver_id=b.driver_id "
                + ") c "
                + "join "
                + "( "
                + "	select "
                + "    	c.phone as phone "
                + "		from "
                + "		(     "
                + "   		select "
                + "       	distinct b.phone as phone "
                + "  		from "
                + "  		( "
                + "       		select  "
                + "          	distinct user_id  "
                + "      		from beatles_ods.profile  "
                + "      		where concat(year,month,day)='$V_YESTERDAY' and user_type=2 "
                + "  		)a "
                + "  		join "
                + "  		( "
                + "     		select  "
                + "         	phone, "
                + "         	pid  "
                + "     		from pdw.passenger  "
                + "     		where concat(year,month,day)='$V_YESTERDAY' "
                + " 		)b "
                + " 		on a.user_id=b.pid "
                + "		)c "
                + " )d "
                + "on c.driver_phone=d.phone";
        List<SQLResult> srList = parse.parse(sql);
        inputTablesExpected.add("gulfstream_dw.dw_v_driver_base");
        inputTablesExpected.add("beatles_ods.profile");
        inputTablesExpected.add("pdw.passenger");
        inputTablesExpected.add("gulfstream_dw.dw_m_driver_strategy");
        outputTablesExpected.add("default.beatles_gulf_driver");

        conditions.add("JOIN:beatles_ods.profile.user_id = pdw.passenger.pid");
        conditions.add("WHERE:concat(pdw.passenger.year,pdw.passenger.month,pdw.passenger.day) = '$V_YESTERDAY'");
        conditions.add("WHERE:concat(gulfstream_dw.dw_v_driver_base.year,gulfstream_dw.dw_v_driver_base.month,gulfstream_dw.dw_v_driver_base.day) = '$V_YESTERDAY'");
        conditions.add("WHERE:(concat(beatles_ods.profile.year,beatles_ods.profile.month,beatles_ods.profile.day) = '$V_YESTERDAY' and beatles_ods.profile.user_type = 2)");
        conditions.add("WHERE:(concat(gulfstream_dw.dw_m_driver_strategy.year,gulfstream_dw.dw_m_driver_strategy.month,gulfstream_dw.dw_m_driver_strategy.day) between '$V_7DAY_AGO' and '$V_YESTERDAY' and gulfstream_dw.dw_m_driver_strategy.strive_count > 0)");
        conditions.add("JOIN:gulfstream_dw.dw_m_driver_strategy.driver_id = gulfstream_dw.dw_v_driver_base.driver_id");
        conditions.add("JOIN:gulfstream_dw.dw_v_driver_base.driver_phone = pdw.passenger.phone");

        Set<String> clone1 = clone(conditions);
        Set<String> fromNameSet1 = new LinkedHashSet<String>();
        fromNameSet1.add("gulfstream_dw.dw_v_driver_base.driver_phone");
        ColLine col1 = new ColLine("driver_phone", null, fromNameSet1, clone1, null, null);

        Set<String> clone2 = clone(conditions);
        Set<String> fromNameSet2 = new LinkedHashSet<String>();
        fromNameSet2.add("gulfstream_dw.dw_m_driver_strategy.driver_id");
        ColLine col2 = new ColLine("driver_id", null, fromNameSet2, clone2, null, null);
        lineSetExpected.add(col1);
        lineSetExpected.add(col2);

        SQLResult sr = srList.get(0);
        outputTablesActual = sr.getOutputTables();
        inputTablesActual = sr.getInputTables();
        lineListActualed = sr.getColLineList();
        assertSetEquals(outputTablesExpected, outputTablesActual);
        assertSetEquals(inputTablesExpected, inputTablesActual);
        assertCoLineSetEqual(lineSetExpected, lineListActualed);
        printResult(outputTablesActual, inputTablesActual, lineListActualed);
    }

    public void testParseJoinUnionFix() throws Exception {
        Set<String> inputTablesExpected = new HashSet<String>();
        Set<String> outputTablesExpected = new HashSet<String>();
        Set<String> conditions = new HashSet<String>();
        Set<ColLine> lineSetExpected = new HashSet<ColLine>();
        Set<String> outputTablesActual;
        Set<String> inputTablesActual;
        List<ColLine> lineListActualed;
        String sql = "use gulfstream_dm;" +
                "insert overwrite table g_target_passenger_river_diversion_new       " +
                "    select                                                          " +
                "        coalesce(type_user,'-999') as t_type_user                   " +
                "    from                                                            " +
                "    (                                                               " +
                "        select                                                      " +
                "            passenger_id,                                           " +
                "            case when c.phone is not null then 0 else 1 end type_user" +
                "        from                                                        " +
                "        (                                                           " +
                "            select                                                  " +
                "                x.passenger_id                                      " +
                "            from gulfstream_ods.g_order x                           " +
                "            union all                                               " +
                "            select                                                  " +
                "                passengerid passenger_id                            " +
                "            from                                                    " +
                "                pdw.dw_order                                        " +
                "        ) t1                                                        " +
                "        left outer join                                             " +
                "        (                                                           " +
                "            select                                                  " +
                "                pid,                                                " +
                "                phone                                               " +
                "            from                                                    " +
                "                pdw.passenger                                       " +
                "        ) b                                                         " +
                "        on (t1.passenger_id = b.pid)                                " +
                "        left outer join                                             " +
                "        (                                                           " +
                "            select                                                  " +
                "                phone                                               " +
                "            from gulfstream_ods.no_target_passenger                 " +
                "        ) c                                                         " +
                "        on (b.phone=c.phone)                                        " +
                "    ) a                                                             " +
                "    left outer join                                                 " +
                "    (                                                               " +
                "        select                                                      " +
                "            param['oid'] oid                                        " +
                "        from                                                        " +
                "            gulfstream_ods.g_guide_statistics                       " +
                "    ) d                                                             " +
                "    on (a.passenger_id = d.oid)                                     " +
                "    left outer join                                                 " +
                "    (                                                               " +
                "            select                                                  " +
                "                oid                                                 " +
                "            from                                                    " +
                "            (                                                       " +
                "                select                                              " +
                "                    param['oid'] oid                                " +
                "                from gulfstream_ods.g_guide_statistics2             " +
                "            ) t2                                                    " +
                "    ) e                                                             " +
                "    on (d.oid = e.oid)                                              " +
                "    left outer join                                                 " +
                "    (                                                               " +
                "        select                                                      " +
                "            passenger_phone                                         " +
                "        from gulfstream_ods.g_order                                 " +
                "        union all                                                   " +
                "        select                                                      " +
                "            passenger_phone                                         " +
                "        from                                                        " +
                "        (                                                           " +
                "            select                                                  " +
                "                passengerid passenger_id                            " +
                "            from                                                    " +
                "                pdw.dw_order                                        " +
                "        ) f                                                         " +
                "        left outer join                                             " +
                "        (                                                           " +
                "            select                                                  " +
                "                passenger.phone as passenger_phone,                 " +
                "                pid                                                 " +
                "            from                                                    " +
                "                pdw.passenger                                       " +
                "        ) g                                                         " +
                "        on f.passenger_id = g.pid                                   " +
                "    ) h                                                             " +
                "    on (e.oid = h.passenger_phone)                                  ";
        List<SQLResult> srList = parse.parse(sql);
        inputTablesExpected.add("gulfstream_ods.g_guide_statistics");
        inputTablesExpected.add("pdw.passenger");
        inputTablesExpected.add("gulfstream_ods.g_order");
        inputTablesExpected.add("gulfstream_ods.no_target_passenger");
        inputTablesExpected.add("gulfstream_ods.g_guide_statistics2");
        inputTablesExpected.add("pdw.dw_order");
        outputTablesExpected.add("gulfstream_dm.g_target_passenger_river_diversion_new");

        conditions.add("COLFUN:coalesce((case when gulfstream_ods.no_target_passenger.phone isnotnull then 0 else 1 end),'-999')");
        conditions.add("LEFTOUTERJOIN:gulfstream_ods.g_order.passenger_id&pdw.dw_order.passengerid = pdw.passenger.pid");
        conditions.add("LEFTOUTERJOIN:gulfstream_ods.g_guide_statistics2.param['oid'] = gulfstream_ods.g_order.passenger_phone&pdw.passenger.phone");
        conditions.add("LEFTOUTERJOIN:gulfstream_ods.g_order.passenger_id&pdw.dw_order.passengerid = gulfstream_ods.g_guide_statistics.param['oid']");
        conditions.add("LEFTOUTERJOIN:pdw.passenger.phone = gulfstream_ods.no_target_passenger.phone");
        conditions.add("LEFTOUTERJOIN:pdw.dw_order.passengerid = pdw.passenger.pid");
        conditions.add("LEFTOUTERJOIN:gulfstream_ods.g_guide_statistics.param['oid'] = gulfstream_ods.g_guide_statistics2.param['oid']");

        Set<String> clone1 = clone(conditions);
        Set<String> fromNameSet1 = new LinkedHashSet<String>();
        fromNameSet1.add("gulfstream_ods.no_target_passenger.phone");
        ColLine col1 = new ColLine("t_type_user", null, fromNameSet1, clone1, null, null);
        lineSetExpected.add(col1);

        SQLResult sr = srList.get(0);
        outputTablesActual = sr.getOutputTables();
        inputTablesActual = sr.getInputTables();
        lineListActualed = sr.getColLineList();
        assertSetEquals(outputTablesExpected, outputTablesActual);
        assertSetEquals(inputTablesExpected, inputTablesActual);
        assertCoLineSetEqual(lineSetExpected, lineListActualed);
        printResult(outputTablesActual, inputTablesActual, lineListActualed);
    }

    private void assertCoLineSetEqual(Set<ColLine> lineSetExpected,
                                      List<ColLine> lineListActualed) {
        assertEquals(lineSetExpected.size(), lineListActualed.size());
        for (ColLine colLine : lineListActualed) {
            int i = 0;
            for (ColLine colLine2 : lineSetExpected) {
                i++;
                if (colLine.getToNameParse().equals(colLine2.getToNameParse())) {
                    assertEquals(colLine2.getFromNameSet(), colLine.getFromNameSet());
                    assertSetEquals(colLine2.getAllConditionSet(), colLine.getAllConditionSet());
                    i = 0;
                    break;
                }
                if (i == lineListActualed.size()) {
                    fail();
                }
            }
        }
    }

    private void assertSetEquals(Set<String> expected, Set<String> actual) {
        assertEquals(expected.size(), actual.size());
        for (String string : expected) {
            assertTrue(actual.contains(string));
        }
    }

    private Set<String> clone(Set<String> set) {
        Set<String> list2 = new HashSet<String>(set.size());
        list2.addAll(set);
        return list2;
    }


    public void testSql() throws Exception {
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

        Set<String> outputTablesActual;
        Set<String> inputTablesActual;
        List<ColLine> lineListConvert;

        List<SQLResult> srList = parse.parse(str5);

        SQLResult sr = srList.get(0);
        outputTablesActual = sr.getOutputTables();
        inputTablesActual = sr.getInputTables();
        lineListConvert = sr.getColLineList();
        printResult(outputTablesActual, inputTablesActual, lineListConvert);
    }


    private void printResult(Set<String> outputTablesActual,
                             Set<String> inputTablesActual, List<ColLine> lineListConvert) {
        System.out.println("inputTable:" + inputTablesActual);
        System.out.println("outputTable:" + outputTablesActual);
        for (ColLine colLine : lineListConvert) {
//            System.out.println("ToTable:" + colLine.getToTableName() + ",ToNameParse:" + colLine.getToNameParse() + ",ToName:" + colLine.getToName() + ",FromName:" + colLine.getFromNameSet() + ",AllCondition:" + colLine.getAllConditionSet());
            System.out.println("targetTable:" + colLine.getToTableName() + ",queryColName:" + colLine.getToNameParse() + ",FromSourceColName:" + colLine.getFromNameSet() + ",AllCondition:" + colLine.getAllConditionSet());

        }

    }
}
