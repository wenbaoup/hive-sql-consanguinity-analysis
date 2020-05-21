package com.wb.parse;

import com.wb.bean.ColLine;
import com.wb.bean.SQLResult;

import java.util.List;
import java.util.Set;

/**
 * @author wenbao
 * @version 1.0.0
 * @email wenbao@yijiupi.com
 * @Description LineParser 测试类
 * @createTime 15:26 2020/5/21
 */
public class LineParserTest {

    private static String SQLSTRING1 = "insert\n" +
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

    public static void main(String[] args) throws Exception {
        LineParser parse = new LineParser();
        String sql;
        if (1 == args.length) {
            sql = args[0];
        } else {
            sql = SQLSTRING1;
        }
        List<SQLResult> srList = parse.parse(sql);

        SQLResult sr = srList.get(0);
        printResult(sr.getOutputTables(), sr.getInputTables(), sr.getColLineList());
    }


    private static void printResult(Set<String> outputTablesActual,
                                    Set<String> inputTablesActual, List<ColLine> lineListConvert) {
        System.out.println("inputTable:" + inputTablesActual);
        System.out.println("outputTable:" + outputTablesActual);
        for (ColLine colLine : lineListConvert) {
            System.out.println("targetTable:" + colLine.getToTableName() + ",queryColName:" + colLine.getToNameParse() + ",FromSourceColName:" + colLine.getFromNameSet() + ",AllCondition:" + colLine.getAllConditionSet());
        }

    }
}
