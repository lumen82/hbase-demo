package com.lumen.hbasedemo;

import com.lumen.hbasedemo.dao.BaseDao;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Demo {
    private static String table = "gl_detail";
    private static String cfInfo = "info";
    private static String cfMoney = "money";
    private static String rowKey = "0000000001";

    public static void main(String[] args) throws IOException {
        BaseDao dao = new BaseDao();
        dao.createTable(table, cfInfo, cfMoney);
        dao.insertRow(table, rowKey, getDetail());
        testGetRow(dao);
//        dao.deleteRowByKey(table, rowKey);
//        dao.deleteTable(table);
    }

    public static Map<String, Map<String, String>> getDetail(){
        Map<String, Map<String, String>> detail = new HashMap<String, Map<String, String>>();
        Map<String, String> info = new HashMap<String, String>();
        info.put("accountCode", "1001");
        info.put("accountName", "测试测试");
        detail.put("info", info);

        Map<String, String> money = new HashMap<String, String>();
        money.put("local", "100");
        money.put("group", "100");
        money.put("global", "100");
        detail.put("money", money);
        return detail;
    }

    public static void testGetRow(BaseDao dao) throws IOException {
        Result result = dao.getRowData(table, rowKey);
        String[][] keys = new String[][]{
                {cfInfo, "accountCode"},
                {cfInfo, "accountName"},
                {cfMoney, "local"},
                {cfMoney, "group"},
                {cfMoney, "global"}
        };
        for(String[] key : keys){
            Cell cell = result.getColumnCells(key[0].getBytes(), key[1].getBytes()).get(0);
            System.out.printf("getRowData: cf = %s,  qualifier = %s, value = %s \n" ,
                    Bytes.toString(CellUtil.cloneFamily(cell)),
                    Bytes.toString(CellUtil.cloneQualifier(cell)),
                    Bytes.toString(CellUtil.cloneValue(cell))
            );
        }
    }

    public static void deleteRow(BaseDao dao) throws IOException{
        dao.deleteRowByKey(table, rowKey);
    }
}
