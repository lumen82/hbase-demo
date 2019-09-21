package com.lumen.hbasedemo.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class BaseDao {
    private Connection connection;

    public Connection getConnection() throws IOException {
        if(connection == null){
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "10.6.234.108");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(configuration);
        }
        return connection;
    }

    public boolean isTableExist(String tableName) throws IOException {
            TableName table = TableName.valueOf(tableName);
            return isTableExist(table);
    }

    public boolean isTableExist(TableName tableName) throws IOException{
        Admin admin = getConnection().getAdmin();
        return admin.tableExists(tableName);
    }

    public void createTable(String tableName, String ...cfs) throws  IOException{
        TableName table = TableName.valueOf(tableName);
        if(isTableExist(table)){
            System.out.println("table " + tableName + " exists");
            return;
        }
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(table);
        if(cfs != null && cfs.length > 0){
            for(String cf : cfs){
                ColumnFamilyDescriptor cfdes = getCfDes(cf);
                builder.setColumnFamily(cfdes);
            }
        }
        Connection connection = getConnection();
        connection.getAdmin().createTable(builder.build());
    }

    public void addColumnFaimily(String tableName, String cfName) throws IOException {
        Connection connection = getConnection();
        TableName table = TableName.valueOf(tableName);
        ColumnFamilyDescriptor cfDes = getCfDes(cfName);
        connection.getAdmin().addColumnFamily(table, cfDes);
    }

    public void insertRow(String tableName, String rowKey, String cf, String qualifier, String value) throws IOException {
        Put put = new Put(rowKey.getBytes());
        put.addColumn(cf.getBytes(), qualifier.getBytes(), value.getBytes());
        getConnection().getTable(TableName.valueOf(tableName)).put(put);
    }

    public void insertRow(String tableName, String rowKey, Map<String, Map<String, String>> keyValuesMap) throws IOException {
        Set<String>  cfs = keyValuesMap.keySet();
        Put put = new Put(rowKey.getBytes());
        for(String cf : cfs){
            Map<String, String> valueMap = keyValuesMap.get(cf);
            Set<String> qualifiers = valueMap.keySet();
            for(String qualifier : qualifiers){
                put.addColumn(cf.getBytes(), qualifier.getBytes(), valueMap.get(qualifier).getBytes());
            }
        }
        getTable(tableName).put(put);
    }

    public Result getRowData(String tableName, String rowKey) throws IOException {
        Table table = getTable(tableName);
        Get get = new Get(rowKey.getBytes());
        return table.get(get);
    }

    public Result getRowDataByQualifier(String tableName, String rowKey, String cf, String qualifier) throws IOException {
        Table table = getTable(tableName);
        Get get = new Get(rowKey.getBytes());
        get.addColumn(cf.getBytes(), qualifier.getBytes());
        return table.get(get);
    }

    public void deleteRowByKey(String tableName, String rowKey) throws IOException {
        Table table = getTable(tableName);
        Delete delete = new Delete(rowKey.getBytes());
        table.delete(delete);
    }

    public void deleteTable(String tableName) throws IOException {
        TableName table = TableName.valueOf(tableName);
        Admin admin = getConnection().getAdmin();
        admin.disableTable(table);
        admin.deleteTable(table);
    }

    public Table getTable(String tableName) throws IOException {
        Table table = getConnection().getTable(TableName.valueOf(tableName));
        return table;
    }

    public ColumnFamilyDescriptor getCfDes(String cfName){
        return ColumnFamilyDescriptorBuilder.of(cfName);
    }
}
