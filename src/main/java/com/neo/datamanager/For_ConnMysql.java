package com.neo.datamanager;

import java.sql.*;
/**
 * java 连接mysql公共类
 */

public class For_ConnMysql {
	public static final String driverName = "com.mysql.jdbc.Driver";
	public Connection conn_Mysql(String url,String user,String pass){
		Connection conn = null;
		try {
			Class.forName(driverName);//加载驱动
			conn = DriverManager.getConnection(url,user,pass);//建立连接
		}
		catch (ClassNotFoundException e)
		{
			System.out.println("Sorry,can`t find the Driver!");
			e.printStackTrace();
		}
		catch (SQLException e){
			e.printStackTrace();
		}
		return conn;
	}

	public ResultSet query_Sql(String v_sql,Connection conn){
		ResultSet rs = null;
		try{
			Statement stm = conn.createStatement();//获取句柄，用来执行sql
			rs = stm.executeQuery(v_sql);//执行sql
		}
		catch (SQLException e){
			e.printStackTrace();
		}
		return rs;
	}

	public boolean dml_Sql(String v_sql,Connection conn){
		boolean flag;
		try{
			Statement stm = conn.createStatement();//获取句柄，用来执行sql
			stm.execute(v_sql);
			flag = true;
		}
		catch (SQLException e){
			flag = false;
			System.out.println("sql except");
			e.printStackTrace();
		}
		return flag;
	}

	public static void main(String[] args) {
		System.out.println("fuck you!!");
		String db_name ;
		String col_name ;
		For_ConnMysql fc = new For_ConnMysql();
		Connection conn = fc.conn_Mysql("jdbc:mysql://10.17.2.134:3306/data_dev","root","xiaoniu1234");
		String v_sql = "select t.src_table_name,t.src_column_name from data_dev.src_table_detail t limit 10; ";
		ResultSet rs = fc.query_Sql(v_sql,conn);
		try {
			while (rs.next()) {
				db_name = rs.getString(1);
				col_name = rs.getString(2);
				System.out.println("db:" + db_name + "\t" + "col_name:" + col_name);
			}
			rs.close();//关闭游标
			conn.close();//关闭连接
		}
		catch (SQLException e){
			System.out.println("conn is failed!!");
		}
		System.out.println("hello");
	}
}

