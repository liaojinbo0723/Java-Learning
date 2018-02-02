package com.neo.datamanager;

import java.sql.*;
/**
 * java 连接mysql公共类
 */

public class ConnMysql {
	public static final String driverName = "com.mysql.jdbc.Driver";
	public Connection connMysql(String url,String user,String pass){
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

	public ResultSet querySql(String v_sql,Connection conn){
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

	public boolean dmlSql(String v_sql,Connection conn){
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
		String dbname ;
		String colname ;
		ConnMysql fc = new ConnMysql();
		Connection conn = fc.connMysql("jdbc:mysql://10.8.34.67:3306/dm","zxbi_rw","y-1stpp6RkpY");
		String sql = "load data local infile 'result.txt' into table adw.t_user_person_temp fields terminated by '\t'";
		fc.dmlSql(sql,conn);

//		String sql = "select t.src_table_name,t.src_column_name from data_dev.src_table_detail t limit 10; ";

//		ResultSet rs = fc.querySql(sql,conn);
//		try {
//			while (rs.next()) {
//				dbname = rs.getString(1);
//				colname = rs.getString(2);
//				System.out.println("db:" + dbname + "\t" + "col_name:" + colname);
//			}
//			rs.close();//关闭游标
//			conn.close();//关闭连接
//		}
//		catch (SQLException e){
//			System.out.println("conn is failed!!");
//		}
//		System.out.println("hello");
	}
}

