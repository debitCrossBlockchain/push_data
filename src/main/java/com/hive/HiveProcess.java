package com.hive;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.Producer;

import java.sql.DriverManager;
import com.google.gson.Gson;
import com.app.*;
import com.kafak.*;
public class HiveProcess {
	
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	public static void ProccessQuery(String url,String user,String pwd,int start,int end,String kafkaAddress) throws SQLException {
		//url ="jdbc:hive2://10.100.12.30:10000/default"
		//user = "yjy_research"
		//pwd = "research"
		try {
		      Class.forName(driverName);
		    } catch (ClassNotFoundException e) {
		      // TODO Auto-generated catch block
		      e.printStackTrace();
		      System.exit(1);
		    }
		    //replace "hive" here with the name of the user the queries should run as
		    Connection con = DriverManager.getConnection(url, user, pwd);
		    Producer<String, String> producer = com.kafak.KafkaProcess.GetProducer(kafkaAddress);
		    try
		    {
		    Statement stmt = con.createStatement();
		    stmt.execute("set mapreduce.job.queuename=root.yjy");
		    stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict");
		    // show tables
		    // String sql = "show tables '" + tableName + "'";
		    String sql = ("select * from dm_research.risk_phone_libaray_hbase limit %d,%d");
		    sql = String.format(sql, start,end);
		    ResultSet res = stmt.executeQuery(sql);
		    //,"max_ovdue_amount":"41160.0","total_ovdue_amount":"41160.0"}
		    Integer integer = new Integer(App.getPartition());

		    String id = integer.toString();
		    
		    while (res.next()) {
		    	 Gson gson = new Gson();
		    	 Map<String, String> map = new HashMap<String, String>();
		    	 if (res.getString(1)==null) {
		    		 map.put("id", "");
		    		
		    	 }else {
		    		 map.put("id", res.getString(1));
		    	 }
		    	 
		    	 if (res.getString(2)==null) {
		    		 map.put("type", "");
		    		
		    	 }else {
		    		 map.put("type", res.getString(2));
		    	 }
		    	 
		    	 if (res.getString(3)==null) {
		    		 map.put("source", "");
		    		
		    	 }else {
		    		 map.put("source", res.getString(3));
		    	 }
		    	 
		    	 if (res.getString(4)==null) {
		    		 map.put("mark", "");
		    		
		    	 }else {
		    		 map.put("mark", res.getString(4));
		    	 }
		    	 
		    	 if (res.getString(5)==null) {
		    		 map.put("overdue_number", "");
		    		
		    	 }else {
		    		 map.put("overdue_number", res.getString(5));
		    	 }
		    	 
		    	 if (res.getString(6)==null) {
		    		 map.put("first_overdue_time", "");
		    		
		    	 }else {
		    		 map.put("first_overdue_time", res.getString(6));
		    	 }
		    	 
		    	 if (res.getString(7)==null) {
		    		 map.put("last_overdue_time", "");
		    		
		    	 }else {
		    		 map.put("last_overdue_time", res.getString(7));
		    	 }
		    	 
		    	 if (res.getString(8)==null) {
		    		 map.put("max_history_overdue_period", "");
		    		
		    	 }else {
		    		 map.put("max_history_overdue_period", res.getString(8));
		    	 }
		    	 
		    	 if (res.getString(9)==null) {
		    		 map.put("current_isoverdue", "");
		    		
		    	 }else {
		    		 map.put("current_isoverdue", res.getString(9));
		    	 }
		    	 
		    	 if (res.getString(10)==null) {
		    		 map.put("max_overdue_amount", "");
		    		
		    	 }else {
		    		 map.put("max_overdue_amount", res.getString(10));
		    	 }
		    	 
		    	 if (res.getString(11)==null) {
		    		 map.put("total_overdue_amount", "");
		    		
		    	 }else {
		    		 map.put("total_overdue_amount", res.getString(11));
		    	 }
		    	 
		    	 String strInfo = gson.toJson(map);
		    	 KafkaProcess.Send(producer, id, "blockchainid", strInfo);
		    
		      }
		    } catch (Exception e) {
			      // TODO Auto-generated catch block
			      e.printStackTrace();
			    }
		    finally{
		      producer.close();
		      con.close();
		    }	
		    System.out.println("complete page!");
	}
	
	public static int QueryCount(String url,String user,String pwd) throws SQLException {
		int num = 0;
		try {
		      Class.forName(driverName);
		    } catch (ClassNotFoundException e) {
		      // TODO Auto-generated catch block
		      e.printStackTrace();
		      System.exit(1);
		    }
		
		    //replace "hive" here with the name of the user the queries should run as
		    Connection con = DriverManager.getConnection(url, user, pwd);
		  
		    try
		    {
		    Statement stmt = con.createStatement();
		    stmt.execute("set mapreduce.job.queuename=root.yjy");
		    stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict");
		    // show tables
		    // String sql = "show tables '" + tableName + "'";
		    String sql = ("select count(*) from dm_research.risk_phone_libaray_hbase");
		    ResultSet res = stmt.executeQuery(sql);
		    //,"max_ovdue_amount":"41160.0","total_ovdue_amount":"41160.0"}
		   
		    
		    while (res.next()) {
		    	num = res.getInt(1);
		      }
		    } catch (Exception e) {
			      // TODO Auto-generated catch block
			      e.printStackTrace();
			    }
		    finally{
		      con.close();
		    }	
		    return num;
	}
	
}
