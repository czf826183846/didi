package com.didichuxing.nlp.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class listen_mvp_UDAF extends UDAF{
	public static class Evaluator implements UDAFEvaluator{
		
		private HashMap<String, Integer> res;
		private boolean flag;
		private String previous_time;
		private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		private Calendar c1 = Calendar.getInstance();
		private Calendar c2 = Calendar.getInstance();
		
		public Evaluator() {
			// TODO Auto-generated constructor stub
			super();
			init();
		}
		
		@Override
		public void init() {
			// TODO Auto-generated method stub
			res = new HashMap<String, Integer>();
			res.put("24h_in",0);
			res.put("24h_out",0);
			flag = false;
			previous_time = "2010-01-01 00:00:00";
		}
		
		public boolean iterate(String time, String type) throws ParseException {
			if(flag) {
				c1.setTime(formatter.parse(time));
				c2.setTime(formatter.parse(previous_time));
				if((c1.getTimeInMillis() - c2.getTimeInMillis()) < 24 * 1000 * 3600) {
					res.put("24h_in",res.get("24h_in")+1);
				}
				else {
					res.put("24h_out",res.get("24h_out")+1);
				}
			}
			if(type.equals("电话") || type.equals("在线")) {
				flag = true;
				previous_time = time;
			}
			else {
				flag = false;
				previous_time = time;
			}
			return true;
		}
		
		
		
		public HashMap<String, Integer> terminatePartial() {
			if(flag) {
				res.put("24h_out",res.get("24h_out")+1);
				flag = false;
			}
			return res;
		}
		
		public boolean merge(HashMap<String, Integer> other) {
			res.put("24h_in",other.get("24h_in"));
			res.put("24h_out",other.get("24h_out"));
			return true;
		}
		
		public String terminate() {

			return res.toString();
		}
	}
}
