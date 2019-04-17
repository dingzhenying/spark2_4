package dc.common.rule;

import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSONArray;


public class RuleCache {
	
	private static ConcurrentHashMap<String, JSONArray> ruleMap=new ConcurrentHashMap<String,JSONArray>();

	private static class LazyHolder {
		private static final RuleCache INSTANCE = new RuleCache();
	}
	private RuleCache(){
		
	}

	public void put(String uri,JSONArray rules){
		ruleMap.put(uri,rules);
	}

	public JSONArray get(String uri){
		return ruleMap.get(uri);
	}

	public static RuleCache getInstance(){
		return LazyHolder.INSTANCE;
	}
}
