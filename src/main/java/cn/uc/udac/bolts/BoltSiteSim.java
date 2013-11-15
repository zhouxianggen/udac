/*
 * BoltSiteSimSiteSn 
 * 
 * 1.0 生成site的sn列表
 *
 * zhouxg@ucweb.com 
 */


package cn.uc.udac.bolts;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


public class BoltSiteSim extends BaseBasicBolt {
	
	static public Logger LOG = Logger.getLogger(BoltSiteSim.class);
	private Jedis[] _arrRedisSnSite;
	private Jedis[] _arrRedisSiteSim;
	private Map _conf;

	private void init(Map conf) {
		try {
			List<String> hosts = (List<String>)conf.get("sn_site_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			
			_arrRedisSnSite = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisSnSite[i] = new Jedis(hosts.get(i), port);
			}
			
			hosts = (List<String>)conf.get("site_sim_redis_hosts");
			
			_arrRedisSiteSim = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisSiteSim[i] = new Jedis(hosts.get(i), port);
			}
		} catch (Exception e) {
			LOG.info("BoltSiteSim.init.exception:", e);
		}
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context) {
		_conf = conf;
		init(_conf);
    }
    
	private int hash(String key, int size) {
		int h = 0;
		
		for (int i = 0; i < key.length(); ++i) {
			h += key.codePointAt(i);
		}
		
		return h % size;
	}
	
    @Override
	public void execute(Tuple input, BasicOutputCollector collector) {
    	try {
    		String time = input.getString(0);
	    	int snSize = input.getInteger(1).intValue();
	    	int snIndex = input.getInteger(2).intValue();
	    	String sn = input.getString(3);
	    	Date date = new SimpleDateFormat("yyyy-MM-dd").parse(time);
	    	Map<String, Double> mapSites = new HashMap<String, Double>();
	    	
	    	for (int i=0; i<2; ++i) {
	    		String timeStamp = new SimpleDateFormat("yyyy-MM").format(date);
	    		String key = "SnSite`" + sn + "`" + timeStamp;
		    	int h = hash(key, _arrRedisSnSite.length);
		    	
		    	Set<redis.clients.jedis.Tuple> s = _arrRedisSnSite[h].zrangeWithScores(key, 0, -1);
		    	LOG.info(String.format("BoltSiteSim: get %d site from %s", s.size(), key));
		    	
		    	for (redis.clients.jedis.Tuple t : s) {
		    		Double pv = t.getScore();
		    		Double add = mapSites.get(key);
		    		if (add != null)
		    			pv += add;
		    		mapSites.put(t.getElement(), pv);
		    	}
		    	
		    	if (date.getMonth() > 1) {
		    		date.setMonth(date.getMonth() - 1);
		    	} else {
		    		date.setYear(date.getYear() - 1);
		    		date.setMonth(12);
		    	}
	    	}
	    	
	    	for (Entry<String, Double> entry: mapSites.entrySet()) {
	    		String key = "SiteSim`" + time + "`" + entry.getKey();
	    		int h = hash(key, _arrRedisSiteSim.length);
	    		int seconds = 4 * 24 * 3600;
	    		
	    		_arrRedisSiteSim[h].zincrby(key, entry.getValue().doubleValue(), 
	    				new Integer(snIndex).toString());
	    		_arrRedisSiteSim[h].expire(key, seconds);
	    	}
	    	
	    	String key = "SiteSim";
	    	int h = hash(key, _arrRedisSiteSim.length);
	    	double process = _arrRedisSiteSim[h].zincrby(key, 1.0/(double)snSize, time);
	    	LOG.info(String.format("BoltSiteSim: process = %f", process));
		} catch (Exception e) {
			LOG.info("BoltSnSite.execute.exception:", e);
			init(_conf);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
