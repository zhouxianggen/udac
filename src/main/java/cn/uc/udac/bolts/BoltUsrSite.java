/*
 * BoltUsrSite 
 * 
 * 1.0 记录usr访问的site分布
 *
 * zhouxg@ucweb.com 
 */


package cn.uc.udac.bolts;


import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


public class BoltUsrSite extends BaseBasicBolt {
	
	static public Logger LOG = Logger.getLogger(BoltUsrSite.class);
	private Map _conf;
	private Jedis[] _arrRedisUsrSite;
	private int _count = 0;

	private void init(Map conf) {
		try {
			List<String> hosts = (List<String>)conf.get("usr_site_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			
			_arrRedisUsrSite = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisUsrSite[i] = new Jedis(hosts.get(i), port);
			}
		} catch (Exception e) {
			LOG.info("BoltUsrSite.init.exception:", e);
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
	    	String usr = input.getString(3);
	    	String url = input.getString(5);
	    	Date tmp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time);
	    	String timeStamp = new SimpleDateFormat("yyyy-MM").format(tmp);
	    	String site = new URL(url).getHost();
	    	String key = "UsrSite`" + usr + "`" + timeStamp;
	    	int h = hash(key, _arrRedisUsrSite.length);
	    	int seconds = 120 * 24 * 3600;
	    	
	    	if (++_count % 1000 == 0) {
	    		LOG.info(String.format("BoltUsrSite %d: time=%s, usr=%s, url=%s, key=%s", 
	    				_count, time, usr, url, key));
	    	}
	    	
	    	_arrRedisUsrSite[h].zincrby(key, 1, site);
	    	_arrRedisUsrSite[h].expire(key, seconds);
		} catch (Exception e) {
			LOG.info("BoltUsrSite.execute.exception:", e);
			init(_conf);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
