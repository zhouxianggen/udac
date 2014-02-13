/*
 * BoltSiteSite
 * 
 * 1.0 记录site间的转移概率
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


public class BoltSiteSite extends BaseBasicBolt {
	
	static public Logger LOG = Logger.getLogger(BoltSiteSite.class);
	private Map _conf;
	private Jedis[] _arrRedisSiteSite;
	private int _count = 0;

	private void init(Map conf) {
		try {
			List<String> hosts = (List<String>)conf.get("site_site_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			
			_arrRedisSiteSite = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisSiteSite[i] = new Jedis(hosts.get(i), port);
			}
		} catch (Exception e) {
			LOG.info("BoltSiteSite.init.exception:", e);
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
	    	String refer = input.getString(1);
	    	String url = input.getString(2);
	    	Date tmp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time);
	    	String timeStamp = new SimpleDateFormat("yyyy-MM-dd").format(tmp);
	    	String siteFrom = new URL(refer).getHost();
	    	String siteTo = new URL(url).getHost();
	    	
	    	if (++_count % 1000 == 0) {
	    		LOG.info(String.format("BoltSiteSite %d: time=%s, url=%s, refer=%s",
	    				_count, time, url, refer));
	    	}
	    	
	    	String key = "SiteSite`from`" + siteFrom + "`" + timeStamp;
	    	int h = hash(key, _arrRedisSiteSite.length);
	    	int seconds = 30 * 24 * 3600;
	    	
    		_arrRedisSiteSite[h].zincrby(key, 1.0, siteTo);
    		_arrRedisSiteSite[h].expire(key, seconds);
    		
    		key = "SiteSite`to`" + siteTo + "`" + timeStamp;
	    	h = hash(key, _arrRedisSiteSite.length);
    		_arrRedisSiteSite[h].zincrby(key, 1.0, siteFrom);
    		_arrRedisSiteSite[h].expire(key, seconds);
		} catch (Exception e) {
			LOG.info("BoltSiteSite.execute.exception:", e);
			init(_conf);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
