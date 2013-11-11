/*
 * BoltSiteUrl
 * 
 * 1.0 记录site访问的url分布
 *
 * zhouxg@ucweb.com 
 */


package cn.uc.udac.zjj.bolts;


import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
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


public class BoltSiteUrl extends BaseBasicBolt {
	
	static public Logger LOG = Logger.getLogger(BoltSiteUrl.class);
	private Jedis[] _arrRedisServer;
	private Map _conf;
	private int _count = 0;

	private void init(Map conf) {
		try {
			List<String> hosts = (List<String>)conf.get("site_url_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			
			LOG.info(String.format("BoltSiteUrl.init, hosts=%s, port=%d", StringUtils.join(hosts, ","), 
					port));
			
			_arrRedisServer = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisServer[i] = new Jedis(hosts.get(i), port);
			}
		} catch (Exception e) {
			LOG.info("BoltSiteUrl.init.exception:", e);
		}
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context) {
		_conf = conf;
		init(_conf);
    }
    
	private int hash(String key) {
		int h = 0;
		
		for (int i = 0; i < key.length(); ++i) {
			h += key.codePointAt(i);
		}
		
		return h % _arrRedisServer.length;
	}
	
    @Override
	public void execute(Tuple input, BasicOutputCollector collector) {
    	try {
    		String time = input.getString(0);
	    	String url = input.getString(5);
	    	String site = new URL(url).getHost();
	    	String key = url;
	    	int h = hash(key);
	    	int pv = _arrRedisServer[h].incr(key).intValue();
	    	
	    	_arrRedisServer[h].expire(key, 10);
	    	
	    	if (++_count % 100 == 0) {
	    		LOG.info(String.format("BoltSiteUrl %d: time=%s url=%s", _count, time, url));
	    	}
	    	
	    	if (pv > 500) {
	    		Date tmp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time);
		    	tmp.setMinutes(tmp.getMinutes()/15);
		    	String timeStamp = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(tmp);
		    	
		    	key = site + "`" + timeStamp;
		    	h = hash(key);
		    	
		    	_arrRedisServer[h].zadd(key, pv, url);
	    		_arrRedisServer[h].expire(key, 1 * 3600);
	    	}
		} catch (Exception e) {
			LOG.info("BoltSiteUrl.execute.exception:", e);
			init(_conf);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
