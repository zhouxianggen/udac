/*
 * BoltSiteUrl
 * 
 * 1.0 记录site访问的url分布
 *
 * zhouxg@ucweb.com 
 */


package cn.uc.udac.bolts;


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
	private Map _conf;
	private Jedis[] _arrRedisSiteUrl;
	private int _count = 0;

	private void init(Map conf) {
		try {
			List<String> hosts = (List<String>)conf.get("site_url_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			
			_arrRedisSiteUrl = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisSiteUrl[i] = new Jedis(hosts.get(i), port);
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
	    	String url = input.getString(5);
	    	String site = new URL(url).getHost();
	    	String key = "SiteUrl`" + url;
	    	int h = hash(key, _arrRedisSiteUrl.length);
	    	int pv = _arrRedisSiteUrl[h].incr(key).intValue();
	    	
	    	_arrRedisSiteUrl[h].expire(key, 15*60);
	    	
	    	if (++_count % 1000 == 0) {
	    		LOG.info(String.format("BoltSiteUrl %d: time=%s, url=%s, key=%s, pv=%d", 
	    				_count, time, url, key, pv));
	    	}
	    	
	    	if (pv > 100) {
	    		Date tmp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time);
		    	tmp.setMinutes(tmp.getMinutes()/15);
		    	String timeStamp = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(tmp);
		    	
		    	key = "SiteUrl`" + site + "`" + timeStamp;
		    	h = hash(key, _arrRedisSiteUrl.length);
		    	LOG.info(String.format("BoltSiteUrl: %s hot url=%s", site, url));
		    	
		    	_arrRedisSiteUrl[h].zadd(key, pv, url);
		    	_arrRedisSiteUrl[h].expire(key, 24 * 3600);
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
