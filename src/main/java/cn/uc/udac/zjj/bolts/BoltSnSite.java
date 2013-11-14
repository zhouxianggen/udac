/*
 * BoltSnSite 
 * 
 * 1.0 记录sn访问的site分布
 *
 * zhouxg@ucweb.com 
 */


package cn.uc.udac.zjj.bolts;


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


public class BoltSnSite extends BaseBasicBolt {
	
	static public Logger LOG = Logger.getLogger(BoltSnSite.class);
	private Jedis[] _arrRedisServer;
	private Map _conf;
	private int _count = 0;

	private void init(Map conf) {
		try {
			List<String> hosts = (List<String>)conf.get("sn_site_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			
			LOG.info(String.format("BoltSnsite.init, hosts=%s, port=%d", StringUtils.join(hosts, ","), 
					port));
			
			_arrRedisServer = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisServer[i] = new Jedis(hosts.get(i), port);
			}
		} catch (Exception e) {
			LOG.info("BoltSnsite.init.exception:", e);
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
	    	String sn = input.getString(3);
	    	String url = input.getString(5);
	    	Date tmp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time);
	    	String timeStamp = new SimpleDateFormat("yyyy-MM").format(tmp);
	    	String site = new URL(url).getHost();
	    	String key = "SnSite:" + sn + "`" + timeStamp;
	    	int h = hash(key);
	    	int seconds = 120 * 24 * 3600;
	    	
	    	if (++_count % 100 == 0) {
	    		LOG.info(String.format("BoltSnSite %d: time=%s sn=%s, url=%s", _count, time, sn, url));
	    		LOG.info(String.format("BoltSnSite %d: key=%s h=%d", _count, key, h));
	    	}
	    	
	    	_arrRedisServer[h].zincrby(key, 1, site);
	    	_arrRedisServer[h].expire(key, seconds);
		} catch (Exception e) {
			LOG.info("BoltSnSite.execute.exception:", e);
			init(_conf);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
