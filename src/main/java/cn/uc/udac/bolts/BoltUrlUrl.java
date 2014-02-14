/*
 * BoltUrlUrl
 * 
 * 1.0 记录url间的转移概率
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


public class BoltUrlUrl extends BaseBasicBolt {
	
	static public Logger LOG = Logger.getLogger(BoltUrlUrl.class);
	private Map _conf;
	private Jedis[] _arrRedisUrlUrl;
	private int _count = 0;

	private void init(Map conf) {
		try {
			List<String> hosts = (List<String>)conf.get("url_url_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			
			_arrRedisUrlUrl = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisUrlUrl[i] = new Jedis(hosts.get(i), port);
			}
		} catch (Exception e) {
			LOG.info("BoltUrlUrl.init.exception:", e);
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
	    	
	    	if (++_count % 1000 == 0) {
	    		LOG.info(String.format("BoltUrlUrl %d: time=%s, url=%s, refer=%s",
	    				_count, time, url, refer));
	    	}
	    	
	    	String key = "UrlUrl`" + refer + "`" + timeStamp;
	    	int h = hash(key, _arrRedisUrlUrl.length);
	    	int seconds = 4 * 24 * 3600;
	    	
    		_arrRedisUrlUrl[h].zincrby(key, 1.0, url);
    		_arrRedisUrlUrl[h].expire(key, seconds);
		} catch (Exception e) {
			//LOG.info("BoltUrlUrl.execute.exception:", e);
			init(_conf);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
