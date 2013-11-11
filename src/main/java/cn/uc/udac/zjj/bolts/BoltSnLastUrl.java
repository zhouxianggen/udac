/*
 * BoltSnLastUrl 
 * 
 * 1.0 记录sn最近访问的一个Url
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
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class BoltSnLastUrl extends BaseBasicBolt {
	
	static public Logger LOG = Logger.getLogger(BoltSnLastUrl.class);
	private Jedis[] _arrRedisServer;
	private Map _conf;
	private int _count = 0;

	private void init(Map conf) {
		try {
			List<String> hosts = (List<String>)conf.get("sn_last_url_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			
			LOG.info(String.format("BoltSnLastUrl.init, hosts=%s, port=%d", StringUtils.join(hosts, ","), 
					port));
			
			_arrRedisServer = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisServer[i] = new Jedis(hosts.get(i), port);
			}
		} catch (Exception e) {
			LOG.info("BoltSnLastUrl.init.exception:", e);
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
	    	String key = sn;
	    	int h = hash(key);
	    	int seconds = 30;
	    	String refer = _arrRedisServer[h].get(key);
	    	
	    	if (++_count % 1 == 0) {
	    		LOG.info(String.format("BoltSnLastUrl %d: time=%s sn=%s, url=%s, refer=%s", 
	    				_count, time, sn, url, refer));
	    		LOG.info(String.format("BoltSnLastUrl %d: key=%s h=%d", _count, key, h));
	    	}
	    	
	    	if (!refer.isEmpty() && refer != url) {
	    		collector.emit(new Values(time, refer, url));
	    	}
	    	_arrRedisServer[h].set(key, url);
	    	_arrRedisServer[h].expire(key, seconds);
		} catch (Exception e) {
			LOG.info("BoltSnLastUrl.execute.exception:", e);
			init(_conf);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("time", "lastUrl", "url"));
	}

}