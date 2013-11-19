/*
 * BoltSnUrl 
 * 
 * 1.0 记录sn访问的url分布
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
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class BoltSnUrl extends BaseBasicBolt {
	
	static public Logger LOG = Logger.getLogger(BoltSnUrl.class);
	private Jedis[] _arrRedisSnUrl;
	private Map _conf;
	private int _count = 0;

	private void init(Map conf) {
		try {
			List<String> hosts = (List<String>)conf.get("sn_url_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			
			LOG.info(String.format("BoltSnUrl.init, hosts=%s, port=%d", StringUtils.join(hosts, ","), 
					port));
			
			_arrRedisSnUrl = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisSnUrl[i] = new Jedis(hosts.get(i), port);
			}
		} catch (Exception e) {
			LOG.info("BoltSnUrl.init.exception:", e);
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
	    	String sn = input.getString(3);
	    	String url = input.getString(5);
	    	Date tmp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time);
	    	String timeStamp = new SimpleDateFormat("yyyy-MM-dd").format(tmp);
	    	String key = "SnUrl`" + sn + "`" + timeStamp;
	    	int h = hash(key, _arrRedisSnUrl.length);
	    	int seconds = 7 * 24 * 3600;
	    	
	    	if (++_count % 1000 == 0) {
	    		LOG.info(String.format("BoltSnUrl %d: time=%s, sn=%s, url=%s", _count, time, sn, url));
	    		LOG.info(String.format("BoltSnUrl: key=%s, h=%d", key, h));
	    	}
	    	
	    	_arrRedisSnUrl[h].zadd(key, 1, url);
	    	_arrRedisSnUrl[h].expire(key, seconds);
		} catch (Exception e) {
			LOG.info("BoltSnUrl.execute.exception:", e);
			init(_conf);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("time", "lastUrl", "url"));
	}

}
