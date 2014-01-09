/*
 * BoltTimeUsr
 * 
 * 1.0 记录每段时间上的usr分布
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


public class BoltTimeUsr extends BaseBasicBolt {
	
	static public Logger LOG = Logger.getLogger(BoltTimeUsr.class);
	private Map _conf;
	private Jedis[] _arrRedisTimeUsr;
	private int _count = 0;

	private void init(Map conf) {
		try {
			List<String> hosts = (List<String>)conf.get("time_usr_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			
			_arrRedisTimeUsr = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisTimeUsr[i] = new Jedis(hosts.get(i), port);
			}
		} catch (Exception e) {
			LOG.info("BoltTimeUsr.init.exception:", e);
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
	    	Date tmp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time);
	    	String timeStamp = new SimpleDateFormat("yyyy-MM-dd-HH").format(tmp);
	    	String key = "TimeUsr`" + timeStamp;
	    	int h = hash(key, _arrRedisTimeUsr.length);
	    	int seconds = 24 * 3600;
	    	
	    	if (++_count % 1000 == 0) {
	    		LOG.info(String.format("BoltTimeUsr %d: time=%s, usr=%s, key=%s", 
	    				_count, time, usr, key));
	    	}
	    	
	    	_arrRedisTimeUsr[h].zincrby(key, 1, usr);
	    	_arrRedisTimeUsr[h].expire(key, seconds);
		} catch (Exception e) {
			LOG.info("BoltTimeUsr.execute.exception:", e);
			init(_conf);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
