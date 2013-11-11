/*
 * BoltImeiImsi
 * 
 * 1.0 记录imei上的imsi分布
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


public class BoltImeiImsi extends BaseBasicBolt {
	
	static public Logger LOG = Logger.getLogger(BoltImsiImei.class);
	private Jedis[] _arrRedisServer;
	private Map _conf;
	private int _count = 0;

	private void init(Map conf) {
		try {
			List<String> hosts = (List<String>)conf.get("imei_imsi_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			
			LOG.info(String.format("BoltImeiImsi.init, hosts=%s, port=%d", StringUtils.join(hosts, ","), 
					port));
			
			_arrRedisServer = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisServer[i] = new Jedis(hosts.get(i), port);
			}
		} catch (Exception e) {
			LOG.info("BoltImeiImsi.init.exception:", e);
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
	    	String imei = input.getString(1);
	    	String imsi = input.getString(2);
	    	Date tmp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time);
	    	String timeStamp = new SimpleDateFormat("yyyy-MM-dd").format(tmp);
	    	String key = imei + "`" + timeStamp;
	    	int h = hash(key);
	    	int seconds = 4 * 24 * 3600;
	    	
	    	if (++_count % 1000 == 0) {
	    		LOG.info(String.format("BoltImeiImsi %d: time=%s imsi=%s, imei=%s", _count, time, imei, imsi));
	    		LOG.info(String.format("BoltImeiImsi %d: key=%s h=%d", _count, key, h));
	    	}
	    	
	    	_arrRedisServer[h].zincrby(key, 1, imsi);
	    	_arrRedisServer[h].expire(key, seconds);
		} catch (Exception e) {
			LOG.info("BoltImeiImsi.execute.exception:", e);
			init(_conf);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
