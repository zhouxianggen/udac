/*
 * BoltSiteSite
 * 
 * 1.0 记录site访问的时间分步
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


public class BoltSiteTime extends BaseBasicBolt {
	
	static public Logger LOG = Logger.getLogger(BoltSiteTime.class);
	private Jedis[] _arrRedisSiteTime;
	private Map _conf;
	private int _count = 0;

	private void init(Map conf) {
		try {
			List<String> hosts = (List<String>)conf.get("site_time_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			
			LOG.info(String.format("BoltSiteTime.init, hosts=%s, port=%d", StringUtils.join(hosts, ","), 
					port));
			
			_arrRedisSiteTime = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisSiteTime[i] = new Jedis(hosts.get(i), port);
			}
		} catch (Exception e) {
			LOG.info("BoltSiteTime.init.exception:", e);
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
	    	Date tmp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time);
	    	String timeStamp = new SimpleDateFormat("yyyy-MM-dd-HH").format(tmp);
	    	String site = new URL(url).getHost();
	    	String key = "SiteTime`" + site;
	    	int h = hash(key, _arrRedisSiteTime.length);
	    	int seconds = 24 * 3600;
	    	
	    	if (++_count % 1000 == 0) {
	    		LOG.info(String.format("BoltSiteTime %d: time=%s, site=%s", _count, time, site));
	    		LOG.info(String.format("BoltSiteTime: key=%s, h=%d", key, h));
	    	}
	    	
	    	_arrRedisSiteTime[h].zincrby(key, 1, timeStamp);
	    	_arrRedisSiteTime[h].expire(key, seconds);
		} catch (Exception e) {
			LOG.info("BoltSiteTime.execute.exception:", e);
			init(_conf);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
