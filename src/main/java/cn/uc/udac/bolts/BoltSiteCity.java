/*
 * BoltSiteCity
 * 
 * 1.0 记录site访问的城市分步
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


public class BoltSiteCity extends BaseBasicBolt {
	
	static public Logger LOG = Logger.getLogger(BoltSiteCity.class);
	private Jedis[] _arrRedisSiteCity;
	private Map _conf;
	private int _count = 0;

	private void init(Map conf) {
		try {
			List<String> hosts = (List<String>)conf.get("site_time_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			
			LOG.info(String.format("BoltSiteCity.init, hosts=%s, port=%d", StringUtils.join(hosts, ","), 
					port));
			
			_arrRedisSiteCity = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisSiteCity[i] = new Jedis(hosts.get(i), port);
			}
		} catch (Exception e) {
			LOG.info("BoltSiteCity.init.exception:", e);
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
    		String city = input.getString(4);
	    	String url = input.getString(5);
	    	Date tmp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time);
	    	String timeStamp = new SimpleDateFormat("yyyy-MM").format(tmp);
	    	String site = new URL(url).getHost();
	    	String key = "SiteCity`" + site + "`" + timeStamp;
	    	int h = hash(key, _arrRedisSiteCity.length);
	    	int seconds = 120 * 24 * 3600;
	    	
	    	if (++_count % 1000 == 0) {
	    		LOG.info(String.format("BoltSiteCity %d: city=%s, site=%s", _count, city, site));
	    		LOG.info(String.format("BoltSiteCity: key=%s, h=%d", key, h));
	    	}
	    	
	    	_arrRedisSiteCity[h].zincrby(key, 1, city);
	    	_arrRedisSiteCity[h].expire(key, seconds);
		} catch (Exception e) {
			LOG.info("BoltSiteCity.execute.exception:", e);
			init(_conf);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
