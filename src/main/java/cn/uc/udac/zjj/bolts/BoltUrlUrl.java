/*
 * BoltUrlUrl
 * 
 * 1.0 记录url间的跳转概率
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


public class BoltUrlUrl extends BaseBasicBolt {
	
	static public Logger LOG = Logger.getLogger(BoltSiteUrl.class);
	private Jedis[] _arrRedisServer;
	private int _count = 0;

	@Override
	public void prepare(Map conf, TopologyContext context) {
		try {
			List<String> hosts = (List<String>)conf.get("url_url_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			LOG.info(String.format("BoltUrlUrl.prepare, hosts=%s, port=%d", StringUtils.join(hosts, ","), 
					port));
			
			_arrRedisServer = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisServer[i] = new Jedis(hosts.get(i), port);
			}
		} catch (Exception e) {
			LOG.info("BoltUrlUrl.prepare.exception:", e);
		}
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
	    	String url = input.getString(3);
	    	String refer = input.getString(4);
	    	Date tmp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time);
	    	tmp.setMinutes(tmp.getMinutes()/30);
	    	String timeStamp = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(tmp);
	    	String siteFrom = new URL(refer).getHost();
	    	String siteTo = new URL(url).getHost();
	    	String key = refer + "`" + timeStamp;
	    	int h = hash(key);
	    	int seconds = 2 * 3600;
	    	
	    	if (++_count % 10000 == 0) {
	    		LOG.info(String.format("BoltUrlUrl %d: time=%s url=%s", _count, time, url));
	    		LOG.info(String.format("BoltUrlUrl %d: key=%s h=%d", _count, key, h));
	    	}
	    	
	    	if (siteFrom != siteTo) {
	    		_arrRedisServer[h].zincrby(key, 1, url);
	    		_arrRedisServer[h].expire(key, seconds);
	    	}
		} catch (Exception e) {
			LOG.info("BoltUrlUrl.execute.exception:", e);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}