/*
 * BoltUrlUrl
 * 
 * 1.0 记录url间的跳转概率
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
	private Jedis[] _arrRedisUrlUrl;
	private Map _conf;
	private int _count = 0;

	private void init(Map conf) {
		try {
			List<String> hosts = (List<String>)conf.get("url_url_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			
			LOG.info(String.format("BoltUrlUrl.init, hosts=%s, port=%d", StringUtils.join(hosts, ","), 
					port));
			
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
	    	String key = "UrlUrl`" + refer + "`" + url;
	    	int h = hash(key, _arrRedisUrlUrl.length);
	    	int pv = _arrRedisUrlUrl[h].incr(key).intValue();
	    	
	    	_arrRedisUrlUrl[h].expire(key, 300);
	    	
	    	if (++_count % 1000 == 0) {
	    		LOG.info(String.format("BoltUrlUrl %d: time=%s, url=%s, refer=%s", _count, time, url, refer));
	    	}
	    	
	    	if (pv > 100) {
	    		Date tmp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time);
		    	tmp.setMinutes(tmp.getMinutes()/30);
		    	String timeStamp = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(tmp);
		    	String siteFrom = new URL(refer).getHost();
		    	String siteTo = new URL(url).getHost();
		    	
		    	key = "UrlUrl`" + refer + "`" + timeStamp;
		    	h = hash(key, _arrRedisUrlUrl.length);
		    	
		    	if (siteFrom.equals(siteTo)) {
		    		LOG.info(String.format("BoltUrlUrl: key=%s, h=%d", key, h));
		    		_arrRedisUrlUrl[h].zadd(key, pv, url);
		    		_arrRedisUrlUrl[h].expire(key, 1 * 3600);
		    	}
	    	}
		} catch (Exception e) {
			LOG.info("BoltUrlUrl.execute.exception:", e);
			init(_conf);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}