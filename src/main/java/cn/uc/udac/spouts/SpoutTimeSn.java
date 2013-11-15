/*
 * SpoutTimeSn 
 * 
 * 1.0 从redis里面拿sn列表
 *
 * zhouxg@ucweb.com 
 */


package cn.uc.udac.spouts;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class SpoutTimeSn extends BaseRichSpout {
	
	static public Logger LOG = Logger.getLogger(SpoutTimeSn.class);
	private SpoutOutputCollector _collector;
	private Jedis[] _arrRedisTimeSn;
	private Map _conf;

	private void init(Map conf) {
		try {
			List<String> hosts = (List<String>)conf.get("time_sn_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			
			LOG.info(String.format("SpoutTimeSn.init, hosts=%s, port=%d", StringUtils.join(hosts, ","), 
					port));
			
			_arrRedisTimeSn = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisTimeSn[i] = new Jedis(hosts.get(i), port);
			}
		} catch (Exception e) {
			LOG.info("SpoutTimeSn.init.exception:", e);
		}
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
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
	public void nextTuple() {
		try {
			Set<String> snSet = new HashSet<String>();
			Calendar calendar = Calendar.getInstance();
			String startTime = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());
			
			for (int i=0; i<4; i+=1) {
				String timeStamp = new SimpleDateFormat("yyyy-MM-dd-HH").format(calendar.getTime());
				String key = "TimeSn`" + timeStamp;
		    	int h = hash(key, _arrRedisTimeSn.length);
		    	
		    	Set<String> s = _arrRedisTimeSn[h].zrange(key, 0, -1);
		    	LOG.info(String.format("SpoutTimeSn: get %d sn from %s", s.size(), key));
		    	snSet.addAll(s);
		    	
		    	calendar.add(Calendar.HOUR_OF_DAY, -1);
			}
			LOG.info(String.format("SpoutTimeSn: get total %d sn", snSet.size()));
			
			int snIndex = 0;
			for (String sn : snSet) {
				_collector.emit(new Values(startTime, snSet.size(), snIndex, sn));
				snIndex += 1;
			}
			
			Thread.sleep(36 * 3600);
		} catch (Exception e) {
			LOG.info("SpoutTimeSn.nextTuple.exception: ", e);
		}
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("time", "snSize", "snIndex", "sn"));
	}

}