/*
 * BoltUrlSim
 * 
 * 1.0 基于用户访问计算url间的相似度
 *
 * zhouxg@ucweb.com 
 */


package cn.uc.udac.bolts;


import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;

import redis.clients.jedis.Jedis;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


public class BoltUrlSim extends BaseBasicBolt {
	
	static public Logger LOG = Logger.getLogger(BoltUrlSim.class);
	private Map _conf;
	private Jedis[] _arrRedisUrlSim;
	private int SIM_LEN = 88;
	private int[][] _permutes;
	private int _count = 0;

	private void init(Map conf) {
		try {
			List<String> hosts = (List<String>)conf.get("url_sim_redis_hosts");
			int port = ( (Long)conf.get("redis_port") ).intValue();
			
			_arrRedisUrlSim = new Jedis[hosts.size()];
			
			for (int i = 0; i < hosts.size(); ++i) {
				_arrRedisUrlSim[i] = new Jedis(hosts.get(i), port);
			}
			
			List<String> ps = (List<String>)conf.get("url_sim_permutes");
			_permutes = new int[ps.size()][];
			for (int i=0; i<ps.size(); i+=1) {
				String[] ns = ps.get(i).split(",");
				_permutes[i] = new int[ns.length];
				for (int j=0; j<ns.length; j+=1) {
					_permutes[i][j] = Integer.parseInt(ns[j]);
				}
			}
			
		} catch (Exception e) {
			LOG.info("BoltUrlSim.init.exception:", e);
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
	
	private List<Integer> str2bit(String str) {
		List<Integer> lst = new ArrayList<Integer>();
		byte[] bytes = str.getBytes();
		for (byte b : bytes) {
			int val = b;
			for (int i = 0; i < 8; i++) {
				lst.add((val & 128) == 0 ? 0 : 1);
				val <<= 1;
			}
		}
		return lst;
	}
	
    @Override
	public void execute(Tuple input, BasicOutputCollector collector) {
    	try {
    		String time = input.getString(0);
    		String usr = input.getString(3);
	    	String url = input.getString(5);
	    	List<Integer> lst = str2bit(usr);
	    	String key = "UrlSim`" + url;
	    	int seconds = 24 * 3600;
	    	int h = hash(key, _arrRedisUrlSim.length);
	    	String value = _arrRedisUrlSim[h].get(key);
	    	String newValue = "";
	    	String[] v = (value != null)? value.split(",") : new String[0];
	    	String[] sim = new String[SIM_LEN];
	    	
	    	for (int i=0; i<SIM_LEN; i+=1) {
	    		int vi = (i < v.length)? Integer.parseInt(v[i]) : 0;
	    		vi += (i < lst.size() && lst.get(i) == 1)? 1 : -1;
	    		newValue += Integer.toString(vi) + ",";
	    		sim[i] = vi >= 0? "1" : "0";
	    	}
	    	
	    	if (++_count % 1000 == 0) {
	    		LOG.info(String.format("BoltUrlSim: url=%s, usr=%s", url, usr));
	    		LOG.info(String.format("BoltUrlSim: bits=%s", Joiner.on("").join(lst)));
	    		LOG.info(String.format("BoltUrlSim: value=%s", (value!=null? value : "null")));
	    		LOG.info(String.format("BoltUrlSim: newValue=%s", newValue));
	    		LOG.info(String.format("BoltUrlSim: sim=%s", Joiner.on("").join(sim)));
	    	}
	    	
	    	_arrRedisUrlSim[h].set(key, newValue);
	    	_arrRedisUrlSim[h].expire(key, seconds);
	    	
	    	/*for (int i=0; i<_permutes.length; i+=1) {
	    		key = "UrlSim`";
	    		for (int j=0; j<_permutes[i].length; j+=1) {
	    			key += sim[_permutes[i][j]]; 
	    		}
	    		h = hash(key, _arrRedisUrlSim.length);
	    		_arrRedisUrlSim[h].sadd(key, url);
	    		_arrRedisUrlSim[h].expire(key, seconds);
	    	}*/
		} catch (Exception e) {
			LOG.info("BoltUrlSim.execute.exception:", e);
			init(_conf);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
