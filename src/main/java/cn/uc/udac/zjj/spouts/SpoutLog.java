/*
 * BoltSnSite 
 * 
 * 1.0 记录sn访问的site分布
 *
 * zhouxg@ucweb.com 
 */


package cn.uc.udac.zjj.spouts;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cn.uc.udac.mqs.UCMessageQueue;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


public class SpoutLog extends BaseRichSpout {
	
	static public Logger LOG = Logger.getLogger(SpoutLog.class);
	private SpoutOutputCollector _collector;
	private UCMessageQueue[] _arrMq;
	HashSet<String> _newsSites;
	private int _count = 0;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		List<String> hosts = (List<String>)conf.get("ucmq_hosts");
		int port = ( (Long)conf.get("ucmq_port") ).intValue();
		String qname = (String)conf.get("ucmq_name");
		List<String> sites = (List<String>)conf.get("news_sites");
		_newsSites = new HashSet<String>(sites);
		
		_arrMq = new UCMessageQueue[hosts.size()];
		
		for (int i=0; i<hosts.size(); i+=1)
			_arrMq[i] = new UCMessageQueue(hosts.get(i), port, qname);
	}

	@Override
	public void nextTuple() {
		for (int i=0; i<_arrMq.length; i+=1) {
			try {
				String msg = _arrMq[i].get();
				String[] parts = msg.split("`");
				
				if (++_count % 10000 == 0)
					LOG.info(String.format("SpoutLog.next, msg=%s", msg));
				
				if (parts.length == 5) {
					String url = parts[4];
					String site = new URL(url).getHost();
					
					if (_newsSites.contains(site)) {
						_collector.emit(new Values(parts));
					}
				}	
			}
			catch (IOException e) {
				LOG.info("SpoutLog.nextTuple.exception:", e);
			}
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
		declarer.declare(new Fields("t", "sn", "ip", "imei", "url"));
	}

}