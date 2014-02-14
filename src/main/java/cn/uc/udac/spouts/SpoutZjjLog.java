/*
 * SpoutZjjLog 
 * 
 * 1.0 从ucmq里面取中间件日志
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
import cn.uc.udac.mqs.UCMessageQueue;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.List;
import java.util.Map;


public class SpoutZjjLog extends BaseRichSpout {
	
	static public Logger LOG = Logger.getLogger(SpoutZjjLog.class);
	private SpoutOutputCollector _collector;
	private UCMessageQueue[] _arrMq;
	private int _count = 0;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		List<String> hosts = (List<String>)conf.get("ucmq_hosts");
		int port = ( (Long)conf.get("ucmq_port") ).intValue();
		String qname = (String)conf.get("ucmq_name");
		
		_arrMq = new UCMessageQueue[hosts.size()];
		
		for (int i=0; i<hosts.size(); i+=1)
			_arrMq[i] = new UCMessageQueue(hosts.get(i), port, qname);
	}

	@Override
	public void nextTuple() {
		for (int i=0; i<_arrMq.length; i+=1) {
			String msg = "";
			try {
				msg = _arrMq[i].get();
				String[] parts = msg.split("`");
				
				if (parts.length == 6) {
					if (++_count % 1000 == 0) {
						LOG.info(String.format("SpoutZjjLog.next, msg=%s", msg));
					}

					if (parts[5].equals("about:blank"))
						continue;
					
					String[] ss = parts[3].split("-");
					if (ss.length != 3)
						continue;
					parts[3] = ss[1];
					
					_collector.emit(new Values(parts));
				}
			}
			catch (IOException e) {
				//LOG.info("SpoutZjjLog.nextTuple.exception: ", e);
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
		declarer.declare(new Fields("time", "imei", "imsi", "usr", "cp", "url"));
	}

}