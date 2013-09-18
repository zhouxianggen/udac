package cn.uc.udac.zjj.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import cn.uc.udac.mqs.UCMessageQueue;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SpoutLog extends BaseRichSpout {
	 
	static public Logger LOG = Logger.getLogger(SpoutLog.class);
	private SpoutOutputCollector _collector;
	private UCMessageQueue[] _mqs;
	private long _count = 0;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		List<String> hosts = (List<String>)conf.get("SpoutLog.ucmq.hosts");
		int port = ( (Long)conf.get("SpoutLog.ucmq.port") ).intValue();
		String qname = (String)conf.get("SpoutLog.ucmq.qname");
		_mqs = new UCMessageQueue[hosts.size()];
		for (int i=0; i<hosts.size(); i+=1)
			_mqs[i] = new UCMessageQueue(hosts.get(i), port, qname);
	}

	@Override
	public void nextTuple() {
		for (int i=0; i<_mqs.length; i+=1) {
			try {
				String[] parts = _mqs[i].get().split("`");
				if (parts.length != 5)
					continue;
				if (_count++ % 1000 == 0)
					LOG.info(String.format("SpoutLog.nextTuple(%d): mq.get=%s", _count, Arrays.toString(parts)));
				_collector.emit(new Values(parts));
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