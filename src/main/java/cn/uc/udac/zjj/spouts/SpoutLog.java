package cn.uc.udac.zjj.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import cn.uc.udac.mqs.UCMessageQueue;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.*;

public class SpoutLog extends BaseRichSpout {
	 
	static public Logger LOG = Logger.getLogger(SpoutLog.class);
	private int _count = 0;
	private SpoutOutputCollector _collector;
	private UCMessageQueue _ucmq;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		String host = (String)conf.get("SpoutLog.ucmq.host");
		int port = ( (Long)conf.get("SpoutLog.ucmq.port") ).intValue();
		String qname = (String)conf.get("SpoutLog.ucmq.qname");
		_ucmq = new UCMessageQueue(host, port, qname);
	}

	@Override
	public void nextTuple() {
		try {
			String[] parts = _ucmq.get().split("`");
			if (parts.length != 5)
				return;
			if (_count++ % 1000 == 0)
				LOG.info(String.format("mq.get=%s, count=%d", Arrays.toString(parts), _count));
			_collector.emit(new Values(parts));
		}
		catch (IOException e) {
			LOG.info("SpoutLog.nextTuple.exception:", e);
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