package cn.uc.udac.zjj.bolts;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class BoltUrlDatePv extends BaseBasicBolt {

	static public Logger LOG = Logger.getLogger(BoltUrlDatePv.class);
	private int _count = 0;
	private OutputCollector _collector;
	private HTable _t_date_url_pv;
	private long _pv_trigger;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _pv_trigger = (Integer)conf.get("BoltDateUrlPv.pv.trigger");
        Configuration hbconf = HBaseConfiguration.create();
        try {
        	_t_date_url_pv = new HTable(hbconf, "t_date_url_pv");
		} catch (IOException e) {
			LOG.info("BoltDateUrlPv.exception = ", e);
		}
    }
    
	public void execute(Tuple input, BasicOutputCollector collector) {
		if (input.size() != 5)
			return;
		_count += 1;
		LOG.info(String.format("BoltDateUrlPv.count = %d", _count));
		String time = input.getString(0);
		String url = input.getString(4);
		try {
			String date = new SimpleDateFormat("yyyy-MM-dd").format(new SimpleDateFormat("yyyy-MM-dd").parse(time));
			long pv = _t_date_url_pv.incrementColumnValue(url.getBytes(), "pv".getBytes(), date.getBytes(), 1);
			if (pv % _pv_trigger == 0)
				_collector.emit(input, new Values(date, url));
		}
		catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date", "url"));
	}

}
