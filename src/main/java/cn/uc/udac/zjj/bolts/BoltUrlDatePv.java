package cn.uc.udac.zjj.bolts;

import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
	Configuration _hbconf;
	private long _pv_trigger;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        _pv_trigger = (Integer)conf.get("BoltUrlDatePv.pv.trigger");
        _hbconf = HBaseConfiguration.create();
    }
    
    private String getDate(String s) throws ParseException {
    	Date d = new SimpleDateFormat("yyyy-MM-dd").parse(s);
    	return new SimpleDateFormat("yyyy-MM-dd").format(d);
    }
    
	public void execute(Tuple input, BasicOutputCollector collector) {
		if (input.size() != 5)
			return;
		if (_count++ % 1000 == 0)
			LOG.info(String.format("BoltUrlDatePv.count = %d", _count));
		String time = input.getString(0);
		String url = input.getString(4);
		try {
			String date = getDate(time);
			HTable t = new HTable(_hbconf, "t_zjj_url_date_pv");
			long pv = t.incrementColumnValue(url.getBytes(), "date".getBytes(), date.getBytes(), 1);
			if (pv % _pv_trigger == 0)
				_collector.emit(input, new Values(date, url));
		}
		catch (Exception e) {
			LOG.info("BoltUrlDatePv.exception = ", e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date", "url"));
	}

}
