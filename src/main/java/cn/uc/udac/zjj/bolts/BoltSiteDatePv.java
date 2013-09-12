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

import cn.uc.udac.mqs.UCMessageQueue;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class BoltSiteDatePv extends BaseRichBolt {

	static public Logger LOG = Logger.getLogger(BoltSiteDatePv.class);
	private int _count = 0;
	private OutputCollector _collector;
	HTable _t_site_date_pv;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		Configuration hbconf = HBaseConfiguration.create();
		try {
			_t_site_date_pv = new HTable(hbconf, "t_zjj_site_date_pv");
    	}
    	catch (Exception e) {
    		LOG.info("BoltSiteDatePv.prepare.exception:", e);
    	}
	}

	private String getDate(String s) throws ParseException {
    	Date d = new SimpleDateFormat("yyyy-MM-dd").parse(s);
    	return new SimpleDateFormat("yyyy-MM-dd").format(d);
    }
	
	@Override
	public void execute(Tuple input) {
		if (input.size() != 5)
			return;
		if (_count++ % 1000 == 0)
			LOG.info(String.format("BoltSiteDatePv.count = %d", _count));
		String time = input.getString(0);
		String url = input.getString(4);
		try {
			String date = getDate(time);
			String site = new URL(url).getHost();
			LOG.info(String.format("date = %s, site = %s", date, site));
			_t_site_date_pv.incrementColumnValue(site.getBytes(), "date".getBytes(), date.getBytes(), 1);
		} catch (Exception e) {
			LOG.info("BoltSiteDatePv.execute.exception:", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
