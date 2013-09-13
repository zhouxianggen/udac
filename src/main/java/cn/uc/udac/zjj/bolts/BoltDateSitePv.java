package cn.uc.udac.zjj.bolts;

import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class BoltDateSitePv extends BaseRichBolt {

	static public Logger LOG = Logger.getLogger(BoltDateSitePv.class);
	private int _count = 0;
	private OutputCollector _collector;
	private HTable _t_date_site_pv;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		Configuration hbconf = HBaseConfiguration.create();
		try {
			_t_date_site_pv = new HTable(hbconf, "t_zjj_date_site_pv");
    	}
    	catch (Exception e) {
    		LOG.info("BoltDateSitePv.prepare.exception:", e);
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
			LOG.info(String.format("BoltDateSitePv.count = %d", _count));
		String time = input.getString(0);
		String url = input.getString(4);
		try {
			String date = getDate(time);
			String site = new URL(url).getHost();
			String key = date + "/" + site;
			_t_date_site_pv.incrementColumnValue(key.getBytes(), "m".getBytes(), "pv".getBytes(), 1);
		} catch (Exception e) {
			LOG.info("BoltDateSitePv.execute.exception:", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
