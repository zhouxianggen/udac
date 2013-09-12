package cn.uc.udac.zjj.bolts;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.log4j.Logger;

import cn.uc.udac.zjj.spouts.SpoutLog;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


public class BoltSnDateSitePv extends BaseBasicBolt {

	static public Logger LOG = Logger.getLogger(BoltSnDateSitePv.class);
	private int _count = 0;
	private OutputCollector _collector;
	private HTable _t_sn_date_site_pv;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        Configuration hbconf = HBaseConfiguration.create();
        try {
			_t_sn_date_site_pv = new HTable(hbconf, "t_sn_date_site_pv");
		} catch (IOException e) {
			LOG.info("BoltSnDateSitePv.exception = ", e);
		}
    }
    
	public void execute(Tuple input, BasicOutputCollector collector) {
		if (input.size() != 5)
			return;
		if (_count++ % 1000 == 0)
			LOG.info(String.format("BoltSnDateSitePv.count = %d", _count));
		String time = input.getString(0);
		String sn = input.getString(1);
		String url = input.getString(4);
		String date = null;
		String site = null;
		try {
			date = new SimpleDateFormat("yyyy-MM-dd").format(new SimpleDateFormat("yyyy-MM-dd").parse(time));
			site = new URL(url).getHost();
			String key = sn + "/" + date;
			Increment increment = new Increment(key.getBytes());
			increment.addColumn("site".getBytes(), site.getBytes(), 1);
			_t_sn_date_site_pv.increment(increment);
		}
		catch (Exception e) {
			LOG.info("BoltSnDateSitePv.exception = ", e);
			return;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
