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
		String time = input.getString(0).trim();
		String sn = input.getString(1).trim();
		String url = input.getString(4).trim();
		try {
			String date = new SimpleDateFormat("yyyy-MM-dd").format(new SimpleDateFormat("yyyy-MM-dd").parse(time));
			String site = new URL(url).getHost();
			String key = sn + "/" + date;
			if (date != null && site != null)
				_t_sn_date_site_pv.incrementColumnValue(key.getBytes(), "site".getBytes(), site.getBytes(), 1);
		}
		catch (Exception e) {
			LOG.info("BoltSnDateSitePv.exception = ", e);
			return;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
