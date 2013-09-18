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

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


public class BoltSnDateSitePv extends BaseBasicBolt {

	static public Logger LOG = Logger.getLogger(BoltSnDateSitePv.class);
	private HTable _t_sn_date_site_pv;
	private long _count = 0;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
    	Configuration hbconf = HBaseConfiguration.create();
    	try {
    		_t_sn_date_site_pv = new HTable(hbconf, "t_zjj_sn_date_site_pv");
    	}
    	catch (Exception e) {
    		LOG.info("BoltSnDateSitePv.prepare.exception:", e);
    	}
    }
    
    private String getDate(String s) throws ParseException {
    	Date d = new SimpleDateFormat("yyyy-MM-dd").parse(s);
    	return new SimpleDateFormat("yyyy-MM-dd").format(d);
    }
    
    @Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if (input.size() != 5)
			return;
		try {
			String time = input.getString(0).trim();
			String sn = input.getString(1).trim();
			String url = input.getString(4).trim();
			String date = getDate(time);
			String site = new URL(url).getHost();
			String key = sn + "/" + date;
			long pv = _t_sn_date_site_pv.incrementColumnValue(key.getBytes(), "site".getBytes(), site.getBytes(), 1);
			if (_count++ % 1000 == 0)
				LOG.info(String.format("BoltSnDateSitePv.execute(%d): sn=%s, date=%s, site=%s, pv=%d", _count, sn, date, site, pv));
		}
		catch (Exception e) {
			LOG.info("BoltSnDateSitePv.execute.exception:", e);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
