package cn.uc.udac.zjj.bolts;

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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class BoltDateUrlPv extends BaseRichBolt {

	static public Logger LOG = Logger.getLogger(BoltDateUrlPv.class);
	private OutputCollector _collector;
	private long _pv_trigger;
	private HTable _t_date_url_pv;
	private long _count = 0;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_pv_trigger = (Long)conf.get("BoltDateUrlPv.pv.trigger");
		Configuration hbconf = HBaseConfiguration.create();
		try {
			_t_date_url_pv = new HTable(hbconf, "t_zjj_date_url_pv");
    	}
    	catch (Exception e) {
    		LOG.info("BoltDateUrlPv.prepare.exception:", e);
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
		try {
			String time = input.getString(0);
			String url = input.getString(4);
			String date = getDate(time);
			String key = date + "/" + url;
			long pv = _t_date_url_pv.incrementColumnValue(key.getBytes(), "m".getBytes(), "pv".getBytes(), 1);
			if (_count % 1000 == 0)
				LOG.info(String.format("BoltDateUrlPv.execute(%d): date=%s, url=%s, pv=%d", _count, date, url, pv));
			if (pv % _pv_trigger == 0)
				_collector.emit(input, new Values(date, url));
		}
		catch (Exception e) {
			LOG.info("BoltDateUrlPv.execute.exception:", e);
		}
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date", "url"));
	}

}
