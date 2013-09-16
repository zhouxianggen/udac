package cn.uc.udac.zjj.bolts;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
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
import org.ansj.splitWord.analysis.ToAnalysis;
import org.ansj.domain.Term;

public class BoltParser extends BaseRichBolt {

	static public Logger LOG = Logger.getLogger(BoltParser.class);
	private int _count = 0;
	private OutputCollector _collector;
	private HTable _t_date_site_word_pv;
	private HTable _t_date_word_site_df;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		Configuration hbconf = HBaseConfiguration.create();
		try {
			_t_date_site_word_pv = new HTable(hbconf, "t_zjj_date_site_word_pv");
			_t_date_word_site_df = new HTable(hbconf, "t_zjj_date_word_site_df");
    	}
    	catch (Exception e) {
    		LOG.info("BoltParser.prepare.exception:", e);
    	}
	}
    
	@Override
	public void execute(Tuple input) {
		if (input.size() != 3)
			return;
		if (_count++ % 1000 == 0)
			LOG.info(String.format("BoltParser.count = %d", _count));
		String site = input.getString(0);
		String date = input.getString(1);
		String txt = input.getString(2);
		String key = date + "/" + site;
		List<Term> parser = ToAnalysis.parse(txt);
		Iterator<Term> it = parser.iterator();
		try {
			while (it.hasNext()) {
				String word = it.next().getName();
				if (word.length() <= 1) continue;
				long pv = _t_date_site_word_pv.incrementColumnValue(key.getBytes(), "word".getBytes(), word.getBytes(), 1);
				if (pv == 1)
					_t_date_word_site_df.incrementColumnValue(word.getBytes(), "date".getBytes(), date.getBytes(), 1);
			}
		}
		catch (IOException e) {
			LOG.info("BoltParser.execute.exception:", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
