package cn.uc.udac.zjj.bolts;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
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
import backtype.storm.tuple.Tuple;

import org.ansj.domain.Term;
import org.ansj.splitWord.Analysis;
import org.ansj.splitWord.analysis.ToAnalysis;

import com.esotericsoftware.minlog.Log;

public class BoltWordSegment extends BaseBasicBolt {

	static public Logger LOG = Logger.getLogger(BoltWordSegment.class);
	private int _count = 0;
	private OutputCollector _collector;
	private HTable _t_site_date_word;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        Configuration hbconf = HBaseConfiguration.create();
        try {
        	_t_site_date_word = new HTable(hbconf, "t_site_date_word");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
	public void execute(Tuple input, BasicOutputCollector collector) {
		if (input.size() != 3)
			return;
		_count += 1;
		LOG.info(String.format("BoltWordSegment.count = %d", _count));
		String site = input.getString(0);
		String date = input.getString(1);
		String txt = input.getString(2);
		String key = site + "/" + date;
		List parser = ToAnalysis.parse(txt);
		Iterator<String> it = parser.iterator();
		while (it.hasNext())
			try {
				_t_site_date_word.incrementColumnValue(key.getBytes(), "word".getBytes(), it.next().getBytes(), 1);
			} catch (IOException e) {
				LOG.info("BoltWordSegment.exception =", e);
			}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
