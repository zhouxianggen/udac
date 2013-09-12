package cn.uc.udac.zjj.bolts;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class BoltCrawler extends BaseBasicBolt {

	static public Logger LOG = Logger.getLogger(BoltCrawler.class);
	private int _count = 0;
	private OutputCollector _collector;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }
    
	public void execute(Tuple input, BasicOutputCollector collector) {
		if (input.size() != 2)
			return;
		_count += 1;
		LOG.info(String.format("BoltCrawler.count = %d", _count));
		String date = input.getString(0);
		String url = input.getString(1);
		try {
			String site = new URL(url).getHost();
			Document doc = Jsoup.connect(url).get();
			_collector.emit(input, new Values(site, date, doc.title()));
		}
		catch (Exception e) {
			LOG.info("BoltCrawler.exception = ", e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("site", "date", "title"));
	}

}
