package cn.uc.udac.zjj.bolts;

import java.net.URL;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BoltCrawler extends BaseRichBolt {

	static public Logger LOG = Logger.getLogger(BoltCrawler.class);
	private int _count = 0;
	private OutputCollector _collector;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
    }
    
	@Override
	public void execute(Tuple input) {
		if (input.size() != 2)
			return;
		if (_count++ % 1000 == 0)
			LOG.info(String.format("BoltCrawler.count = %d", _count));
		String date = input.getString(0);
		String url = input.getString(1);
		try {
			String site = new URL(url).getHost();
			Document doc = Jsoup.connect(url).get();
			_collector.emit(input, new Values(site, date, doc.title()));
		}
		catch (Exception e) {
			LOG.info("BoltCrawler.execute.exception:", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("site", "date", "title"));
	}

}
