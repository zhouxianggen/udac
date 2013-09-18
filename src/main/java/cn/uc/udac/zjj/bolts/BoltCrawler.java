package cn.uc.udac.zjj.bolts;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BoltCrawler extends BaseRichBolt {

	static public Logger LOG = Logger.getLogger(BoltCrawler.class);
	private OutputCollector _collector;
	private HTable _t_url_text;
	private long _count = 0;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		Configuration hbconf = HBaseConfiguration.create();
		try {
			_t_url_text = new HTable(hbconf, "t_zjj_url_text");
    	}
    	catch (Exception e) {
    		LOG.info("BoltCrawler.prepare.exception:", e);
    	}
    }
    
	private String getText(String url) {
		String text = "";
		try {
			Get row = new Get(url.getBytes());
			row.addColumn("m".getBytes(), "text".getBytes());
			Result r = _t_url_text.get(row);
			if (!r.isEmpty())
				text = r.getValue("m".getBytes(), "text".getBytes()).toString();
			else {
				Document doc = Jsoup.connect(url).get();
				Elements ps = doc.getElementsByTag("p");
				for (Element p : ps)
					text += p.text() + "\n";
				if (text.length() > 0) {
					Put puts = new Put(url.getBytes());
					puts.add("m".getBytes(), "text".getBytes(), text.getBytes());
					puts.add("m".getBytes(), "title".getBytes(), doc.title().getBytes());
					_t_url_text.put(puts);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return text;
	}
	
	@Override
	public void execute(Tuple input) {
		if (input.size() != 2)
			return;
		try {
			String date = input.getString(0);
			String url = input.getString(1);
			String site = new URL(url).getHost();
			String text = getText(url);
			if (_count % 1000 == 0)
				LOG.info(String.format("BoltCrawler.execute(%d): date=%s, url=%s, text=%s", _count, date, url, text));
			if (site.length()>0 && date.length()>0 && text.length()>0)
				_collector.emit(input, new Values(site, date, text));
		}
		catch (Exception e) {
			LOG.info("BoltCrawler.execute.exception:", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("site", "date", "text"));
	}

}
