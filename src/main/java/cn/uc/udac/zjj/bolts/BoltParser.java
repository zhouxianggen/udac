package cn.uc.udac.zjj.bolts;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
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

import org.ansj.recognition.NatureRecognition;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.ansj.domain.Term;

public class BoltParser extends BaseRichBolt {

	static public Logger LOG = Logger.getLogger(BoltParser.class);
	private HTable _t_site_feature;
	private String _ner_server;
	private long _count = 0;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_ner_server = (String)conf.get("BoltParser.ner.server");
		Configuration hbconf = HBaseConfiguration.create();
		try {
			_t_site_feature = new HTable(hbconf, "t_zjj_site_feature");
    	}
    	catch (Exception e) {
    		LOG.info("BoltParser.prepare.exception:", e);
    	}
	}
    
	private boolean checkWord(String word, String pos) {
		if (word.length() <= 1) return false;
		if (pos.charAt(0) == 'n') return true;
		if (pos.charAt(0) == 'v') return true;
		return false;
	}
	
	private ArrayList<String> getWords(String txt) {
		ArrayList<String> a = new ArrayList<String>();
		List<Term> terms = ToAnalysis.parse(txt);
		new NatureRecognition(terms).recognition();
		for (int i=0; i<terms.size(); i+=1) {
			Term t = (Term)terms.get(i);
			String word = t.getName();
			String pos = t.getNatrue().natureStr;
			if (!checkWord(word, pos)) continue;
			a.add(word);
		}
		return a;
	}
		
	public String post(String target, String data) throws Exception {  
		URL url = new URL(target);
		URLConnection conn = url.openConnection();
		conn.setDoOutput(true);
		OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());
		writer.write(data);
		writer.flush();
		writer.close();
		InputStreamReader reader  = new InputStreamReader(conn.getInputStream(), "utf-8");
		BufferedReader breader = new BufferedReader(reader);
		String content = null;
		String result = null;
		while((content=breader.readLine())!=null){
			result+=content+"\n";
		}
		return result;
	}
	
	private String[] getPersons(String txt) throws Exception {
		String re = post(_ner_server, txt);
		return re.split(",");
	}
	
	@Override
	public void execute(Tuple input) {
		if (input.size() != 4)
			return;
		String site = input.getString(0);
		String date = input.getString(1);
		String title = input.getString(2);
		String text = input.getString(4);
		String key = site;
		try {
			String[] persons = getPersons(title);
			for (int i=0; i<persons.length; i+=1)
				_t_site_feature.incrementColumnValue(key.getBytes(), "person".getBytes(), persons[i].getBytes(), 1);
		}
		catch (Exception e) {
			LOG.info("BoltParser.execute.exception:", e);
		}
		try {
			ArrayList<String> words = getWords(title);
			for (int i=0; i<words.size(); i+=1)
				_t_site_feature.incrementColumnValue(key.getBytes(), "word".getBytes(), words.get(i).getBytes(), 1);
		}
		catch (Exception e) {
			LOG.info("BoltParser.execute.exception:", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
