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
	private OutputCollector _collector;
	private HTable _t_date_site_word_pv;
	private HTable _t_date_word_site_df;
	private String _ner_server;
	private long _count = 0;

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		_ner_server = (String)conf.get("BoltParser.ner.server");
		Configuration hbconf = HBaseConfiguration.create();
		try {
			_t_date_site_word_pv = new HTable(hbconf, "t_zjj_date_site_word_pv");
			_t_date_word_site_df = new HTable(hbconf, "t_zjj_date_word_site_df");
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
		InputStreamReader reader  = new InputStreamReader(conn.getInputStream(),"utf-8");
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
		String key = date + "/" + site;
		try {
			String[] persons = getPersons(title);
			for (int i=0; i<persons.length; i+=1)
				_t_date_site_word_pv.incrementColumnValue(key.getBytes(), "person1".getBytes(), persons[i].getBytes(), 1);
			persons = getPersons(text);
			for (int i=0; i<persons.length; i+=1)
				_t_date_site_word_pv.incrementColumnValue(key.getBytes(), "person2".getBytes(), persons[i].getBytes(), 1);
		}
		catch (Exception e) {
			LOG.info("BoltParser.execute.exception:", e);
		}
		try {
			ArrayList<String> words = getWords(title);
			for (int i=0; i<words.size(); i+=1)
				_t_date_site_word_pv.incrementColumnValue(key.getBytes(), "word1".getBytes(), words.get(i).getBytes(), 1);
			words = getWords(text);
			for (int i=0; i<words.size(); i+=1)
				_t_date_site_word_pv.incrementColumnValue(key.getBytes(), "word2".getBytes(), words.get(i).getBytes(), 1);
		}
		catch (Exception e) {
			LOG.info("BoltParser.execute.exception:", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
