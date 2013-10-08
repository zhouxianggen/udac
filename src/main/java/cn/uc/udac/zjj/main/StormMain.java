package cn.uc.udac.zjj.main;


import java.util.Map;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import cn.uc.udac.zjj.bolts.*;
import cn.uc.udac.zjj.spouts.*;


public class StormMain {

	static public void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("s_log", new SpoutLog(), 32);
		builder.setBolt("b_sn_date_site_pv", new BoltSnDateSitePv(), 4).shuffleGrouping("s_log");
		builder.setBolt("b_date_site_pv", new BoltDateSitePv(), 4).shuffleGrouping("s_log");
		builder.setBolt("b_date_url_pv", new BoltDateUrlPv(), 16).shuffleGrouping("s_log");
		builder.setBolt("b_crawler", new BoltCrawler(), 64).shuffleGrouping("b_date_url_pv");
		builder.setBolt("b_parser", new BoltParser(), 4).shuffleGrouping("b_crawler");
		
		Config conf = new Config();
		conf.setNumWorkers(20);
		Map myconf = Utils.findAndReadConfigFile("zjj.yaml");
		conf.putAll(myconf);

		try {
			StormSubmitter.submitTopology("zjj", conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}	
	}
	
}
