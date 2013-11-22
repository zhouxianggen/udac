/*
 * udac 
 * 
 * 1.0 uc 定向分析系统
 *
 * zhouxg@ucweb.com 
 */


package cn.uc.udac.main;


import java.util.Map;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import cn.uc.udac.bolts.*;
import cn.uc.udac.spouts.*;


public class StormMain {

	static public void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		Config conf = new Config();

		builder.setSpout("s_zjj_log", new SpoutZjjLog(), 20);
		builder.setBolt("b_site_url", new BoltSiteUrl(), 4).shuffleGrouping("s_zjj_log");
		builder.setBolt("b_site_time", new BoltSiteTime(), 4).shuffleGrouping("s_zjj_log");
		builder.setBolt("b_site_city", new BoltSiteCity(), 4).shuffleGrouping("s_zjj_log");
		builder.setBolt("b_sn_site", new BoltSnSite(), 4).shuffleGrouping("s_zjj_log");
		builder.setBolt("b_time_site", new BoltTimeSite(), 4).shuffleGrouping("s_zjj_log");
		builder.setBolt("b_time_sn", new BoltTimeSn(), 4).shuffleGrouping("s_zjj_log");
		
		builder.setBolt("b_sn_last_url", new BoltSnLastUrl(), 4).shuffleGrouping("s_zjj_log");
		builder.setBolt("b_site_site", new BoltSiteSite(), 4).shuffleGrouping("b_sn_last_url");
		
		builder.setSpout("s_time_sn", new SpoutTimeSn(), 1);
		builder.setBolt("b_site_sim", new BoltSiteSim(), 60).shuffleGrouping("s_time_sn");
		
		conf.setNumWorkers(40);
		Map myconf = Utils.findAndReadConfigFile("udac.yaml");
		conf.putAll(myconf);

		try {
			StormSubmitter.submitTopology("udac", conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}	
	}
	
}
