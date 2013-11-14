/*
 * BoltSnSite 
 * 
 * 1.0 记录sn访问的site分布
 *
 * zhouxg@ucweb.com 
 */


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
		Config conf = new Config();

		builder.setSpout("s_log", new SpoutLog(), 16);
		builder.setBolt("b_sn_site", new BoltSnSite(), 4).shuffleGrouping("s_log");
		builder.setBolt("b_site_url", new BoltSiteUrl(), 4).shuffleGrouping("s_log");
		builder.setBolt("b_city_site", new BoltCitySite(), 4).shuffleGrouping("s_log");
		builder.setBolt("b_time_site", new BoltTimeSite(), 4).shuffleGrouping("s_log");
		builder.setBolt("b_imsi_imei", new BoltImsiImei(), 4).shuffleGrouping("s_log");
		builder.setBolt("b_imei_imsi", new BoltImeiImsi(), 4).shuffleGrouping("s_log");
		builder.setBolt("b_sn_last_url", new BoltSnLastUrl(), 4).shuffleGrouping("s_log");
		builder.setBolt("b_site_site", new BoltSiteSite(), 4).shuffleGrouping("b_sn_last_url");
		builder.setBolt("b_url_url", new BoltUrlUrl(), 4).shuffleGrouping("b_sn_last_url");
		
		conf.setNumWorkers(52);
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
