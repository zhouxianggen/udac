package cn.uc.udac.zjj.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;

public class HbaseMain {
	static public Logger LOG = Logger.getLogger(HbaseMain.class);
	static public void main(String[] args){
		Configuration hbconf = HBaseConfiguration.create();
        try {
			HTable _t_sn_date_site_pv = new HTable(hbconf, "t_sn_date_site_pv");
			_t_sn_date_site_pv.incrementColumnValue("temp".getBytes(), "pv".getBytes(), "temp".getBytes(), 1);
		} catch (IOException e) {
			LOG.info("BoltSnDateSitePv.exception = ", e);
		}
	}

}
