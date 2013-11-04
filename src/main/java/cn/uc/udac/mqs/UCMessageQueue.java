/*
 * UCMessageQueue 
 * 
 * 1.0 ucmq的java接口
 *
 * zhouxg@ucweb.com 
 */


package cn.uc.udac.mqs;


import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import org.apache.log4j.Logger;


public class UCMessageQueue {

	static public Logger LOG = Logger.getLogger(UCMessageQueue.class);
	private String _host;
	private int _port;
	private String _qname;
	
	public UCMessageQueue(String host, int port, String qname) {
		_host = host;
		_port = port;
		_qname = qname;
	}
	
	public String get() throws IOException {
		String resp = "";
		String line = "";
		String tag = "UCMQ_HTTP_OK";
		URL url = new URL(String.format("http://%s:%d/?name=%s&opt=get&ver=2", _host, _port, _qname));
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		BufferedReader reader;
		
		conn.connect();
		reader = new BufferedReader(new InputStreamReader(conn.getInputStream(),"utf-8"));
		while ((line = reader.readLine()) != null)
			resp += line;
		//LOG.info(String.format("resp = %s", resp));
		reader.close();
        conn.disconnect();
        
        return (resp.indexOf(tag) == 0)? resp.substring(tag.length()).trim() : "";
	}

	public boolean put(String msg) throws IOException {
		String resp = "";
		String line = "";
		String tag = "UCMQ_HTTP_OK";
		String content = "";
		URL url = new URL(String.format("%s:%d/?name=%s&opt=put&ver=2", _host, _port, _qname));
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		DataOutputStream out;
		BufferedReader reader;
		
		conn.setDoOutput(true);
		conn.setDoInput(true);
		conn.setRequestMethod("POST");
		conn.connect();
		out = new DataOutputStream(conn.getOutputStream());
		content = URLEncoder.encode(msg, "utf-8");
		out.writeBytes(content); 
	    out.flush();
	    out.close();
	    
	    reader = new BufferedReader(new InputStreamReader(conn.getInputStream(),"utf-8"));
		while ((line = reader.readLine()) != null)
			resp += line;
		reader.close();
        conn.disconnect();
        
        return resp.indexOf(tag) == 0;
	}

	public boolean reset() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

}