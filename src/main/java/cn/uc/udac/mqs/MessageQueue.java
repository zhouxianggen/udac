package cn.uc.udac.mqs;

import java.io.IOException;
import java.net.MalformedURLException;

public interface MessageQueue {

	public String get() throws IOException;

	public boolean put(String msg) throws IOException;

	public boolean reset() throws IOException;

}