package io.zikzakjack.gcp.commons;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SplunkService {

	public static String HTTP_HEADER_CONTENT_TYPE_KEY = "Content-Type";
	public static String HTTP_HEADER_CONTENT_TYPE_JSON = "application/json";
	public static String HTTP_HEADER_CONTENT_TYPE_URLENCODED = "application/x-www-form-urlencoded";
	public static String HTTP_HEADER_AUTH_KEY = "Authorization";
	public static String HTTP_POST = "POST";
	public static String HTTP_DELETE = "DELETE";
	public static String HTTP_PROXY_HOST = "internet.zikzakjack.com";
	public static int HTTP_PROXY_PORT = 83;
	public static int HTTP_OK = 200;
	public static int HTTP_CREATED = 201;
	public static int HTTP_NOT_FOUND = 404;
	public static String HTTP_RESP_CODE = "respCode";
	public static String HTTP_RESP_MSG = "respMsg";
	public static String HTTP_RESP_BODY = "respBody";
	public static String ATTR_KEY = "_key";

	public static String infoMsg = "[DataOps] [{}] splunk rest call succeeded. _key : {}, responseCode : {}, responseMessage : {}, responseBody : {}";
	public static String errMsg = "[DataOps] [{}] splunk rest call failed. responseCode : {}, Exception : {}, dataIn : {}";
	public static String retryInfoMsg = "[DataOps] [{}] splunk rest call retry succeeded. _key : {}";
	public static String retryErrMsg = "[DataOps] [{}] splunk rest call retry failed. responseCode : {}, Exception : {}, dataIn : {}";
	public static String searchInfoMsg = "[DataOps] splunk search. splunkUrl : {}, splunkQuery : {}";

	public static Map<String, String> postRequestToSplunk(String dataIn, String splunkUrl, String splunkToken,
			String _key, String source) throws Exception {
		Map<String, String> respMap = null;
		URL url = new URL(splunkUrl);
		HttpURLConnection http = (HttpURLConnection) url.openConnection();
		http.setRequestMethod(HTTP_POST);
		http.setDoOutput(true);
		http.setRequestProperty(HTTP_HEADER_CONTENT_TYPE_KEY, HTTP_HEADER_CONTENT_TYPE_JSON);
		http.setRequestProperty(HTTP_HEADER_AUTH_KEY, splunkToken);
		byte[] dataInBytes = dataIn.getBytes(UTF_8);
		OutputStream stream = http.getOutputStream();
		stream.write(dataInBytes);
		int respCode = http.getResponseCode();
		String respMsg = http.getResponseMessage();
		respMap = new HashMap<String, String>();
		respMap.put(ATTR_KEY, _key.trim());
		respMap.put(HTTP_RESP_CODE, String.valueOf(respCode).trim());
		respMap.put(HTTP_RESP_MSG, respMsg.trim());
		BufferedReader br = null;
		StringBuilder response = new StringBuilder();
		if (respCode == HTTP_OK | respCode == HTTP_CREATED) {
			br = new BufferedReader(new InputStreamReader(http.getInputStream()));
			String currentLine;
			while ((currentLine = br.readLine()) != null) {
				response.append(currentLine);
			}
			br.close();
			String respBody = response.toString();
			respMap.put(HTTP_RESP_BODY, respBody.trim());
			log.info(infoMsg, source, _key, respCode, respMsg, respBody);
		} else {
			log.error(errMsg, source, respCode, "[ERROR]", dataIn);
		}
		http.disconnect();
		return respMap;
	}

	public static Map<String, String> postRequestToSplunkWithRetry(String dataIn, String splunkUrl, String splunkToken,
			String _key, String source) throws Exception {
		Map<String, String> respMap = null;
		respMap = postRequestToSplunk(dataIn, splunkUrl, splunkToken, _key, source);
		if (respMap != null && respMap.containsKey(HTTP_RESP_CODE)) {
			int respCode = Integer.parseInt(respMap.get(HTTP_RESP_CODE));
			if (respCode == HTTP_OK | respCode == HTTP_CREATED) {
				return respMap;
			} else if (respCode == HTTP_NOT_FOUND) {
				if (splunkUrl.contains(URLEncoder.encode(_key, UTF_8.toString()))) {
					String splunkUrlWithoutKey = splunkUrl.substring(0, splunkUrl.lastIndexOf("/"));
					respMap = postRequestToSplunk(dataIn, splunkUrlWithoutKey, splunkToken, _key, source);
					if (respMap != null && respMap.containsKey(HTTP_RESP_CODE)) {
						int newRespCode = Integer.parseInt(respMap.get(HTTP_RESP_CODE));
						if (newRespCode == HTTP_OK | newRespCode == HTTP_CREATED) {
							log.info(retryInfoMsg, source, _key);
							return respMap;
						} else {
							log.error(retryErrMsg, source, respCode, "[ERROR]", dataIn);
							throw new RuntimeException("splunk rest call failed...");
						}
					}
				}
			} else {
				log.error(retryErrMsg, source, respCode, "[ERROR]", dataIn);
				throw new RuntimeException("splunk rest call failed...");
			}
		}
		return respMap;
	}

	public static String search(String splunkUrl, String splunkToken, String splunkQuery) throws Exception {
		log.info(searchInfoMsg, splunkUrl, splunkQuery);
		String response = null;
		URL url = new URL(splunkUrl);
		HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
		httpConn.setRequestMethod(HTTP_POST);
		httpConn.setRequestProperty(HTTP_HEADER_CONTENT_TYPE_KEY, HTTP_HEADER_CONTENT_TYPE_URLENCODED);
		httpConn.setRequestProperty(HTTP_HEADER_AUTH_KEY, splunkToken);
		httpConn.setDoOutput(true);
		OutputStreamWriter writer = new OutputStreamWriter(httpConn.getOutputStream());

		writer.write(splunkQuery);
		writer.flush();
		writer.close();
		httpConn.getOutputStream().close();
		InputStream responseStream = httpConn.getResponseCode() / 100 == 2 ? httpConn.getInputStream()
				: httpConn.getErrorStream();
		try (Scanner scanner = new Scanner(responseStream).useDelimiter("\\A")) {
			response = scanner.hasNext() ? scanner.next() : "";
		}
		return response;
	}
}
