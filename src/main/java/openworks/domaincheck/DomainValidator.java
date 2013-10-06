package openworks.domaincheck;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

public class DomainValidator {

	@Option(name = "-s", aliases = "--source", usage = "Required. Source CSV file containing the data", required = true)
	private String sourceFileName;

	@Option(name = "-d", aliases = "--dest", usage = "Required. Destination CSV to be created after domain validation", required = true)
	private String destFileName;

	@Option(name = "-i", aliases = "--urlIndex", usage = "Required. Index position of the row in CSV that contains URL", required = true)
	private int urlIndex;

	@Option(name = "-v", aliases = "--validRowElemCount", usage = "Optional. Row in the CSV will be ignored if it didn't match this count!")
	private int lengthOfValidRow = -1;

	@Option(name = "-b", aliases = "--batchSize", usage = "Optional. Number of threads per batch. Default:100")
	private int batchSize = 100;

	@Option(name = "-f", aliases = "--includeFailedDomains", usage = "Optional. Should the domains failed from processing to be included as well. Default:true")
	private boolean includeFailedDomains = true;

	@Option(name = "-t", aliases = "--batchTimeout", usage = "Optional. Batch timeout in secs. Default:60s")
	private long batchTimeoutInSecs = 60;

	private CSVReader csvReader;

	private CSVWriter csvWriter;

	private List<String[]> unknownHostsURLList = new ArrayList<String[]>();

	final RequestConfig requestConfig = RequestConfig.custom()
			.setSocketTimeout(50000).setConnectTimeout(20000).build();
	final CloseableHttpAsyncClient httpclient = HttpAsyncClients.custom()
			.setDefaultRequestConfig(requestConfig).build();

	private static final Logger logger = Logger
			.getLogger(DomainValidator.class);

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		DomainValidator validator = new DomainValidator();
		if (!validator.init(args)) {
			System.exit(1);
		}
		validator.process();

		System.exit(0);
	}

	private boolean init(String[] args) {
		CmdLineParser parser = new CmdLineParser(this);
		try {
			parser.parseArgument(args);
			FileReader reader = new FileReader(this.sourceFileName);
			csvReader = new CSVReader(reader);
			FileWriter writer = new FileWriter(this.destFileName);
			csvWriter = new CSVWriter(writer);
			return true;
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			System.err.println();
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			System.err.println();
		} catch (IOException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			System.err.println();
		}
		return false;
	}

	private void process() throws Exception {
		long startTime = System.currentTimeMillis();
		List<String[]> csvData = csvReader.readAll();
		httpclient.start();
		logger.info("Domain validator started!");
		BatchIterator<String[]> iterator = new BatchIterator<String[]>(csvData,
				batchSize);
		batch(iterator, false);

		logger.info("Begin processing the urls ended up in UHE, second time!");
		BatchIterator<String[]> unknownHostIterator = new BatchIterator<String[]>(
				unknownHostsURLList, 5);
		batch(unknownHostIterator, true);
		logger.info("Time to process" + ":"
				+ (System.currentTimeMillis() - startTime) + "ms");
		Thread.sleep(10000);
		httpclient.close();
		csvReader.close();
		csvWriter.flush();
		csvWriter.close();
	}

	/**
	 * @param iterator
	 * @throws IOException
	 */
	private void batch(BatchIterator<String[]> iterator,
			boolean isUnknownHostBatch) throws IOException {
		int batchNum = 1;
		while (iterator.hasNext()) {
			long batchStartTime = System.currentTimeMillis();
			Collection<String[]> currentBatch = iterator.next();
			Map<String, String[]> urlMap = getURLMapFromCSV(currentBatch);
			performRequest(urlMap, !isUnknownHostBatch);
			csvWriter.flush();
			logger.info("Time to process the batch-" + batchNum + ":"
					+ (System.currentTimeMillis() - batchStartTime) + "ms");
			batchNum++;
		}
	}

	private void performRequest(final Map<String, String[]> urlMap,
			final boolean addToUnknownHostsList) {
		final CountDownLatch latch = new CountDownLatch(urlMap.size());
		for (final String url : urlMap.keySet()) {
			try {
				httpclient.execute(new HttpGet(url),
						new FutureCallback<HttpResponse>() {

							public void failed(Exception e) {
								latch.countDown();
								logger.error(url
										+ " failed from being processed", e);
								if (e instanceof UnknownHostException) {
									if (addToUnknownHostsList) {
										unknownHostsURLList.add(urlMap.get(url));
									}
									return;
								}
								if (includeFailedDomains) {
									csvWriter.writeNext(urlMap.get(url));
								}
							}

							public void completed(HttpResponse response) {
								latch.countDown();
								try {
									InputStream stream = response.getEntity()
											.getContent();
									String responseString = new Scanner(stream)
											.useDelimiter("\\A").next();
									if (!isDomainForSale(responseString)) {
										csvWriter.writeNext(urlMap.get(url));
										return;
									}
								} catch (IllegalStateException e) {
									logger.error(
											url
													+ " failed from being processed but included in CSV",
											e);

								} catch (IOException e) {
									logger.error(
											url
													+ " failed from being processed but included in CSV",
											e);

								}
								if (includeFailedDomains) {
									csvWriter.writeNext(urlMap.get(url));
								}
							}

							public void cancelled() {
								latch.countDown();
								csvWriter.writeNext(urlMap.get(url));
							}
						});
			} catch (Exception e) {
				latch.countDown();
				logger.error("Exception while executing request for " + url, e);
			}
		}
		try {
			latch.await(batchTimeoutInSecs, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			logger.error("Error during wait: ", e);

		}

	}

	private boolean isDomainForSale(String html) {
		if (html.toLowerCase().contains("domain for sale")
				|| html.toLowerCase().contains("buy this domain")) {
			return true;
		}
		return false;
	}

	private Map<String, String[]> getURLMapFromCSV(Collection<String[]> csvData) {
		Map<String, String[]> urlMap = new HashMap<String, String[]>();
		Iterator<String[]> iterator = csvData.iterator();
		while (iterator.hasNext()) {
			String[] csvLine = iterator.next();
			String url = csvLine[this.urlIndex];
			if (!isStringEmpty(url) && (csvLine.length >= lengthOfValidRow)) {

				if (!url.startsWith("http")) {
					url = "http://" + url;
				}
				urlMap.put(url, csvLine);
			}
		}
		return urlMap;
	}

	private boolean isStringEmpty(String str) {
		if ((str == null) || str.isEmpty()) {
			return true;
		}
		return false;
	}

}
