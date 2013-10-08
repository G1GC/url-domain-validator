package openworks.domaincheck;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import com.ning.http.client.AsyncHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.AsyncHttpClientConfig.Builder;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.HttpResponseHeaders;
import com.ning.http.client.HttpResponseStatus;
import com.ning.http.client.Response;

public class DomainValidator {

	@Option(name = "-s", aliases = "--source", usage = "Required. Source CSV file containing the data", required = true)
	private String sourceFileName;

	@Option(name = "-d", aliases = "--dest", usage = "Required. Destination CSV to be created after domain validation", required = true)
	private String destFileName;

	@Option(name = "-i", aliases = "--urlIndex", usage = "Required. Index position of the row in CSV that contains URL", required = true)
	private int urlIndex;

	@Option(name = "-v", aliases = "--validRowElemCount", usage = "Optional. Row in the CSV will be ignored if it didn't match this count!")
	private int lengthOfValidRow = -1;

	@Option(name = "-b", aliases = "--batchSize", usage = "Optional. Number of threads per batch. Recommended to set batch timeout as well. Default:100")
	private int batchSize = 100;

	@Option(name = "-f", aliases = "--includeFailedDomains", usage = "Optional. Should the domains failed from processing to be included as well. Default:true")
	private String includeFailedDomainsString = "true";

	private boolean includeFailedDomains = true;

	@Option(name = "-t", aliases = "--batchTimeout", usage = "Optional. Batch timeout in secs. Default:120s")
	private long batchTimeoutInSecs = 120;

	@Option(name = "-r", aliases = "--retryFailed", usage = "Optional. Retry the failed URLs finally with 1/10th of batch size. Default:false")
	private String retryFailedURLsString = "false";

	private boolean retryFailedURLs = false;

	private CSVReader csvReader;

	private CSVWriter csvWriter;

	private List<String[]> failedURLList = new ArrayList<String[]>();

	private AsyncHttpClient client;

	private AtomicLong failedURLs = new AtomicLong(0);

	private AtomicLong invalidDomainURLs = new AtomicLong(0);

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
			this.includeFailedDomains = Boolean
					.parseBoolean(includeFailedDomainsString);
			this.retryFailedURLs = Boolean.parseBoolean(retryFailedURLsString);
			Builder builder = new AsyncHttpClientConfig.Builder();
			builder.setCompressionEnabled(true).setFollowRedirects(true)
					.setConnectionTimeoutInMs(40000).setIOThreadMultiplier(8)
					.setAllowPoolingConnection(true)
					.setRequestTimeoutInMs(60000).build();
			this.client = new AsyncHttpClient(builder.build());
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
		logger.info("Domain validator started!");
		BatchIterator<String[]> iterator = new BatchIterator<String[]>(csvData,
				batchSize);
		if (retryFailedURLs) {
			batch(iterator, false);

			logger.info("Begin processing the urls ended up in UHE, second time!");
			int failedBatchSize = batchSize / 10;
			if (failedBatchSize < 5) {
				failedBatchSize = 5;
			} else if (failedBatchSize < 10) {
				failedBatchSize = 10;
			}
			BatchIterator<String[]> unknownHostIterator = new BatchIterator<String[]>(
					failedURLList, failedBatchSize);
			batch(unknownHostIterator, true);
		} else {
			batch(iterator, true);
		}

		logger.info("Time to process" + ":"
				+ (System.currentTimeMillis() - startTime) + "ms");
		logger.info("Total URLs:" + csvData.size() + "\nFailed URLs:"
				+ failedURLs.longValue() + "\nInvalid Domain URLs:"
				+ invalidDomainURLs.longValue());
		Thread.sleep(10000);
		csvReader.close();
		csvWriter.flush();
		csvWriter.close();
	}

	/**
	 * @param iterator
	 * @throws IOException
	 */
	private void batch(BatchIterator<String[]> iterator,
			boolean isFailedURLBatch) throws IOException {
		int batchNum = 1;
		while (iterator.hasNext()) {
			long batchStartTime = System.currentTimeMillis();
			Collection<String[]> currentBatch = iterator.next();
			Map<String, String[]> urlMap = getURLMapFromCSV(currentBatch);
			performRequest(urlMap, !isFailedURLBatch);
			csvWriter.flush();
			logger.info("Time to process the batch-" + batchNum + ":"
					+ (System.currentTimeMillis() - batchStartTime) + "ms");
			batchNum++;
		}
	}

	private void performRequest(final Map<String, String[]> urlMap,
			final boolean addFailedURLToList) {
		final CountDownLatch latch = new CountDownLatch(urlMap.size());
		for (final String url : urlMap.keySet()) {
			try {
				client.prepareGet(url).execute(new AsyncHandler<Void>() {
					private final Response.ResponseBuilder builder = new Response.ResponseBuilder();

					@Override
					public void onThrowable(Throwable e) {
						latch.countDown();
						failedURLs.incrementAndGet();
						logger.error(url + " failed from being processed", e);
						if (addFailedURLToList) {
							failedURLList.add(urlMap.get(url));
						}
						if (includeFailedDomains) {
							csvWriter.writeNext(urlMap.get(url));
						}

					}

					@Override
					public com.ning.http.client.AsyncHandler.STATE onBodyPartReceived(
							HttpResponseBodyPart content) throws Exception {
						builder.accumulate(content);
						return STATE.CONTINUE;
					}

					@Override
					public com.ning.http.client.AsyncHandler.STATE onStatusReceived(
							HttpResponseStatus status) throws Exception {
						builder.accumulate(status);
						return STATE.CONTINUE;
					}

					@Override
					public com.ning.http.client.AsyncHandler.STATE onHeadersReceived(
							HttpResponseHeaders headers) throws Exception {
						builder.accumulate(headers);
						return STATE.CONTINUE;
					}

					@Override
					public Void onCompleted() throws Exception {
						latch.countDown();
						String responseString = builder.build()
								.getResponseBody();
						if (!isDomainForSale(responseString)) {
							csvWriter.writeNext(urlMap.get(url));
							return null;
						} else {
							invalidDomainURLs.incrementAndGet();
						}

						if (includeFailedDomains) {
							csvWriter.writeNext(urlMap.get(url));
						}
						return null;
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
