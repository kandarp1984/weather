package weather.concurrency;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import weather.util.SparkUtil;

public class LoadDataTask extends RecursiveTask<TaskResult> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1138847495320308685L;

	private volatile String stationName = null;

	private final String DATA_URL = "https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/<station_name>data.txt";

	public LoadDataTask(String stationName) {
		this.stationName = stationName;
	}

	@SuppressWarnings("resource")
	@Override
	protected TaskResult compute() {
		Thread.currentThread().setName(stationName);
		System.out.println("reading weather data for " + stationName);

		TaskResult taskResult = new TaskResult();
		taskResult.setStatus(false);
		taskResult.setStation(stationName);
		String stationDataUrl = DATA_URL.replaceAll("<station_name>", stationName);
		URL url = null;
		BufferedReader reader = null;
		String data = null;
		List<Row> rows = new ArrayList<>();

		try {
			url = new URL(stationDataUrl);
			reader = new BufferedReader(new InputStreamReader(url.openStream()));

			boolean skip = true;
			while ((data = reader.readLine()) != null) {
				if (data != null && !"Site Closed".equalsIgnoreCase(data)) {
					data = data.trim().replaceAll("( )+", ",").replaceAll("#", "").replaceAll(Pattern.quote("*"), "")
							.replaceAll("---", "-99999");
					String[] datas = data.split(",");

					if (!skip) {
						try {
							int year = Integer.parseInt(datas[0]);
							int month = Integer.parseInt(datas[1]);
							double maxTemp = Double.parseDouble(datas[2]);
							double minTemp = Double.parseDouble(datas[3]);
							int af = Integer.parseInt(datas[4]);
							double rain = Double.parseDouble(datas[5]);

							if (datas.length >= 7) {
								double sun = Double.parseDouble(datas[6]);

								Row row = RowFactory.create(stationName, year, month, maxTemp, minTemp, af, rain, sun);
								rows.add(row);
							} else {
								Row row = RowFactory.create(stationName, year, month, maxTemp, minTemp, af, rain, -99999d);
								rows.add(row);
							}

						} catch (NumberFormatException nfe) {
							System.err.println(
									"this data contains string but number expected hence it will not be added in data set");
							System.err.println("Station : ".concat(stationName).concat(" - Year : ").concat(datas[0])
									.concat(" - Month : ").concat(datas[1]));
							continue;
						}
					}

					if (datas.length > 1 && "degC".equalsIgnoreCase(datas[1])) {
						skip = false;
					}
				}
			}

			JavaRDD<Row> javaRDD = new JavaSparkContext(SparkUtil.getLocalSparkSession().sparkContext())
					.parallelize(rows);
			Dataset<Row> dataSet = SparkUtil.getLocalSparkSession().createDataFrame(javaRDD, SparkUtil.getColumns());

			taskResult.setStationData(dataSet);
			taskResult.setStatus(true);

			System.out.println("successfully completed reading weather data for " + stationName);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return taskResult;
	}

}
