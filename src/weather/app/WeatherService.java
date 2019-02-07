package weather.app;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import weather.concurrency.LoadDataTask;
import weather.concurrency.TaskResult;

public class WeatherService {

	private static final int MAX_THREADS = Runtime.getRuntime().availableProcessors();
	private static final String[] stations = new String[] { "aberporth", "armagh", "ballypatrick", "bradford",
			"braemar", "camborne", "cambridge", "cardiff", "chivenor", "cwmystwyth", "dunstaffnage", "durham",
			"eastbourne", "eskdalemuir", "heathrow", "hurn", "lerwick", "leuchars", "lowestoft", "manston", "nairn",
			"newtonrigg", "oxford", "paisley", "ringway", "rossonwye", "shawbury", "sheffield", "southampton",
			"stornoway", "suttonbonington", "tiree", "valley", "waddington", "whitby", "wickairport", "yeovilton" };

	public static void main(String[] args) {

		List<Future<TaskResult>> futureList = new ArrayList<>();
		ForkJoinPool executorPool = new ForkJoinPool(MAX_THREADS);

		for (String station : stations) {
			futureList.add(executorPool.submit(new LoadDataTask(station)));
		}

		try {
			boolean result = true;
			for (Future<TaskResult> future : futureList) {
				TaskResult taskResult = future.get();
				result = result & taskResult.isStatus();
			}

			if (result) {
				System.out.println("successfully loaded all stations weather data");
				Dataset<Row> allStationsData = null;

				for (Future<TaskResult> taskResult : futureList) {
					if (allStationsData == null) {
						allStationsData = taskResult.get().getStationData();
					} else {
						Dataset<Row> unionDataset = allStationsData.union(taskResult.get().getStationData());
						allStationsData = unionDataset;
					}
				}

				longRunningStation(allStationsData);
				onlineStation(allStationsData);

				mayAnalysis(allStationsData);

				getMaximumRainFall(allStationsData);
				getMinimumRainFall(allStationsData);

				getMaximumRainFallPerStation(allStationsData);
				getMinimumRainFallPerStation(allStationsData);

				getMaximumSunshine(allStationsData);
				getMinimumSunshine(allStationsData);

				getMaximumSunshinePerStation(allStationsData);
				getMinimumSunshinePerStation(allStationsData);

			}
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	private static void longRunningStation(Dataset<Row> allStationsData) {
		System.out.println("long running station");
		Dataset<Row> stationData = allStationsData.groupBy(col("station")).agg(count(lit(1)).alias("measures"))
				.select(col("station"), col("measures"));

		stationData.show(false);

		Dataset<Row> filteredData = stationData.agg(max("measures").alias("max_measures"));

		Dataset<Row> measureData = stationData
				.join(filteredData, stationData.col("measures").eqNullSafe(filteredData.col("max_measures")))
				.select(col("station"), col("measures").alias("No of Measures"));
		measureData.show(false);
	}

	private static void onlineStation(Dataset<Row> allStationsData) {
		System.out.println("running station");
		Dataset<Row> stationData = allStationsData.groupBy(col("station")).agg(count(lit(1)).alias("measures"))
				.select(col("station"), col("measures"));

		Dataset<Row> filteredData = stationData.groupBy(col("station")).agg(max("measures").alias("measures"))
				.select(col("station").alias("long_station"), col("measures"));

		Dataset<Row> measuresData = allStationsData
				.join(filteredData, allStationsData.col("station").eqNullSafe(filteredData.col("long_station")))
				.groupBy(col("station"), col("measures")).agg(min(col("year")).alias("year"))
				.select(col("station"), col("year").alias("Online Since"), col("measures").alias("No Of Measures"))
				.orderBy(col("station")).distinct();

		measuresData.show(40, false);
	}

	private static void mayAnalysis(Dataset<Row> allStationsData) {
		Dataset<Row> mayData = allStationsData.where(
				col("month").eqNullSafe(5).and(col("max_temp").notEqual(-99999)).and(col("min_temp").notEqual(-99999)));

		Dataset<Row> summaryData = mayData.select(col("station"), col("year"),
				(col("max_temp").plus(col("min_temp"))).divide(2).alias("temp"));

		Dataset<Row> worstTempData = summaryData.groupBy(col("station")).agg(max("temp").alias("average_temp"))
				.select(col("station").alias("station_worst"), col("average_temp"));
		Dataset<Row> bestTempData = summaryData.groupBy(col("station")).agg(min("temp").alias("average_temp"))
				.select(col("station").alias("station_best"), col("average_temp"));

		System.out.println("May : worst year temprature for station");
		Dataset<Row> worstYearData = summaryData
				.join(worstTempData,
						summaryData.col("station").eqNullSafe(worstTempData.col("station_worst"))
								.and(summaryData.col("temp").eqNullSafe(worstTempData.col("average_temp"))))
				.select(col("station"), col("year"), worstTempData.col("average_temp").alias("Average Temp")).distinct()
				.orderBy(col("station"));

		worstYearData.show(40, false);

		System.out.println("May : best year temprature for station");
		Dataset<Row> bestYearData = summaryData
				.join(bestTempData,
						summaryData.col("station").eqNullSafe(bestTempData.col("station_best"))
								.and(summaryData.col("temp").eqNullSafe(bestTempData.col("average_temp"))))
				.select(col("station"), col("year"), bestTempData.col("average_temp").alias("Average Temp")).distinct()
				.orderBy(col("station"));

		bestYearData.show(40, false);
	}

	private static void getMaximumRainFall(Dataset<Row> allStationsData) {
		Dataset<Row> filteredData = allStationsData.agg(max(col("rain")).alias("max_rain"));

		System.out.println("maximum rain across all stations");
		Dataset<Row> rainData = allStationsData
				.join(filteredData, allStationsData.col("rain").eqNullSafe(filteredData.col("max_rain")))
				.select(col("station"), col("year"), col("month"), col("rain").alias("Maximum Rain"));

		rainData.show(false);
	}

	private static void getMinimumRainFall(Dataset<Row> allStationsData) {
		Dataset<Row> filteredData = allStationsData.select(col("rain")).where(col("rain").gt(-99999));

		Dataset<Row> minRainData = filteredData.agg(min(col("rain")).alias("min_rain"));

		System.out.println("minimum rain across all stations");
		Dataset<Row> rainData = allStationsData
				.join(minRainData, allStationsData.col("rain").eqNullSafe(minRainData.col("min_rain")))
				.select(col("station"), col("year"), col("month"), col("rain").alias("Minimum Rain"));

		rainData.show(false);
	}

	private static void getMaximumRainFallPerStation(Dataset<Row> allStationsData) {
		Dataset<Row> filteredData = allStationsData.groupBy(col("station")).agg(max(col("rain")).alias("max_rain"))
				.select(col("station").alias("max_station"), col("max_rain"));

		System.out.println("maximum rain per station");
		Dataset<Row> rainData = allStationsData
				.join(filteredData,
						allStationsData.col("rain").eqNullSafe(filteredData.col("max_rain"))
								.and(allStationsData.col("station").eqNullSafe(filteredData.col("max_station"))))
				.select(col("station"), col("year"), col("month"), col("rain").alias("Maximum Rain"));

		rainData.show(40, false);
	}

	private static void getMinimumRainFallPerStation(Dataset<Row> allStationsData) {
		Dataset<Row> minRainData = allStationsData.select(col("rain"), col("station")).where(col("rain").gt(-99999));

		Dataset<Row> filteredData = minRainData.groupBy(col("station")).agg(min(col("rain")).alias("min_rain"))
				.select(col("station").alias("min_station"), col("min_rain"));

		System.out.println("minimum rain per station");
		Dataset<Row> rainData = allStationsData
				.join(filteredData,
						allStationsData.col("rain").eqNullSafe(filteredData.col("min_rain"))
								.and(allStationsData.col("station").eqNullSafe(filteredData.col("min_station"))))
				.select(col("station"), col("year"), col("month"), col("rain").alias("Minimum Rain"));

		rainData.show(40, false);
	}

	private static void getMaximumSunshine(Dataset<Row> allStationsData) {
		Dataset<Row> filteredData = allStationsData.agg(max(col("sun")).alias("max_sun"));

		System.out.println("best sunshine across all stations");
		Dataset<Row> sunshineData = allStationsData
				.join(filteredData, allStationsData.col("sun").eqNullSafe(filteredData.col("max_sun")))
				.select(col("station"), col("year"), col("month"), col("sun").alias("Best Sunshine"));

		sunshineData.show(false);
	}

	private static void getMinimumSunshine(Dataset<Row> allStationsData) {
		Dataset<Row> filteredData = allStationsData.select(col("sun")).where(col("sun").gt(-99999));

		Dataset<Row> minSunshineData = filteredData.agg(min(col("sun")).alias("min_sun"));

		System.out.println("worst sunshine across all stations");
		Dataset<Row> sunshineData = allStationsData
				.join(minSunshineData, allStationsData.col("sun").eqNullSafe(minSunshineData.col("min_sun")))
				.select(col("station"), col("year"), col("month"), col("sun").alias("Worst Sunshine"));

		sunshineData.show(false);
	}

	private static void getMaximumSunshinePerStation(Dataset<Row> allStationsData) {
		Dataset<Row> filteredData = allStationsData.groupBy(col("station")).agg(max(col("sun")).alias("max_sun"))
				.select(col("station").alias("max_station"), col("max_sun"));

		System.out.println("best sunshine per station");
		Dataset<Row> sunshineData = allStationsData
				.join(filteredData,
						allStationsData.col("sun").eqNullSafe(filteredData.col("max_sun"))
								.and(allStationsData.col("station").eqNullSafe(filteredData.col("max_station"))))
				.select(col("station"), col("year"), col("month"), col("sun").alias("Best Sunshine"))
				.orderBy(col("station"));

		sunshineData.show(40, false);
	}

	private static void getMinimumSunshinePerStation(Dataset<Row> allStationsData) {
		Dataset<Row> minRainData = allStationsData.select(col("sun"), col("station")).where(col("sun").gt(-99999));

		Dataset<Row> filteredData = minRainData.groupBy(col("station")).agg(min(col("sun")).alias("min_sun"))
				.select(col("station").alias("min_station"), col("min_sun"));

		System.out.println("worst sunshine per station");
		Dataset<Row> sunshineData = allStationsData
				.join(filteredData,
						allStationsData.col("sun").eqNullSafe(filteredData.col("min_sun"))
								.and(allStationsData.col("station").eqNullSafe(filteredData.col("min_station"))))
				.select(col("station"), col("year"), col("month"), col("sun").alias("Worst Sunshine"))
				.orderBy(col("station"));

		sunshineData.show(40, false);
	}
}
