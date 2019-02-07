package weather.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkUtil {

	private static SparkContext sparkContext = null;
	private static SparkSession sparkSession = null;
	
	private synchronized static SparkContext getLocalSparkContext() {
		if(sparkContext == null) {
			SparkConf config = new SparkConf();
			config.setMaster("local");
			config.setAppName("Weather Report Generation");
			config.set("spark.executor.instances", "2");
			
			return new SparkContext(config);
		}
		
		return sparkContext;
	}
	
	public synchronized static SparkSession getLocalSparkSession() {
		if(sparkSession == null) {
			sparkSession = SparkSession.builder().sparkContext(getLocalSparkContext()).getOrCreate();
		}
		return sparkSession;
	}
	
	public static StructType getColumns() {
		List<StructField> fields = new ArrayList<>();
		
		fields.add(DataTypes.createStructField("station", DataTypes.StringType, false));
		fields.add(DataTypes.createStructField("year", DataTypes.IntegerType, false));
		fields.add(DataTypes.createStructField("month", DataTypes.IntegerType, false));
		fields.add(DataTypes.createStructField("max_temp", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("min_temp", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("air_frost", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("rain", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("sun", DataTypes.DoubleType, true));
		
		return DataTypes.createStructType(fields);
	}
}
