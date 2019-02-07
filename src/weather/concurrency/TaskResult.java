package weather.concurrency;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class TaskResult {

	private String station;
	private Dataset<Row> stationData;
	private boolean status;
	
	public String getStation() {
		return station;
	}
	
	public void setStation(String station) {
		this.station = station;
	}
	
	public Dataset<Row> getStationData() {
		return stationData;
	}
	
	public void setStationData(Dataset<Row> stationData) {
		this.stationData = stationData;
	}
	
	public boolean isStatus() {
		return status;
	}
	
	public void setStatus(boolean status) {
		this.status = status;
	}
	
}
