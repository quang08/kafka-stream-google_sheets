# About
- This project is a simple project of a real-time data streaming pipeline that takes data from Apache Kafka and streams it into Google Spreadsheet utilizing Google Spreadsheet API

## Details
- This project reads data from a local broker on port 9092 that fetches data from `https://api.open-meteo.com/v1/forecast` API
- Then writes onto a Google Spreadsheet within an hourly interval. Incoming data is divided into fixed-size, non-overlapping windows. 
- During the streaming process, several aggregations and initialization were done to achive the final result:
* Data is continously consumed from the Kafka topic
* Data is divided into non-overlapping one-hour intervals
* Initialization: For each hour, `intializer_fn` is called with the first data point
* Reduction: As new data points arrive within the hour, `reducer_fn` updates the summary
* At the end of each hour, the window is finalized and written onto the Google Sheet

![image](https://github.com/quang08/kafka-stream-google_sheets/assets/84165564/c420bb53-1ea6-4b5c-b3b1-ccd53c7951a5)
 
