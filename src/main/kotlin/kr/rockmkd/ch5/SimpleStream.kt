package kr.rockmkd.ch5

import kr.rockmkd.model.SensorReading
import kr.rockmkd.util.SensorSource
import kr.rockmkd.util.SensorTimeAssigner
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

fun main(args: Array<String>) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(Configuration()).setParallelism(1)

    // use event time for the application
    env.streamTimeCharacteristic = TimeCharacteristic.EventTime

    // create a DataStream<SensorReading> from a stream source
    val sensorData: DataStream<SensorReading> = env.addSource(SensorSource())
        .assignTimestampsAndWatermarks(SensorTimeAssigner())        // assign timestamps and watermarks (required for event time)


    sensorData
        .filter {
            it.temperature > 25
        }
        .print()
    env.execute()

}

