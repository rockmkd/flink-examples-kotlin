package kr.rockmkd.ch5

import kr.rockmkd.common.model.SensorReading
import kr.rockmkd.common.sources.SensorSource
import kr.rockmkd.common.util.SensorTimeAssigner
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

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

