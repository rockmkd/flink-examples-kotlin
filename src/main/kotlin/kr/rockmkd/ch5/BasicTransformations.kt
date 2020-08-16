package kr.rockmkd.ch5

import kr.rockmkd.common.model.SensorReading
import kr.rockmkd.common.sources.SensorSource
import kr.rockmkd.common.util.SensorTimeAssigner
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

val logger = LoggerFactory.getLogger("main")

fun main(args: Array<String>) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(3)

    // use event time for the application
    env.streamTimeCharacteristic = TimeCharacteristic.EventTime

    // create a DataStream<SensorReading> from a stream source
    val sensorData: DataStream<SensorReading> = env.addSource(SensorSource())
        .assignTimestampsAndWatermarks(SensorTimeAssigner())        // assign timestamps and watermarks (required for event time)

    val avgTemp: DataStream<SensorReading> = sensorData
        .filter {
            it.temperature > 25
        }.map(MapFunction<SensorReading, SensorReading> {
            val celsius = (it.temperature - 32) * (5.0 / 9.0)
            SensorReading(it.id, it.timestamp, celsius)
        }).keyBy(KeySelector<SensorReading, String> {
            it.id
        }).timeWindow(Time.seconds(5))
        .apply(TemperatureAverager())
    avgTemp.print()
    env.execute()

}

class TemperatureAverager : WindowFunction<SensorReading, SensorReading, String, TimeWindow> {
    /**
     * apply() is invoked once for each window.
     *
     * @param sensorId the key (sensorId) of the window
     * @param window meta data for the window
     * @param input an iterable over the collected sensor readings that were assigned to the window
     * @param out a collector to emit results from the function
     */
    override fun apply(
        sensorId: String,
        window: TimeWindow,
        input: MutableIterable<SensorReading>,
        out: Collector<SensorReading>
    ) {
        var cnt = 0
        var sum = 0.0
        for (r in input) {
            cnt++
            sum += r.temperature
        }

        val avgTemp = sum / cnt
        out.collect(SensorReading(sensorId, window.end, avgTemp))
    }
}
