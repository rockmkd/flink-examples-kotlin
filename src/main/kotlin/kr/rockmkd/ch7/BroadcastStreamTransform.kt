package kr.rockmkd.ch7

import kr.rockmkd.common.model.SensorReading
import kr.rockmkd.common.sources.SensorSource
import kr.rockmkd.common.sources.ThresholdSource
import kr.rockmkd.common.sources.ThresholdUpdate
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

fun main() {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(Configuration()).apply {
        parallelism = 3
        streamTimeCharacteristic = TimeCharacteristic.EventTime
    }

    val sensorData: DataStream<SensorReading> = env.addSource(SensorSource())
    val threshHold: DataStream<ThresholdUpdate> = env.addSource(
        ThresholdSource()
    )
    val broadcastStateDescriptor =
        MapStateDescriptor<String, Double>("thresholds", String::class.java, Double::class.java)
    sensorData.connect(threshHold.broadcast(broadcastStateDescriptor))
        .process(UpdatableTemperatureAlertFunction())
        .print()

    env.execute()
}

class UpdatableTemperatureAlertFunction :
    BroadcastProcessFunction<SensorReading, ThresholdUpdate, Tuple3<String, Double, Double>>() {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val thresholdStateDescriptor =
        MapStateDescriptor<String, Double>("thresholds", String::class.java, Double::class.java)

    override fun processElement(
        sensorReading: SensorReading,
        readOnlyContext: ReadOnlyContext,
        out: Collector<Tuple3<String, Double, Double>>
    ) {
        val thresholds = readOnlyContext.getBroadcastState(thresholdStateDescriptor)

        if (thresholds.contains(sensorReading.id)) {
            val sensorThreshold: Double = thresholds.get(sensorReading.id)

            if (sensorThreshold < sensorReading.temperature) {
                out.collect(Tuple3(sensorReading.id, sensorReading.temperature, sensorThreshold))
            }
        }
    }

    override fun processBroadcastElement(
        update: ThresholdUpdate,
        context: Context,
        p2: Collector<Tuple3<String, Double, Double>>?
    ) {
        val thresholds = context.getBroadcastState(thresholdStateDescriptor)
        if (update.threshold != 0.0) {
            thresholds.put(update.id, update.threshold)
        } else {
            thresholds.remove(update.id)
        }
    }

}

