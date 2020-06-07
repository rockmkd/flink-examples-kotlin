package kr.rockmkd.ch7

import kr.rockmkd.model.SensorReading
import kr.rockmkd.util.sources.SensorSource
import kr.rockmkd.util.ThresholdSource
import kr.rockmkd.util.ThresholdUpdate
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import kotlin.math.absoluteValue

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment().apply {
        parallelism = 3
        streamTimeCharacteristic = TimeCharacteristic.EventTime
    }

    val sensorData: DataStream<SensorReading> = env.addSource(SensorSource())
    val threshHold: DataStream<ThresholdUpdate> = env.addSource(ThresholdSource())
    val broadcastStateDescriptor =
        MapStateDescriptor<String, Double>("thresholds", String::class.java, Double::class.java)
    sensorData
        .keyBy(KeySelector<SensorReading, String> { it.id })
        .connect(threshHold.broadcast(broadcastStateDescriptor))
        .process(KeyedTemperatureAlertFunction())
        .print()

    env.execute()
}

class KeyedTemperatureAlertFunction :
    KeyedBroadcastProcessFunction<String, SensorReading, ThresholdUpdate, Tuple3<String, Double, Double>>() {

    private lateinit var lastTemperature: ValueState<Double>

    private val logger = LoggerFactory.getLogger(javaClass)
    private val thresholdStateDescriptor =
        MapStateDescriptor<String, Double>("thresholds", String::class.java, Double::class.java)

    override fun open(parameters: Configuration) {
        val valueStateDescriptor = ValueStateDescriptor<Double>("lastTemp", Double::class.java)
        lastTemperature = runtimeContext.getState(valueStateDescriptor)
    }

    override fun processElement(
        sensorReading: SensorReading,
        readOnlyContext: ReadOnlyContext,
        out: Collector<Tuple3<String, Double, Double>>
    ) {
        val thresholds = readOnlyContext.getBroadcastState(thresholdStateDescriptor)

        if (thresholds.contains(sensorReading.id)) {
            val sensorThreshold = thresholds.get(sensorReading.id)
            lastTemperature.value()?.let {
                val tempDiff = (sensorReading.temperature - it).absoluteValue
                if (tempDiff > sensorThreshold) {
                    lastTemperature.update(it)
                    out.collect(Tuple3(sensorReading.id, sensorReading.temperature, tempDiff))
                }
            }
        }
        lastTemperature.update(sensorReading.temperature)
    }

    override fun processBroadcastElement(
        update: ThresholdUpdate,
        context: Context,
        collector: Collector<Tuple3<String, Double, Double>>
    ) {
        val thresholds = context.getBroadcastState(thresholdStateDescriptor)
        if (update.threshold != 0.0) {
            thresholds.put(update.id, update.threshold)
        } else {
            thresholds.remove(update.id)
        }
    }

}

