package kr.rockmkd.ch7

import kr.rockmkd.model.SensorReading
import kr.rockmkd.util.SensorSource
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import kotlin.math.absoluteValue

fun main() {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(Configuration()).apply {
        parallelism = 3
        streamTimeCharacteristic = TimeCharacteristic.EventTime
    }

    val sensorData: DataStream<SensorReading> = env.addSource(SensorSource())
    val keyedData: KeyedStream<SensorReading, String> = sensorData.keyBy (KeySelector {it.id} )

    keyedData.flatMap(TemperatureAlertFunction(1.7)).print()
    env.execute()
}

class TemperatureAlertFunction(private val threshHold: Double) :
    RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>>() {

    private lateinit var lastTempState: ValueState<Double>

    override fun flatMap(data: SensorReading, out: Collector<Tuple3<String, Double, Double>>) {
        val lastTemp: Double = lastTempState.value()?:0.0
        val diff = (data.temperature - lastTemp).absoluteValue
        if (diff > threshHold) {
            out.collect(Tuple3(data.id, data.temperature, diff))
        }
        lastTempState.update(data.temperature)
    }

    override fun open(parameters: Configuration) {
        val lastTempDescriptor = ValueStateDescriptor<Double>("lastTemp", Double::class.java)
        lastTempState = runtimeContext.getState<Double>(lastTempDescriptor)
    }

}