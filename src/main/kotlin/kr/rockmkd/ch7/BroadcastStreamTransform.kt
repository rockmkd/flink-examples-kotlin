package kr.rockmkd.ch7

import kr.rockmkd.model.SensorReading
import kr.rockmkd.util.SensorSource
import kr.rockmkd.util.ThresholdSource
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment().apply {
        parallelism = 3
        streamTimeCharacteristic = TimeCharacteristic.EventTime
    }

    val sensorData: DataStream<SensorReading> = env.addSource(SensorSource())
    val threshHold: DataStream<Double> = env.addSource(ThresholdSource())
    val broadcastStateDescriptor = MapStateDescriptor<String, Double>("thresholds", String::class.java, Double::class.java)
    sensorData.connect(threshHold.broadcast(broadcastStateDescriptor))
        .process(UpdatableTemperatureAlertFunction())
}

class UpdatableTemperatureAlertFunction: BroadcastProcessFunction<SensorReading, Double, Tuple3<SensorReading, Double, Double>>(){

    override fun processElement(p0: SensorReading?, p1: ReadOnlyContext?, p2: Collector<Tuple3<SensorReading, Double, Double>>?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun processBroadcastElement(p0: Double?, p1: Context?, p2: Collector<Tuple3<SensorReading, Double, Double>>?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}
