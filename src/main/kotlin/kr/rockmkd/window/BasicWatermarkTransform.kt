package kr.rockmkd.window

import org.apache.flink.api.common.state.BroadcastState
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment().apply {
        parallelism = 3
        streamTimeCharacteristic = TimeCharacteristic.EventTime
    }

    val broadcastStateDescriptor =
        MapStateDescriptor<String, Double>("threshold", String::class.java, Double::class.java)
    env.fromCollection(temperatureData)
        .assignTimestampsAndWatermarks(object: AssignerWithPeriodicWatermarks<SimpleSensor>{
            override fun getCurrentWatermark() = Watermark(1000)
            override fun extractTimestamp(el: SimpleSensor, previousElementTimestamp: Long) = el.timestamp
        })
        .keyBy(KeySelector<SimpleSensor, String> { it.id })
        .connect(env.fromCollection(thresholdData)
//            .assignTimestampsAndWatermarks(object: AssignerWithPeriodicWatermarks<Threshold>{
//                override fun getCurrentWatermark() = Watermark(1000)
//                override fun extractTimestamp(el: Threshold, previousElementTimestamp: Long) = el.timestamp
//            })
            .broadcast(broadcastStateDescriptor)
        )
        .process(ThresholdFunction())

    env.execute()
}

class ThresholdFunction : KeyedBroadcastProcessFunction<String, SimpleSensor, Threshold, String>() {

    private val logger = LoggerFactory.getLogger(this.javaClass)

    private val threshHoldDescriptor = MapStateDescriptor<String, Double>(
        "threshold",
        TypeInformation.of(object : TypeHint<String>() {}),
        TypeInformation.of(object : TypeHint<Double>() {})
    )

    override fun processBroadcastElement(threshold: Threshold, ctx: Context, out: Collector<String>) {
        val state: BroadcastState<String, Double> = ctx.getBroadcastState(threshHoldDescriptor)
        state.get(threshold.id) ?: state.put(threshold.id, threshold.temperature)
        logger.info("set threshold")
    }

    override fun processElement(simpleSensor: SimpleSensor, ctx: ReadOnlyContext, out: Collector<String>) {
        val threshold = ctx.getBroadcastState(threshHoldDescriptor).get(simpleSensor.id)

        if (threshold != null) {
            logger.info("threshold: $threshold, timestamp: ${simpleSensor.timestamp}")
        } else {
            logger.error("No Threshold")
        }
    }

}

val temperatureData = listOf(
    SimpleSensor("1", 0.5,1574086014066),
    SimpleSensor("2", 1.0,1574086114066),
    SimpleSensor("3", 1.5,1574086214066),
    SimpleSensor("1", 2.5,1574086314066),
    SimpleSensor("2", 3.5,1574086414066),
    SimpleSensor("3", 4.5,1574086514066),
    SimpleSensor("1", 5.5,1574086614066),
    SimpleSensor("2", 6.5,1574086714066),
    SimpleSensor("3", 7.5,1574086084066),
    SimpleSensor("1", 5.3,1574086914066),
    SimpleSensor("2", 6.2,1574085014066),
    SimpleSensor("3", 7.1, 1574086014066)
)

val thresholdData = listOf(
    Threshold("1", 1.0, 1573087014066),
    Threshold("2", 1.5, 1573087017066),
    Threshold("3", 1.7, 1573087018066)
)


data class SimpleSensor(val id: String, val temperature: Double, val timestamp:Long)

data class Threshold(val id: String, val temperature: Double, val timestamp:Long)
