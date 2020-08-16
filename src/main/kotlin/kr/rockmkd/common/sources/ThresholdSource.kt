package kr.rockmkd.common.sources

import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import kotlin.random.Random

class ThresholdSource : RichSourceFunction<ThresholdUpdate>() {

    private var running: Boolean = true;

    override fun run(sourceContext: SourceFunction.SourceContext<ThresholdUpdate>) {
        val taskIdx: Int = this.runtimeContext.indexOfThisSubtask;
        while (running) {
            sourceContext.collect(
                ThresholdUpdate(
                    "sensor_" + ((taskIdx * 10) + Random.nextInt(1, 10)),
                    Random.nextDouble(0.0, 5.0)
                )
            )
            Thread.sleep(Random.nextLong(2, 5) * 1000)
        }
    }

    override fun cancel() {
        running = false
    }

}

data class ThresholdUpdate(val id: String, val threshold: Double)
