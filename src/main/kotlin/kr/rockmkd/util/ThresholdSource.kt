package kr.rockmkd.util

import kr.rockmkd.model.SensorReading
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import kotlin.random.Random

class ThresholdSource : RichSourceFunction<Double>() {

    private var running: Boolean = true;

    override fun run(sourceContext: SourceFunction.SourceContext<Double>) {
        while (running) {
            sourceContext.collect(Random.nextDouble(0.0, 100.0))
        }

        Thread.sleep(1000)
    }

    override fun cancel() {
        running = false
    }

}
