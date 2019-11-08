package kr.rockmkd.util

import kr.rockmkd.model.SensorReading
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import kotlin.random.Random

class SensorSource : RichSourceFunction<SensorReading>() {

    private var running: Boolean = true;

    override fun run(sourceContext: SourceFunction.SourceContext<SensorReading>) {
        val taskIdx: Int = this.runtimeContext.indexOfThisSubtask;

        val sensorIds = (1..10).map { "sensor_" + (taskIdx * 10 + it) }
        val curFTemp = (1..10).map { 65 + Random.nextDouble(10.0, 100.0) }

        while (running) {
            val curTime = System.currentTimeMillis()
            sensorIds.forEachIndexed { index, id ->
                sourceContext.collect(SensorReading(id, curTime, curFTemp[index] + Random.nextDouble(0.0, 4.0)))
            }
            Thread.sleep(100)
        }

    }

    override fun cancel() {
        running = false
    }

}
