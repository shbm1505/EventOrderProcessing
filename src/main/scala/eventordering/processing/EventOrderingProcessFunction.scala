package eventordering.processing

import java.time.Duration

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import eventordering.model.Event

class EventOrderingProcessFunction(orderConfigurations: Map[String, List[String]]) extends KeyedProcessFunction[String, Event, Event] {
  // Use a concurrent map to handle thread-safety
  private val buffer = scala.collection.concurrent.TrieMap[String, collection.mutable.Buffer[Event]]()
  private val watermarkTimeout = Duration.ofSeconds(5)

  override def processElement(
                               value: Event,
                               ctx: KeyedProcessFunction[String, Event, Event]#Context,
                               out: Collector[Event]
                             ): Unit = {
    val correlationId = value.correlation_id
    val orderConfig = orderConfigurations.getOrElse(correlationId, Nil)

    if (orderConfig.isEmpty) {
      // If there's no order configuration for this correlationId, emit the event as-is
      out.collect(value)
    } else {
      // Create a buffer for each correlationId if it doesn't exist
      buffer.getOrElseUpdate(correlationId, collection.mutable.Buffer.empty[Event]) += value

      // Register a timer to emit buffered events if no watermark has been received for a while
      ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + watermarkTimeout.toMillis())
    }
  }

  override def onTimer(
                        timestamp: Long,
                        ctx: KeyedProcessFunction[String, Event, Event]#OnTimerContext,
                        out: Collector[Event]
                      ): Unit = {
    // Iterate over all correlationIds
    buffer.keys.foreach { correlationId =>
      // Sort the events in the buffer based on the orderConfig
      val sortedEvents =
        buffer(correlationId).sortBy(event => (orderConfigurations(correlationId).indexOf(event.event_type), event.timestamp))

      // Emit the sorted events
      sortedEvents.foreach(out.collect)
    }

    // Clear the buffer
    buffer.clear()
  }
}
