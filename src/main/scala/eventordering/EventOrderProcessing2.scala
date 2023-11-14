package eventordering

import java.time.Duration
import java.io.InputStream
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

object EventOrderingProcessor2 {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  case class Event(microservice: String, event_type: String, correlation_id: String, timestamp: String)
  case class OrderConfig(correlation_id: String, order: List[String])

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaSource = createKafkaSource()
    val lines: DataStream[String] = env.fromSource(
      kafkaSource,
      WatermarkStrategy.noWatermarks(),
      "Kafka Source"
    )

    // Read order configurations from the JSON file
    val orderConfigurations: Map[String, List[String]] = readOrderConfigurations("event_order.json")
    logger.info("Order Configurations: {}", orderConfigurations)

    val events: DataStream[Event] = lines.filter(_.nonEmpty).map(new EventMapper)

    // Process events with ordering logic
    val orderedEvents: DataStream[Event] = events
      .keyBy(_.correlation_id)
      .process(new EventOrderingProcessFunction(orderConfigurations))

    // Print the ordered events to the console
    orderedEvents.print()

    // Execute the Flink job
    env.execute("Kafka Event Consumer")
  }

  private def createKafkaSource(): KafkaSource[String] = {
    KafkaSource.builder[String]()
      .setBootstrapServers("localhost:9092")
      .setTopics("domain_events")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()
  }

  private def readOrderConfigurations(filePath: String): Map[String, List[String]] = {
    try {
      val orderConfigJson: InputStream = getClass.getClassLoader.getResourceAsStream(filePath)
      val objectMapper = new ObjectMapper()
      objectMapper.registerModule(DefaultScalaModule)
      objectMapper.readValue(orderConfigJson, classOf[Array[OrderConfig]]).map(config => config.correlation_id -> config.order).toMap
    } catch {
      case e: Exception =>
        logger.error(s"Error reading order configuration JSON from $filePath", e)
        Map.empty[String, List[String]]
    }
  }

  class EventMapper extends MapFunction[String, Event] {
    override def map(jsonString: String): Event = {
      try {
        val objectMapper = new ObjectMapper()
        objectMapper.registerModule(DefaultScalaModule)
        objectMapper.readValue(jsonString, classOf[Event])
      } catch {
        case e: Exception =>
          logger.error("Error parsing event JSON", e)
          Event("", "", "", "")
      }
    }
  }

  class EventOrderingProcessFunction(orderConfigurations: Map[String, List[String]]) extends KeyedProcessFunction[String, Event, Event] {
    private val buffer = collection.mutable.Map[String, collection.mutable.Buffer[Event]]()
    private val watermarkTimeout = Duration.ofSeconds(5)

    override def processElement(value: Event, ctx: KeyedProcessFunction[String, Event, Event]#Context, out: Collector[Event]): Unit = {
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
    override

    def

    onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, Event]#OnTimerContext, out: Collector[Event]): Unit = {
      // Iterate over all correlationIds
      for ((correlationId, events) <- buffer) {
        // Sort the events in the buffer based on the orderConfig
        val sortedEvents = events.sortBy(event => (orderConfigurations(correlationId).indexOf(event.event_type), event.timestamp))

        // Emit the sorted events
        sortedEvents.foreach(out.collect)
      }

      // Clear the buffer
      buffer.clear()
    }
  }
}
