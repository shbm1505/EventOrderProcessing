package eventordering

import eventordering.config.ConfigurationReader
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import eventordering.model.Event
import eventordering.processing.{EventOrderingProcessFunction, EventMapper}
import org.slf4j.{Logger, LoggerFactory}

class EventProcessingJob {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def run(): Unit = {
    try {
      // Load configurations
      val orderConfigFilePath = Config.getProperty("order.config.file")
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // Create Kafka source
      val kafkaSource = ConfigurationReader.createKafkaSource()
      val lines: DataStream[String] = env.fromSource(
        kafkaSource,
        WatermarkStrategy.noWatermarks(),
        "Kafka Source"
      )

      // Read order configurations from the JSON file
      val orderConfigurations: Map[String, List[String]] = ConfigurationReader.readOrderConfigurations(orderConfigFilePath)
      logger.info("Order Configurations: {}", orderConfigurations)

      // Map raw input to Event objects
      val events: DataStream[Event] = lines.filter(_.nonEmpty).map(new EventMapper)

      // Process events with ordering logic
      val orderedEvents: DataStream[Event] = events
        .keyBy(_.correlation_id)
        .process(new EventOrderingProcessFunction(orderConfigurations))

      orderedEvents.print()

      // Execute the Flink job
      env.execute("Kafka Event Consumer")

    } catch {
      case ex: Exception =>
        logger.error("Error during Flink job execution", ex)
    }
  }
}
