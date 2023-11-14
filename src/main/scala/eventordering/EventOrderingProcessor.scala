package eventordering

import org.slf4j.{Logger, LoggerFactory}

object EventOrderingProcessor {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    try {
      val eventProcessingJob = new EventProcessingJob()
      eventProcessingJob.run()
    } catch {
      case ex: Exception =>
        logger.error("Error running Flink job", ex)
    }
  }
}