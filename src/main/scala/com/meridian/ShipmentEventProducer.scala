
package com.meridian

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import java.time.Instant

object ShipmentEventProducer {

  def main(args: Array[String]): Unit = {

    val bootstrapServers = "localhost:9092"
    val topic = "meridian-shipment-events"

    val properties = new Properties()

    properties.put(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      bootstrapServers
    )

    properties.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer].getName
    )

    properties.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer].getName
    )

    properties.put(
      ProducerConfig.ACKS_CONFIG,
      "all"
    )

    val producer =
      new KafkaProducer[String, String](properties)

    try {

      for (i <- 1 to 5) {

        val shipmentId = f"SHP_MERIDIAN_$i%03d"

        val event =
          s"""{"shipment_id":"$shipmentId","event_type":"IN_TRANSIT","location":"Salem","event_time":"${Instant.now()}"}"""

        val record =
          new ProducerRecord[String, String](
            topic,
            shipmentId,
            event
          )

        val metadata = producer.send(record).get()

        println(
          s"Published: $shipmentId | Partition: ${metadata.partition()} | Offset: ${metadata.offset()}"
        )
      }
      // Test event: Missing shipment ID
      val missingIdEvent =
        s"""{"event_type":"IN_TRANSIT","location":"Salem","event_time":"${Instant.now()}"}"""

      val missingIdRecord =
        new ProducerRecord[String, String](
          topic,
          "MISSING_ID_TEST",
          missingIdEvent
        )

      val missingIdMetadata = producer.send(missingIdRecord).get()

      println(
        s"Published: MISSING_ID_TEST | Partition: ${missingIdMetadata.partition()} | Offset: ${missingIdMetadata.offset()}"
      )
      // Test event: Invalid JSON
      val invalidJsonEvent =
        """{"shipment_id":"SHP_INVALID_001","event_type":"""

      val invalidJsonRecord =
        new ProducerRecord[String, String](
          topic,
          "INVALID_JSON_TEST",
          invalidJsonEvent
        )

      val invalidJsonMetadata = producer.send(invalidJsonRecord).get()

      println(
        s"Published: INVALID_JSON_TEST | Partition: ${invalidJsonMetadata.partition()} | Offset: ${invalidJsonMetadata.offset()}"
      )

    } finally {
      producer.close()
    }
  }
}
