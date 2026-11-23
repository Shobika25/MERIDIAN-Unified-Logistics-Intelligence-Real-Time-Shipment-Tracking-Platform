
package com.meridian

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import java.time.Instant

object IdempotencyTestProducer {

  def main(args: Array[String]): Unit = {

    val properties = new Properties()

    properties.put(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      "localhost:9092"
    )

    properties.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer].getName
    )

    properties.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer].getName
    )

    properties.put(ProducerConfig.ACKS_CONFIG, "all")

    val producer =
      new KafkaProducer[String, String](properties)

    try {
      val shipmentId = "SHP_IDEMPOTENCY_001"

      val event =
        s"""{"shipment_id":"$shipmentId","event_type":"IN_TRANSIT","location":"Salem","event_time":"${Instant.now()}"}"""

      val record = new ProducerRecord[String, String](
        "meridian-shipment-events",
        shipmentId,
        event
      )

      val metadata = producer.send(record).get()

      println(
        s"Published: $shipmentId | Partition: ${metadata.partition()} | Offset: ${metadata.offset()}"
      )

    } finally {
      producer.close()
    }
  }
}
