package com.nthakur.gateways

import akka.actor.Actor
import akka.camel.Producer
import akka.camel.Oneway
import com.typesafe.config.ConfigFactory
import net.nthakur.model.DomainEvent
import dummy.avro.{HeaderAvro, DomainEventAvro}
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.{EncoderFactory, Encoder}
import java.io.{ByteArrayOutputStream}

/**
 * Created under license Apache 2.0
 * User: nthakur
 * Date: 28/05/12
 * Time: 17:11
 *
 */

/**
 * Emit Domain Events into the ether.
 */
class DomainEventEmitter extends Actor with Producer with Oneway {
  //todo: untested
  def endpointUri = "spring-amqp:emissionX:PosRK?type=direct"

  //This method is called by akka-camel before a Message is sent via the endpointUri.
  override def transformOutgoingMessage(msg: Any): Any = {
    //msg has been sent to this Actor via ! operator. The call site is within the Producer trait 'receive' method.
    //todo: Avro magic on msg which is a DomainEvent: 1. implict converstion to DomainEventAvro. 2. DomainEventAvro -> byte[] 3. byte[] -> Rabbit.
    convertToAvroByteArray(msg.asInstanceOf[DomainEvent])
  }

  private def convertToAvroByteArray(event: DomainEvent): Array[Byte] = {
    // This should be done using annotations and reflection!
    val domainEventAvro: DomainEventAvro = DomainEventAvro.newBuilder()
      .setHeader(HeaderAvro.newBuilder()
      .setEventType(event.header.eventType)
      .setInstanceId(event.header.instanceId)
      .setSource(event.header.source)
      .setOccurrenceTime(event.header.occurrenceTime.getMillis)
      .setDetectionTime(event.header.detectionTime.getMillis)
      .build())
      .setPayload(event.payload.contents)
      .build()

    val bos: java.io.ByteArrayOutputStream = new ByteArrayOutputStream
    val writer: GenericDatumWriter[DomainEventAvro] = new GenericDatumWriter[DomainEventAvro](DomainEventAvro.SCHEMA$)
    val encoder: Encoder = EncoderFactory.get().binaryEncoder(bos, null)

    writer.write(domainEventAvro, encoder)
    encoder.flush()
    bos.toByteArray
  }
}

class GeneratorEventEmitter extends Actor with Producer with Oneway {
  //todo: untested
  def endpointUri = "spring-amqp:SimulationX:tempQ?type=fanout&autodelete=false"

  //This method is called by akka-camel before a Message is sent via the endpointUri.
  override def transformOutgoingMessage(msg: Any): Any = {
    //msg has been sent to this Actor via ! operator. The call site is within the Producer trait 'receive' method.
    msg
  }

  private def convertToAvroByteArray(event: DomainEvent): Array[Byte] = {
    // This should be done using annotations and reflection!
    val domainEventAvro: DomainEventAvro = DomainEventAvro.newBuilder()
      .setHeader(HeaderAvro.newBuilder()
      .setEventType(event.header.eventType)
      .setInstanceId(event.header.instanceId)
      .setSource(event.header.source)
      .setOccurrenceTime(event.header.occurrenceTime.getMillis)
      .setDetectionTime(event.header.detectionTime.getMillis)
      .build())
      .setPayload(event.payload.contents)
      .build()

    val bos: java.io.ByteArrayOutputStream = new ByteArrayOutputStream
    val writer: GenericDatumWriter[DomainEventAvro] = new GenericDatumWriter[DomainEventAvro](DomainEventAvro.SCHEMA$)
    val encoder: Encoder = EncoderFactory.get().binaryEncoder(bos, null)

    writer.write(domainEventAvro, encoder)
    encoder.flush()
    bos.toByteArray
  }
}


object Config {
  val EMITTER_ENDPOINT_URI = ConfigFactory.load().getString("basketGenerator.emitter.amqp.uri")
  val GENERATOR_ENDPOINT_URI = ConfigFactory.load().getString("basketGenerator.generator.uri")
}
