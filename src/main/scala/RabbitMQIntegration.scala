import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import com.rabbitmq.client.{ConnectionFactory, Connection, AMQP, Channel}
import com.typesafe.config.ConfigFactory
import scala.util.parsing.json._

/**
 * Created under license Apache 2.0
 * User: nthakur
 * Date: 11/05/12
 * Time: 15:45
 *
 */

// Message classes
// sealed stops this trait from being used outside this module.
// trait allows a mix of abstract and concrete methods - a mixin which allows implementation reuse.
sealed trait SimulatorEvent
//case classes/objects can be used for pattern matching, have compiler generated toString, equality, constructor methods.
case class GeneratorEvent(message: Map[String, String]) extends SimulatorEvent
case class BusinessEvent(message: Map[String, AnyRef]) extends SimulatorEvent

class RabbitMQIntegration extends Actor with ActorLogging {

  private[this] var conn: Option[com.rabbitmq.client.Connection] = None
  private[this] var businessChannel: Option[com.rabbitmq.client.Channel] = None
  private[this] var simulationChannel: Option[com.rabbitmq.client.Channel] = None

  def receive = {
    case GeneratorEvent(message) =>
      publishGeneratorEvent(message)
    case BusinessEvent(message) =>
      publishBusinessEvent(message)
  }

  private def publishBusinessEvent(message: Map[String, AnyRef]) {
    // We call .toMap to convert from Mutable to Immutable map.
    val builder = new AMQP.BasicProperties.Builder
    // It's important to set the contentType property - otherwise node-amqp will see bytes instead of JSON.
    businessChannel.get.basicPublish(Config.RABBITMQ_EXCHANGE_BUSINESS, Config.RABBITMQ_ROUTINGKEY_BUSINESS,
      builder.contentType("application/json").build(), JSONObject(message).toString().getBytes);
  }

  private def publishGeneratorEvent(message: Map[String, String]) {
    // We call .toMap to convert from Mutable to Immutable map.
    val builder = new AMQP.BasicProperties.Builder
    // It's important to set the contentType property - otherwise node-amqp will see bytes instead of JSON.
    // Note: we publish via the exchange to all Q's so no routing_key required.
    log.info("Sending message: " + JSONObject(message).toString())
    simulationChannel.get.basicPublish(Config.RABBITMQ_EXCHANGE_SIMULATION, "",
      builder.contentType("application/json").build(), JSONObject(message).toString().getBytes);
  }

  // This should be shared amongst each instance of IntegrationLayer to avoid expensive activity.
  object RabbitMQConnection {
    private val connection: Connection = null;

    def getConnection(): Connection = {
      connection match {
        case null => {
          val factory = new ConnectionFactory()
          factory.setHost("localhost")
          factory.newConnection()
        }
        case _ => connection
      }
    }
  }

  // Called before Actor starts accepting messages.
  override def preStart() {

    // Set up RabbitMQ
    conn = Some(RabbitMQConnection.getConnection())
    businessChannel = conn map { c => c.createChannel() }
    simulationChannel = conn map { c => c.createChannel() }

    //PubSub Q
    simulationChannel.get.exchangeDeclare(Config.RABBITMQ_EXCHANGE_SIMULATION, "fanout", false)//durable
    // Competing consumer Q
    businessChannel.get.exchangeDeclare(Config.RABBITMQ_EXCHANGE_BUSINESS, "direct", false)//durable=false
    // todo: Review settings as per tutorial 2 if you want to ensure tasks aren't lost e.g. durable Q's and persistent messages
    businessChannel.get.queueDeclare(Config.RABBITMQ_Q_BUSINESS, false, false, false, null)//durable, exclusive, auto-delete.
    businessChannel.get.queueBind(Config.RABBITMQ_Q_BUSINESS, Config.RABBITMQ_EXCHANGE_BUSINESS, Config.RABBITMQ_ROUTINGKEY_BUSINESS)

    log.info("Connected to RabbitMQ!")
  }

  // Called when Actor terminates, having terminated its children.
  override def postStop() {
    // 'foreach' applies the function to the Options value if its non-empty (!= None).
    businessChannel foreach (ch => ch.close()) //Good practice! But not necessary if the connection is being closed.
    simulationChannel foreach (ch => ch.close())
    conn foreach (co => co.close())

    // Let's be really neat!
    businessChannel = None
    simulationChannel = None
    conn = None

  }

  /**
   * Nice way to encapsulate access to configuration data.
   */
  object Config {
    val RABBITMQ_HOST = ConfigFactory.load().getString("basketGenerator.rabbitmq.host")
    val RABBITMQ_EXCHANGE_BUSINESS = ConfigFactory.load().getString("basketGenerator.rabbitmq.exchange.business")
    val RABBITMQ_EXCHANGE_SIMULATION = ConfigFactory.load().getString("basketGenerator.rabbitmq.exchange.simulation")
    val RABBITMQ_Q_BUSINESS = ConfigFactory.load().getString("basketGenerator.rabbitmq.Q.business")
    val RABBITMQ_ROUTINGKEY_BUSINESS = ConfigFactory.load().getString("basketGenerator.rabbitmq.routingKey.business")
  }
}
