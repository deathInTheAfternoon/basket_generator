import akka.actor.{Props, ActorRef, ActorLogging, Actor}
import com.typesafe.config.ConfigFactory
import com.rabbitmq.client.{AMQP, Channel}
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
case object SimulationCompleted extends SimulatorEvent
case object SimulationStarted extends SimulatorEvent
case class BusinessEvent(message: Map[String, AnyRef]) extends SimulatorEvent

class RabbitMQIntegration extends Actor with ActorLogging {


  private[this] var conn: Option[com.rabbitmq.client.Connection] = None
  private[this] var chan: Option[com.rabbitmq.client.Channel] = None

  def receive = {
    case SimulationStarted =>
      log.info("Received Simulation Event.")
    case BusinessEvent(message) =>
      basicPublish(message)
  }

  def basicPublish(message: Map[String, AnyRef]) {
    // We call .toMap to convert from Mutable to Immutable map.
    val builder = new AMQP.BasicProperties.Builder
    // It's important to set the contentType property - otherwise node-amqp will see bytes instead of JSON.
    chan.get.basicPublish(Config.RABBITMQ_EXCHANGE, Config.RABBITMQ_Q_BUSINESS, builder.contentType("application/json").build(), JSONObject(message).toString().getBytes);
  }
  // Called before Actor starts accepting messages.
  override def preStart() {

    // Set up RabbitMQ
    conn = Some(RabbitMQConnection.getConnection())
    chan = conn map {
      c => c.createChannel()
    }

    chan.get.exchangeDeclare(Config.RABBITMQ_EXCHANGE, "direct", false)
    chan.get.queueDeclare(Config.RABBITMQ_Q_BUSINESS, false, false, false, null)
    chan.get.queueBind(Config.RABBITMQ_Q_BUSINESS, Config.RABBITMQ_EXCHANGE, Config.RABBITMQ_ROUTINGKEY_BUSINESS)

    log.info("Connected to RabbitMQ!")
  }

  // Called when Actor terminates, having terminated its children.
  override def postStop() {
    // 'foreach' applies the function to the Options value if its non-empty (!= None).

    //Rabbit
    log.info("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    chan foreach (ch => ch.close()) //Good practice! But not necessary if the connection is being closed.
    conn foreach (co => co.close())

    // Let's be really neat!
    chan = None
    conn = None

  }

  /**
   * Nice way to encapsulate access to configuration data.
   */
  object Config {
    val RABBITMQ_EXCHANGE = ConfigFactory.load().getString("basketGenerator.rabbitmq.exchange")
    val RABBITMQ_Q_SIMULATION = ConfigFactory.load().getString("basketGenerator.rabbitmq.Q.simulation")
    val RABBITMQ_Q_BUSINESS = ConfigFactory.load().getString("basketGenerator.rabbitmq.Q.business")
    val RABBITMQ_ROUTINGKEY_SIMULATION = ConfigFactory.load().getString("basketGenerator.rabbitmq.routingKey.simulation")
    val RABBITMQ_ROUTINGKEY_BUSINESS = ConfigFactory.load().getString("basketGenerator.rabbitmq.routingKey.business")
  }
}
