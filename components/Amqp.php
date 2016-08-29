<?php
namespace talma\amqp\components;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use yii\base\Component;
use yii\base\Exception;
use yii\helpers\Json;

/**
 * AMQP wrapper.
 *
 * @property AMQPStreamConnection $connection AMQP connection.
 * @property AMQPChannel $channel AMQP channel.
 *
 * @SuppressWarnings(CamelCaseMethodName)
 */
class Amqp extends Component
{
    const TYPE_TOPIC = 'topic';
    const TYPE_DIRECT = 'direct';
    const TYPE_HEADERS = 'headers';
    const TYPE_FANOUT = 'fanout';

    /**
     * @var AMQPStreamConnection
     */
    protected static $ampqConnection;

    /**
     * @var AMQPChannel[]
     */
    protected $channels = [];

    /**
     * @var string
     */
    public $host = '127.0.0.1';

    /**
     * @var integer
     */
    public $port = 5672;

    /**
     * @var string
     */
    public $user;

    /**
     * @var string
     */
    public $password;

    /**
     * @var string
     */
    public $vhost = '/';

    /* @var array */
    public $exchange_configs = [];

    /* @var array */
    public $queue_configs = [];

    /**
     * @inheritdoc
     */
    public function init()
    {
        parent::init();
        if (empty($this->user)) {
            throw new Exception("Parameter 'user' was not set for AMQP connection.");
        }
        if (empty(self::$ampqConnection)) {
            self::$ampqConnection = new AMQPStreamConnection(
                $this->host,
                $this->port,
                $this->user,
                $this->password,
                $this->vhost
            );
        }
    }

    /**
     * Returns AMQP connection.
     *
     * @return AMQPStreamConnection
     */
    public function getConnection()
    {
        return self::$ampqConnection;
    }

    /**
     * Returns AMQP connection.
     *
     * @param string $channel_id
     *
     * @return AMQPChannel
     */
    public function getChannel($channel_id = null)
    {
        $index = $channel_id ?: 'default';
        if (!array_key_exists($index, $this->channels)) {
            $this->channels[$index] = $this->connection->channel($channel_id);
        }

        return $this->channels[$index];
    }

    /**
     * @param $exchange
     * @param string $type
     */
    public function exchange_declare($exchange, $type = self::TYPE_TOPIC)
    {
        $config = $this->exchange_configs[$exchange];
        $this->channel->exchange_declare(
            $exchange,
            isset($config['options']['type']) ? $config['options']['type'] : $type,
            isset($config['options']['passive']) ? $config['options']['passive'] : false,
            isset($config['options']['durable']) ? $config['options']['durable'] : true,
            isset($config['options']['auto_delete']) ? $config['options']['auto_delete'] : true
        );
    }

    /**
     * @param $queue
     *
     * @return mixed
     */
    public function queue_declare($queue)
    {
        $config = $this->queue_configs[$queue];
        list ($queueName) = $this->channel->queue_declare(
            $queue,
            isset($config['options']['passive']) ? $config['options']['passive'] : false,
            isset($config['options']['durable']) ? $config['options']['durable'] : true,
            isset($config['options']['exclusive']) ? $config['options']['exclusive'] : false,
            isset($config['options']['auto_delete']) ? $config['options']['auto_delete'] : true,
            isset($config['options']['nowait']) ? $config['options']['nowait'] : false,
            isset($config['arguments']) ? $config['arguments'] : null
        );

        return $queueName;
    }

    /**
     * Sends message to the exchange.
     *
     * @param string $exchange
     * @param string $routing_key
     * @param string|array $message
     * @param string $type Use self::TYPE_DIRECT if it is an answer
     *
     * @return void
     */
    public function send($exchange, $routing_key, $message, $type = self::TYPE_TOPIC)
    {
        $message = $this->prepareMessage($message);
        if ($type == self::TYPE_TOPIC) {
            $this->exchange_declare($exchange, $type);
        }
        $this->channel->basic_publish($message, $exchange, $routing_key);
    }

    /**
     * Sends message to the exchange and waits for answer.
     *
     * @param string $queue
     * @param string $exchange
     * @param string $routing_key
     * @param string|array $message
     * @param integer $timeout Timeout in seconds.
     *
     * @return string
     */
    public function ask($queue, $exchange, $routing_key, $message, $timeout)
    {
        $queueName = $this->queue_declare($queue);
        $message = $this->prepareMessage($message, [
            'reply_to' => $queueName,
        ]);
        // queue name must be used for answer's routing key
        $this->channel->queue_bind($queueName, $exchange, $queueName);

        $response = null;
        $callback = function (AMQPMessage $answer) use ($message, &$response) {
            $response = $answer->body;
        };

        $this->channel->basic_consume($queueName, '', false, false, false, false, $callback);
        $this->channel->basic_publish($message, $exchange, $routing_key);
        while (!$response) {
            // exception will be thrown on timeout
            $this->channel->wait(null, false, $timeout);
        }

        return $response;
    }

    /**
     * Listens the exchange for messages.
     *
     * @param string $queue
     * @param callable $callback
     *
     * @throws Exception
     */
    public function listen($queue, $callback)
    {
        if (!key_exists($queue, $this->queue_configs)) {
            throw new Exception('amqp queue: ' . $queue . ' no found.');
        }

        $queue_config = $this->queue_configs[$queue];
        $queueName = $this->queue_declare($queue);

        foreach ($queue_config['binds'] as $routing_key => $exchange) {
            if (!key_exists($exchange, $this->exchange_configs)) {
                throw new Exception('amqp exchange: ' . $exchange . ' no found.');
            }
            $this->exchange_declare($exchange);
            $this->channel->queue_bind($queueName, $exchange, $routing_key, false);
        }

        $this->channel->basic_consume(
            $queueName,
            isset($queue_config['consumerOptions']['consumer_tag']) ? $queue_config['consumerOptions']['consumer_tag'] : '',
            isset($queue_config['consumerOptions']['no_local']) ? $queue_config['consumerOptions']['no_local'] : false,
            false, // no_ack
            isset($queue_config['consumerOptions']['exclusive']) ? $queue_config['consumerOptions']['exclusive'] : false,
            isset($queue_config['consumerOptions']['nowait']) ? $queue_config['consumerOptions']['nowait'] : false,
            $callback,
            isset($queue_config['consumerOptions']['ticket']) ? $queue_config['consumerOptions']['ticket'] : null,
            isset($queue_config['consumerOptions']['argument']) ? $queue_config['consumerOptions']['argument'] : null
        );

        while (count($this->channel->callbacks)) {
            $this->channel->wait();
        }

        $this->channel->close();
        $this->connection->close();
    }

    /**
     * Returns prepaired AMQP message.
     *
     * @param string|array|object $message
     * @param array $properties
     *
     * @return AMQPMessage
     * @throws Exception If message is empty.
     */
    public function prepareMessage($message, $properties = null)
    {
        if (empty($message)) {
            throw new Exception('AMQP message can not be empty');
        }
        if (is_array($message) || is_object($message)) {
            $message = Json::encode($message);
        }

        return new AMQPMessage($message, $properties);
    }
}
