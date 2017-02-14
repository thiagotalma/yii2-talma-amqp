<?php

namespace talma\amqp\components;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Message\AMQPMessage;
use Yii;

/**
 * AMQP trait for controllers.
 *
 * @property Amqp $amqp AMQP object.
 * @property AMQPConnection $connection AMQP connection.
 * @property AMQPChannel $channel AMQP channel.
 */
trait AmqpTrait
{
    /**
     * @var Amqp
     */
    protected $amqpContainer;

    /**
     * Listened exchange.
     *
     * @var string
     */
    public $exchange;

    /**
     * Listened Queue.
     *
     * @var string
     */
    public $queue;

    /**
     * Returns AMQP object.
     *
     * @return Amqp
     */
    public function getAmqp()
    {
        if (empty($this->amqpContainer)) {
            $this->amqpContainer = Yii::$app->amqp;
        }

        return $this->amqpContainer;
    }

    /**
     * Returns AMQP connection.
     *
     * @return AMQPConnection
     */
    public function getConnection()
    {
        return $this->amqp->getConnection();
    }

    /**
     * Returns AMQP channel.
     *
     * @param string $channel_id
     *
     * @return AMQPChannel
     */
    public function getChannel($channel_id = null)
    {
        return $this->amqp->getChannel($channel_id);
    }

    /**
     * Sends message to the exchange.
     *
     * @param string $routing_key
     * @param string|array|AMQPMessage $message
     * @param string $exchange
     * @param string $type
     *
     * @return void
     */
    public function send($routing_key, $message, $exchange = null, $type = Amqp::TYPE_TOPIC)
    {
        $this->amqp->send($exchange ?: $this->exchange, $routing_key, $message, $type);
    }
    
    /**
     * @param $routing_key
     * @param array $message
     * @param $exchange
     * @param int $delay time in milliseconds
     *
     * @return void
     */
    public function sendDelay($routing_key, $message, $exchange, $delay)
    {
        $headers = new AMQPTable(['x-delay' => $delay]);
        $amqpMessage = new AMQPMessage(Json::encode($message));
        $amqpMessage->set('application_headers', $headers);

        $this->amqp->channel->basic_publish($amqpMessage, $exchange, $routing_key);
    }

    /**
     * Sends message to the exchange and waits for answer.
     *
     * @param string $queue
     * @param string $exchange
     * @param string $routing_key
     * @param string|array|AMQPMessage $message
     * @param int $timeout
     *
     * @return string
     */
    public function ask($queue, $exchange, $routing_key, $message, $timeout = 10)
    {
        return $this->amqp->ask($queue, $exchange, $routing_key, $message, $timeout);
    }
}
