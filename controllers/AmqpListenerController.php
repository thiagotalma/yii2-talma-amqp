<?php

namespace talma\amqp\controllers;

use PhpAmqpLib\Message\AMQPMessage;
use talma\amqp\components\Amqp;
use talma\amqp\components\AmqpInterpreter;
use talma\amqp\components\AmqpTrait;
use yii\console\Controller;
use yii\console\Exception;
use yii\helpers\Inflector;
use yii\helpers\Json;

/**
 * AMQP listener controller
 */
class AmqpListenerController extends Controller
{
    use AmqpTrait;

    /**
     * @inheritdoc
     */
    public $defaultAction = 'run';

    /* @var boolean */
    public $debug;

    /* @var AmqpInterpreter */
    protected $interpreter;

    /**
     * Interpreter classes for AMQP messages. This class will be used if interpreter class not set for queue.
     *
     * @var array
     */
    public $interpreters = [];

    /**
     * Consumer tag
     *
     * @var string
     */
    protected $consumerTag = 'consumer';

    /**
     * @inheritdoc
     */
    public function options($actionId)
    {
        return array_merge(
            parent::options($actionId),
            ['exchange', 'queue', 'debug']
        );
    }

    /**
     * Listener for queue
     */
    public function actionRun()
    {
        $this->amqp->listen($this->queue, [$this, 'callback'], $this->consumerTag);
    }

    /**
     * @inheritDoc
     */
    public function init()
    {
        parent::init();

        if (class_exists($this->interpreters[$this->queue])) {
            $instance = new $this->interpreters[$this->queue];
            if ($instance instanceof AmqpInterpreter) {
                $instance->debug = $this->debug;
                $this->interpreter = $instance;
            }
        }

        if (!$this->interpreter) {
            $this->interpreter = new AmqpInterpreter();
        }

        if (extension_loaded('pcntl')) {
            define('AMQP_WITHOUT_SIGNALS', false);
            pcntl_signal(SIGTERM, [$this, 'signalHandler']);
            pcntl_signal(SIGHUP, [$this, 'signalHandler']);
            pcntl_signal(SIGINT, [$this, 'signalHandler']);
            pcntl_signal(SIGQUIT, [$this, 'signalHandler']);
            pcntl_signal(SIGUSR1, [$this, 'signalHandler']);
            pcntl_signal(SIGUSR2, [$this, 'signalHandler']);
            pcntl_signal(SIGALRM, [$this, 'alarmHandler']);
        } else {
            echo 'Unable to process signals.' . PHP_EOL;
            exit(1);
        }
    }

    /**
     * @param AMQPMessage $msg
     *
     * @throws Exception
     */
    public function callback(AMQPMessage $msg)
    {
        $exchange = $msg->get('exchange');
        $routingKey = $msg->get('routing_key');
        $channel = $msg->get('channel');
        $deliveryTag = $msg->get('delivery_tag');

        // Send a message with the string "quit" to cancel the consumer.
        if ($msg->body === 'quit') {
            $channel->basic_cancel($msg->delivery_info['consumer_tag']);

            return;
        }

        $method = 'read' . Inflector::camelize($routingKey);
        $msgBody = null;

        try {
            $msgBody = Json::decode($msg->body, true);
        } catch (\Exception $e) {
            $errorInfo = 'Invalid or malformed JSON.' . PHP_EOL . $e->getMessage();
            $this->fail($errorInfo, __METHOD__, $e);
        }

        if (method_exists($this->interpreter, $method)) {
            if ($msgBody) {
                $ack = false;
                $info = [
                    'exchange' => $exchange,
                    'queue' => $this->queue,
                    'routing_key' => $routingKey,
                    'reply_to' => $msg->has('reply_to') ? $msg->get('reply_to') : null,
                ];

                try {
                    // Do the Job
                    $ack = $this->interpreter->$method($msgBody, $info);
                    if ($ack) {
                        $channel->basic_ack($deliveryTag);
                    }
                    Amqp::log($exchange, $routingKey, $msg, __METHOD__, ['result' => $ack ? 'success' : 'error']);
                } catch (\Exception $exc) {
                    $errorInfo = "consumer fail:" . $exc->getMessage()
                        . PHP_EOL . "info:" . print_r($info, true)
                        . PHP_EOL . "body:" . PHP_EOL . print_r($msg->body, true);
                    $this->fail($errorInfo, __METHOD__, $exc);
                }

                if (!$ack) {
                    $channel->basic_nack($deliveryTag);
                }
            }
        } else {
            $errorInfo = "Unknown routing key '$routingKey'.";
            $errorInfo .= PHP_EOL . 'Interpreter: ' . get_class($this->interpreter);
            $errorInfo .= PHP_EOL . 'Exchange: ' . $exchange;
            $errorInfo .= PHP_EOL . 'Queue: ' . $this->queue;
            $errorInfo .= PHP_EOL . 'Body: ' . $msg->body;

            $this->fail($errorInfo, __METHOD__);
        }

        $this->interpreter->debug(print_r($msgBody, true));
    }

    /**
     * @param $errorInfo
     * @param $method
     * @param $exception Exception
     */
    protected function fail($errorInfo, $method, $exception = null)
    {
        if ($exception) {
            $errorInfo .= PHP_EOL . $exception->getTraceAsString() . PHP_EOL;
        }

        \Yii::warning($errorInfo, $method);
        \Yii::$app->log->logger->flush(true);

        $this->interpreter->log($errorInfo, AmqpInterpreter::MESSAGE_ERROR);
    }

    /**
     * Signal handler
     *
     * @param  int $signalNumber
     *
     * @return void
     */
    public function signalHandler($signalNumber)
    {
        echo 'Handling signal: #' . $signalNumber . PHP_EOL;
        switch ($signalNumber) {
            case SIGTERM:  // 15 : supervisor default stop
            case SIGQUIT:  // 3  : kill -s QUIT
                $this->stopHard();
                break;
            case SIGINT:   // 2  : ctrl+c
                $this->stop();
                break;
            case SIGHUP:   // 1  : kill -s HUP
                $this->restart();
                break;
            case SIGUSR1:  // 10 : kill -s USR1
                // send an alarm in 1 second
                pcntl_alarm(1);
                break;
            case SIGUSR2:  // 12 : kill -s USR2
                // send an alarm in 10 seconds
                pcntl_alarm(10);
                break;
            default:
                break;
        }

        return;
    }

    /**
     * Alarm handler
     *
     * @param  int $signalNumber
     *
     * @return void
     */
    public function alarmHandler($signalNumber)
    {
        echo 'Handling alarm: #' . $signalNumber . PHP_EOL;
        echo memory_get_usage(true) . PHP_EOL;

        return;
    }

    /**
     * Restart the consumer on an existing connection
     */
    public function restart()
    {
        echo 'Restarting consumer.' . PHP_EOL;
        $this->stopSoft();
        $this->amqp->listen($this->queue, [$this, 'callback']);
    }

    /**
     * Close the connection to the server
     */
    public function stopHard()
    {
        echo 'Stopping consumer by closing connection.' . PHP_EOL;
        $this->amqp->connection->close();
    }

    /**
     * Close the channel to the server
     */
    public function stopSoft()
    {
        echo 'Stopping consumer by closing channel.' . PHP_EOL;
        $this->amqp->channel->close();
    }

    /**
     * Tell the server you are going to stop consuming
     * It will finish up the last message and not send you any more
     */
    public function stop()
    {
        echo 'Stopping consumer by cancel command.' . PHP_EOL;
        // this gets stuck and will not exit without the last two parameters set
        $this->amqp->channel->basic_cancel($this->consumerTag, false, true);
    }
}
