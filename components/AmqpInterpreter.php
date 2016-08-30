<?php

namespace talma\amqp\components;

use yii\helpers\Console;

/**
 * AMQP interpreter class.
 */
class AmqpInterpreter
{
    const MESSAGE_INFO = 0;
    const MESSAGE_ERROR = 1;

    /* @var boolean */
    public $debug;

    /**
     * Logs info and error messages.
     *
     * @param $message
     * @param $type
     */
    public static function log($message, $type = self::MESSAGE_INFO)
    {
        $format = [$type == self::MESSAGE_ERROR ? Console::FG_RED : Console::FG_BLUE];
        Console::stdout(Console::ansiFormat(date('Y-m-d H:i:s') . PHP_EOL . $message . PHP_EOL, $format));
    }

    /**
     * Debug messages.
     *
     * @param $message
     */
    public function debug($message)
    {
        if ($this->debug) {
            Console::stdout(Console::ansiFormat($message . PHP_EOL, [Console::FG_GREEN]));
        }
    }
}
