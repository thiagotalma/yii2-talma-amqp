<?php

namespace talma\amqp\components;

use yii\log\FileTarget;

/**
 * Class AmqpLog
 */
class AmqpLog extends FileTarget
{
    /** {@inheritdoc} */
    public $logFile = '@runtime/logs/amqp.log';

    /** {@inheritdoc} */
    public $categories = [Amqp::LOG_CATEGORY];

    /** {@inheritdoc} */
    public $logVars = [];
}
