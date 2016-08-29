<?php

namespace talma\amqp\controllers;

use Yii;
use yii\console\Controller;

/**
 * Class ListenerManageController
 */
class ListenerManageController extends Controller
{
    /* @var string */
    public $interpreterPath;

    /**
     * @var string Bootstrap script path (if empty, current command runner will be used)
     */
    public $bootstrapScript;

    /**
     * @var array Queue need to keep and count
     *
     * example:
     * 'configs' => [
     *     ['queue' => 'market.uber.recall', 'count' => 2]
     * ]
     */
    public $configs;

    /**
     * @inheritdoc
     */
    public function beforeAction($action)
    {
        $this->interpreterPath = Yii::$app->controllerMap['cron']['interpreterPath'];
        $this->bootstrapScript = Yii::$app->controllerMap['cron']['bootstrapScript'];

        return parent::beforeAction($action);
    }

    /**
     * Keep listener processes running
     */
    public function actionKeep()
    {
        foreach ($this->configs as $config) {
            $this->keep($config['queue'], $config['count']);
        }
    }

    /**
     * @param $queue
     * @param $count
     *
     * @return bool
     */
    protected function keep($queue, $count)
    {
        $command = "ps aux | grep '$this->interpreterPath $this->bootstrapScript listener --queue=$queue' | grep -v 'grep' | wc -l";
        echo $command . PHP_EOL;
        $nowCount = shell_exec($command);
        echo "number of processes :" . (int)$nowCount . PHP_EOL;
        $add = $count - $nowCount;
        if ($add <= 0) {
            return false;
        }
        $command = "$this->interpreterPath $this->bootstrapScript listener --queue=$queue";
        $this->runCommandBackground($command, $add);

        return true;
    }

    /**
     * kill linster processes
     */
    public function actionKill()
    {
        $command = "ps aux | grep '$this->interpreterPath $this->bootstrapScript listener ' | grep -v 'grep' |  awk -F ' ' '{print $2}'";
        echo $command . PHP_EOL;
        $result = shell_exec($command);
        $data = explode(PHP_EOL, $result);
        foreach ($data as $pid) {
            if (!empty((int)$pid)) {
                shell_exec('kill ' . (int)$pid);
                echo "killed pid: $pid" . PHP_EOL;
            }
        }
    }

    /**
     * @return bool
     */
    protected function isWindowsOS()
    {
        return strncmp(PHP_OS, 'WIN', 3) === 0;
    }

    /**
     * @param $command
     * @param $count
     */
    protected function runCommandBackground($command, $count)
    {
        for ($i = 0; $i < $count; $i++) {
            if ($this->isWindowsOS()) {
                //Windows OS
                pclose(popen('start /B "Yii run command" ' . $command, 'r'));
            } else {
                //nix based OS
                system($command . ' > /dev/null 2>&1 &');
                echo "start : $command" . PHP_EOL;
            }
        }
    }
}
