<?php

namespace go1\util_index;

use go1\util_index\task\Task;

interface ReindexInterface
{
    /**
     * @param Task $task
     * @return int
     */
    public function handle(Task $task);

    public function count(Task $task);
}
