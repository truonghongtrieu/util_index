<?php

namespace go1\util_index;

use go1\util_index\task\Task;

interface ReindexInterface
{
    public function handle(Task $task): int;

    public function count(Task $task): int;
}
