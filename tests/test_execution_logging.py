"""日志模块测试"""

import pytest
from datetime import datetime
from data_converge.execution_logging import ExecutionLogger


class TestExecutionLogger:
    """执行日志测试类"""

    def test_log_task_start(self):
        """记录任务开始"""
        logger = ExecutionLogger()

        logger.log_task_start(
            task_id="task1",
            task_name="gpu_5min",
            context={"window_time": "2026-08-27 10:00:00"}
        )

        state = logger.get_task_state("task1")
        assert state is not None
        assert state["status"] == "running"

    def test_log_task_complete(self):
        """记录任务完成"""
        logger = ExecutionLogger()

        logger.log_task_start("task1", "gpu_5min")
        logger.log_task_complete("task1", success=True, message="Completed")

        state = logger.get_task_state("task1")
        assert state["status"] == "completed"
        assert state["success"] is True

    def test_log_task_failure(self):
        """记录任务失败"""
        logger = ExecutionLogger()

        logger.log_task_start("task1", "gpu_5min")
        logger.log_task_failure("task1", error="SQL execution error")

        state = logger.get_task_state("task1")
        assert state["status"] == "failed"
        assert state["error"] == "SQL execution error"

    def test_get_task_history(self):
        """获取任务历史"""
        logger = ExecutionLogger()

        logger.log_task_start("task1", "gpu_5min")
        logger.log_task_complete("task1")
        logger.log_task_start("task2", "gpu_hour")
        logger.log_task_complete("task2")

        history = logger.get_task_history()
        assert len(history) == 2

    def test_cleanup_old_logs(self):
        """清理旧日志"""
        logger = ExecutionLogger()

        logger.log_task_start("old_task", "gpu_5min")
        logger.log_task_complete("old_task")

        deleted = logger.cleanup_old_logs(max_age_days=0)
        assert deleted >= 1

    def test_get_dag_execution_summary(self):
        """获取DAG执行摘要"""
        logger = ExecutionLogger()

        logger.log_task_start("task1", "gpu_5min")
        logger.log_task_complete("task1")
        logger.log_task_start("task2", "gpu_hour")
        logger.log_task_failure("task2", error="Timeout")

        summary = logger.get_dag_execution_summary("dag1")
        assert summary["total"] == 2
        assert summary["completed"] == 1
        assert summary["failed"] == 1
