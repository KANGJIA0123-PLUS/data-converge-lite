"""日志模块 - 执行状态记录和日志老化"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field


@dataclass
class TaskState:
    """任务状态记录"""
    task_id: str
    task_name: str
    status: str
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    success: Optional[bool] = None
    message: Optional[str] = None
    error: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)


class ExecutionLogger:
    """执行日志记录器"""

    def __init__(self, max_history: int = 1000):
        """初始化日志记录器

        Args:
            max_history: 最大历史记录数
        """
        self.max_history = max_history
        self._states: Dict[str, TaskState] = {}
        self._history: List[TaskState] = []

    def log_task_start(self, task_id: str, task_name: str, context: Optional[Dict[str, Any]] = None) -> None:
        """记录任务开始

        Args:
            task_id: 任务ID
            task_name: 任务名称
            context: 执行上下文
        """
        state = TaskState(
            task_id=task_id,
            task_name=task_name,
            status="running",
            context=context or {}
        )
        self._states[task_id] = state

    def log_task_complete(self, task_id: str, success: bool = True, message: Optional[str] = None) -> None:
        """记录任务完成

        Args:
            task_id: 任务ID
            success: 是否成功
            message: 消息
        """
        if task_id not in self._states:
            return

        state = self._states[task_id]
        state.status = "completed" if success else "failed"
        state.success = success
        state.end_time = datetime.now()
        state.message = message

        self._add_to_history(state)

    def log_task_failure(self, task_id: str, error: str) -> None:
        """记录任务失败

        Args:
            task_id: 任务ID
            error: 错误信息
        """
        if task_id not in self._states:
            return

        state = self._states[task_id]
        state.status = "failed"
        state.success = False
        state.error = error
        state.end_time = datetime.now()

        self._add_to_history(state)

    def _add_to_history(self, state: TaskState) -> None:
        """添加到历史记录

        Args:
            state: 任务状态
        """
        self._history.append(state)
        if len(self._history) > self.max_history:
            self._history.pop(0)

    def get_task_state(self, task_id: str) -> Optional[Dict[str, Any]]:
        """获取任务状态

        Args:
            task_id: 任务ID

        Returns:
            状态字典
        """
        state = self._states.get(task_id)
        if state is None:
            return None

        return {
            "task_id": state.task_id,
            "task_name": state.task_name,
            "status": state.status,
            "start_time": state.start_time.isoformat() if state.start_time else None,
            "end_time": state.end_time.isoformat() if state.end_time else None,
            "success": state.success,
            "message": state.message,
            "error": state.error,
            "context": state.context
        }

    def get_task_history(self) -> List[Dict[str, Any]]:
        """获取任务历史

        Returns:
            历史记录列表
        """
        return [
            {
                "task_id": s.task_id,
                "task_name": s.task_name,
                "status": s.status,
                "start_time": s.start_time.isoformat() if s.start_time else None,
                "end_time": s.end_time.isoformat() if s.end_time else None,
                "success": s.success,
                "error": s.error
            }
            for s in self._history
        ]

    def cleanup_old_logs(self, max_age_days: int = 7) -> int:
        """清理旧日志

        Args:
            max_age_days: 最大保留天数

        Returns:
            删除的记录数
        """
        cutoff_time = datetime.now() - timedelta(days=max_age_days)
        original_length = len(self._history)

        self._history = [
            state for state in self._history
            if state.start_time and state.start_time > cutoff_time
        ]

        deleted = original_length - len(self._history)

        stale_states = [
            task_id for task_id, state in self._states.items()
            if state.start_time and state.start_time < cutoff_time
        ]
        for task_id in stale_states:
            del self._states[task_id]

        return deleted

    def get_dag_execution_summary(self, dag_id: str) -> Dict[str, Any]:
        """获取DAG执行摘要

        Args:
            dag_id: DAG ID

        Returns:
            执行摘要
        """
        dag_tasks = [s for s in self._history if s.task_name.startswith(dag_id) or s.context.get("dag_id") == dag_id]

        if not dag_tasks:
            dag_tasks = self._history

        summary = {
            "total": len(dag_tasks),
            "completed": sum(1 for s in dag_tasks if s.status == "completed"),
            "failed": sum(1 for s in dag_tasks if s.status == "failed"),
            "running": sum(1 for s in dag_tasks if s.status == "running"),
        }

        summary["success_rate"] = summary["completed"] / summary["total"] if summary["total"] > 0 else 0

        return summary

    def get_running_tasks(self) -> List[Dict[str, Any]]:
        """获取正在运行的任务

        Returns:
            运行中的任务列表
        """
        return [
            self.get_task_state(task_id)
            for task_id, state in self._states.items()
            if state.status == "running"
        ]
