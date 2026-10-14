"""选主模块 - Redis实现主备选举"""

__all__ = ["LeaderElector"]

from .elector import LeaderElector
