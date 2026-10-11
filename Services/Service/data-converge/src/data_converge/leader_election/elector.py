"""选主模块 - Redis实现主备选举"""

import redis
from typing import Optional


class LeaderElector:
    """Redis主备选举器"""

    LEADER_KEY_PREFIX = "data_converge:leader:"

    def __init__(self, cluster_name: str, redis_host: str = "localhost", redis_port: int = 6379):
        """初始化选举器

        Args:
            cluster_name: 集群名称
            redis_host: Redis主机地址
            redis_port: Redis端口
        """
        self.cluster_name = cluster_name
        self.leader_key = f"{self.LEADER_KEY_PREFIX}{cluster_name}"
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    def elect_as_leader(self, node_id: str, ttl: int = 30) -> bool:
        """尝试选举为主节点

        Args:
            node_id: 节点ID
            ttl: 主节点有效期（秒）

        Returns:
            True表示选举成功，False表示已有主节点
        """
        current_leader = self.redis_client.get(self.leader_key)
        if current_leader is not None:
            return False

        success = self.redis_client.set(self.leader_key, node_id, ex=ttl, nx=True)
        return success is not None

    def is_leader(self, node_id: str) -> bool:
        """检查当前节点是否为主节点

        Args:
            node_id: 节点ID

        Returns:
            True表示是主节点
        """
        current_leader = self.redis_client.get(self.leader_key)
        return current_leader == node_id

    def release_leadership(self, node_id: str) -> bool:
        """释放主节点资格

        Args:
            node_id: 节点ID

        Returns:
            True表示释放成功
        """
        current_leader = self.redis_client.get(self.leader_key)
        if current_leader != node_id:
            return False

        self.redis_client.delete(self.leader_key)
        return True

    def renew_leadership(self, node_id: str, ttl: int = 30) -> bool:
        """续约主节点

        Args:
            node_id: 节点ID
            ttl: 新的有效期（秒）

        Returns:
            True表示续约成功
        """
        current_leader = self.redis_client.get(self.leader_key)
        if current_leader != node_id:
            return False

        return self.redis_client.expire(self.leader_key, ttl)

    def get_current_leader(self) -> Optional[str]:
        """获取当前主节点ID

        Returns:
            主节点ID，不存在返回None
        """
        return self.redis_client.get(self.leader_key)

    def get_leader_ttl(self) -> int:
        """获取主节点剩余TTL

        Returns:
            剩余秒数，-1表示无TTL，-2表示键不存在
        """
        return self.redis_client.ttl(self.leader_key)
