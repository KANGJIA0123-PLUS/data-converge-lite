"""选主模块测试"""

import pytest
from unittest.mock import Mock, patch
from data_converge.leader_election import LeaderElector


class TestLeaderElector:
    """选主模块测试类"""

    def test_elect_as_leader_when_no_leader_exists(self):
        """当无主节点时，应该成功选举为主"""
        with patch("redis.Redis") as mock_redis:
            mock_redis_instance = Mock()
            mock_redis.return_value = mock_redis_instance
            mock_redis_instance.get.return_value = None
            mock_redis_instance.set.return_value = True

            elector = LeaderElector("test_cluster")
            is_leader = elector.elect_as_leader("node1", ttl=30)

            assert is_leader is True

    def test_fail_to_elect_when_leader_exists(self):
        """当主节点已存在时，应该选举失败"""
        with patch("redis.Redis") as mock_redis:
            mock_redis_instance = Mock()
            mock_redis.return_value = mock_redis_instance
            mock_redis_instance.get.return_value = "node2"

            elector = LeaderElector("test_cluster")
            is_leader = elector.elect_as_leader("node1", ttl=30)

            assert is_leader is False

    def test_release_leadership(self):
        """释放主节点资格"""
        with patch("redis.Redis") as mock_redis:
            mock_redis_instance = Mock()
            mock_redis.return_value = mock_redis_instance
            mock_redis_instance.get.return_value = "node1"
            mock_redis_instance.delete.return_value = 1

            elector = LeaderElector("test_cluster")
            elector.release_leadership("node1")

            mock_redis_instance.delete.assert_called_once()

    def test_is_leader(self):
        """检查当前节点是否为主"""
        with patch("redis.Redis") as mock_redis:
            mock_redis_instance = Mock()
            mock_redis.return_value = mock_redis_instance
            mock_redis_instance.get.return_value = "node1"

            elector = LeaderElector("test_cluster")
            result = elector.is_leader("node1")

            assert result is True

    def test_is_not_leader_when_different_node_leads(self):
        """当其他节点为主时，当前节点不是主"""
        with patch("redis.Redis") as mock_redis:
            mock_redis_instance = Mock()
            mock_redis.return_value = mock_redis_instance
            mock_redis_instance.get.return_value = "node2"

            elector = LeaderElector("test_cluster")
            result = elector.is_leader("node1")

            assert result is False

    def test_renew_leadership_success(self):
        """续约主节点成功"""
        with patch("redis.Redis") as mock_redis:
            mock_redis_instance = Mock()
            mock_redis.return_value = mock_redis_instance
            mock_redis_instance.get.return_value = "node1"
            mock_redis_instance.expire.return_value = True

            elector = LeaderElector("test_cluster")
            result = elector.renew_leadership("node1", ttl=30)

            assert result is True
