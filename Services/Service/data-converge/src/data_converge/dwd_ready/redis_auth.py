"""Compatibility exports; Redis platform adapters live in infrastructure.redis.auth."""

from data_converge.infrastructure.redis.auth import (
    make_connect_callback,
)

__all__ = ['make_connect_callback']
