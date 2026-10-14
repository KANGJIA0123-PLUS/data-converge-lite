"""Compatibility exports; Redis platform adapters live in infrastructure.redis.auth."""

__all__ = ['make_connect_callback']

from data_converge.infrastructure.redis.auth import (
    make_connect_callback,
)
