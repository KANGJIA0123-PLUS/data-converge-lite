"""Compatibility exports; Redis platform adapters live in infrastructure.redis.credentials."""

__all__ = ['SentinelCredentials', 'load_sentinel_credentials', 'parse_sentinel_credentials', 'sentinel_identity']

from data_converge.infrastructure.redis.credentials import (
    SentinelCredentials, load_sentinel_credentials, parse_sentinel_credentials, sentinel_identity,
)
