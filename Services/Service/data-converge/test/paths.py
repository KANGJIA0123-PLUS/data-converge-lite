"""Repository paths shared by source and company bytecode-build tests."""

from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
SERVICE_ROOT = REPOSITORY_ROOT / 'Services/Service/data-converge'
SOURCE_ROOT = SERVICE_ROOT / 'src'
RESOURCES_ROOT = SERVICE_ROOT / 'resources'
TEST_ROOT = SERVICE_ROOT / 'test'
