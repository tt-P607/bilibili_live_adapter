"""Bilibili adapter 发送回执回归测试。"""

from __future__ import annotations

import pytest

from plugins.bilibili_live_adapter.plugin import BilibiliLiveAdapter
from src.core.components.types import PlatformSendStatus


@pytest.mark.asyncio
async def test_live_adapter_explicitly_rejects_outbound() -> None:
    """直播 adapter 不支持发弹幕时返回明确 FAILED 结果。"""
    adapter = object.__new__(BilibiliLiveAdapter)

    result = await adapter._send_platform_message(
        {"message_segment": [{"type": "text", "data": "hello"}]}
    )

    assert result.status is PlatformSendStatus.FAILED
    assert result.success is False
    assert "B 站直播适配器仅支持入站" in str(result.error)
