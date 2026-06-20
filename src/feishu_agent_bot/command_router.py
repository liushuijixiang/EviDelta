from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Command:
    name: str
    argument: str = ""


def parse_command(text: str) -> Command:
    stripped = text.strip().replace("／", "/")
    if not stripped.startswith("/"):
        return Command("text", stripped)
    parts = re.split(r"\s+", stripped, maxsplit=1)
    name = parts[0].split("@", 1)[0].lower()
    argument = parts[1].strip() if len(parts) == 2 else ""
    return Command(name, argument)


HELP_TEXT = """可用命令：
/help - 查看帮助
/ping - 检查服务状态
/research <主题> - 进入调研配置确认；回复 0 直接进行
/research <主题> --depth quick|standard|professional - 带显式配置时直接创建任务
/research --no-auto-retry <主题> - 创建调研任务，报告校验失败时不自动重试
/research <主题> --monitor-daily 09:00 --monitor-tz Asia/Shanghai - 完成后自动注册监测
/research <主题> --monitor-every 6h --monitor-notify high - 完成后按间隔监测
/status <任务ID> - 查询任务进度
/report latest <任务ID> - 查询当前研究报告
/report versions <任务ID> - 查询已有研究报告版本
/report resend <任务ID> [v版本] [pdf|xlsx|json] - 发送当前或指定版本报告文件
/update status [任务ID] - 查询待审批报告更新
/update approve <revision_id> - 发布待审批报告更新
/update reject <revision_id> - 拒绝待审批报告更新
/cancel <任务ID> - 取消自己创建的任务
/pause <任务ID> - 暂停自己创建的任务
/resume <任务ID> - 恢复自己暂停的任务
/monitor create <任务ID> daily 09:00 Asia/Shanghai - 创建持续监测计划
/monitor status <任务ID> - 查看监测状态
/monitor list - 列出自己的监测任务
/monitor pause|resume|run|update <任务ID> - 管理监测计划
/monitor cancel-current <任务ID> - 取消当前监测周期，不删除 Schedule
/monitor delete <任务ID> - 生成删除确认 token
/monitor delete-confirm <任务ID> <token> - 确认删除未来监测 Schedule
/monitor help - 查看监测命令

示例：/research 新能源汽车充电设备行业主要竞品"""
