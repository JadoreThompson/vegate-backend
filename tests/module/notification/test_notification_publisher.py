from uuid import uuid4

import pytest
from sqlalchemy import select

from core.db import get_db_session
from module.notification import Notification as NotificationModel
from module.notification.enums import NotificationStatus, NotificationType
from module.notification.publisher import NotificationPublisher
from module.notification.schema import (
    BacktestCapacityConstrainedNotificationContext,
    DeploymentCapacityConstrainedNotificationContext,
)
from module.util import create_user


@pytest.mark.asyncio(loop_scope="session")
async def test_publish_creates_notification_with_correct_values():
    user = await create_user("test_pub_user_1")

    publisher = NotificationPublisher()
    deployment_id = uuid4()
    context = DeploymentCapacityConstrainedNotificationContext(
        deployment_id=deployment_id
    )

    await publisher.publish(
        user_id=user.id,
        type=NotificationType.DEPLOYMENT_CAPACITY_CONSTRAINED,
        context=context,
    )

    async with get_db_session() as db_sess:
        result = await db_sess.execute(
            select(NotificationModel).where(NotificationModel.user_id == user.id)
        )
        notification = result.scalar_one()

    assert notification.user_id == user.id
    assert notification.type == NotificationType.DEPLOYMENT_CAPACITY_CONSTRAINED.value
    assert notification.context == {"deployment_id": str(deployment_id)}
    assert notification.channel_type == "email"
    assert notification.status == NotificationStatus.PENDING
    assert notification.id is not None
    assert notification.created_at is not None
    assert notification.last_attempted_at is None


@pytest.mark.asyncio(loop_scope="session")
async def test_publish_creates_backtest_notification():
    user = await create_user("test_pub_user_2")

    publisher = NotificationPublisher()
    backtest_id = uuid4()
    context = BacktestCapacityConstrainedNotificationContext(backtest_id=backtest_id)

    await publisher.publish(
        user_id=user.id,
        type=NotificationType.BACKTEST_CAPACITY_CONSTRAINED,
        context=context,
    )

    async with get_db_session() as db_sess:
        result = await db_sess.execute(
            select(NotificationModel).where(NotificationModel.user_id == user.id)
        )
        notification = result.scalar_one()

    assert notification.user_id == user.id
    assert notification.type == NotificationType.BACKTEST_CAPACITY_CONSTRAINED.value
    assert notification.context == {"backtest_id": str(backtest_id)}
    assert notification.channel_type == "email"
    assert notification.status == NotificationStatus.PENDING
