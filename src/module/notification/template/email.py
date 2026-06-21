from dataclasses import dataclass
from .base import NotificationTemplateEngine
from ..schema import (
    NotificationType,
    Notification,
    BacktestCapacityConstrainedNotificationContext,
    DeploymentCapacityConstrainedNotificationContext,
    DeploymentRunningNotificationContext,
)


@dataclass(frozen=True, slots=True)
class RenderedEmailTemplate:
    recipient: str
    subject: str
    body: str


class EmailNotificationTemplateEngine(
    NotificationTemplateEngine[RenderedEmailTemplate]
):

    def render(
        self, notification: Notification, recipient: str
    ) -> RenderedEmailTemplate:
        if notification.type == NotificationType.DEPLOYMENT_RUNNING:
            return self._render_deployment_running(notification, recipient)
        if notification.type == NotificationType.DEPLOYMENT_CAPACITY_CONSTRAINED:
            return self._render_deployment_capacity_constrained(notification, recipient)
        if notification.type == NotificationType.BACKTEST_CAPACITY_CONSTRAINED:
            return self._render_backtest_capacity_constrained(notification, recipient)
        raise ValueError(f"Unsupported notification type: {notification.type}")

    def _render_deployment_running(
        self, notification: Notification, recipient: str
    ) -> RenderedEmailTemplate:
        if not isinstance(
            notification.context, DeploymentRunningNotificationContext
        ):
            raise ValueError(
                "Invalid notification context type. Expected DeploymentRunningNotificationContext."
            )

        return RenderedEmailTemplate(
            recipient=recipient,
            subject="Deployment Is Now Running",
            body=(
                f"Dear {recipient},\n\n"
                f"Your deployment '{notification.context.deployment_id}' is now live and running.\n\n"
                "Best regards,\n"
                "Vega Team"
            ),
        )

    def _render_deployment_capacity_constrained(
        self, notification: Notification, recipient: str
    ) -> str:
        if not isinstance(
            notification.context, DeploymentCapacityConstrainedNotificationContext
        ):
            raise ValueError(
                "Invalid notification context type. Expected DeploymentCapacityConstrainedNotificationContext."
            )

        return RenderedEmailTemplate(
            recipient=recipient,
            subject="Deployment Capacity Constrained",
            body=(
                f"Dear {recipient},\n\n"
                f"Deployment capacity is currently constrained."
                f"Your deployment '{notification.context.deployment_id}' has been cancelled."
                f"Please take necessary actions.\n\n"
                "Best regards,\n"
                "Vega Team"
            ),
        )

    def _render_backtest_capacity_constrained(
        self, notification: Notification, recipient: str
    ) -> RenderedEmailTemplate:
        if not isinstance(
            notification.context, BacktestCapacityConstrainedNotificationContext
        ):
            raise ValueError(
                "Invalid notification context type. Expected BacktestCapacityConstrainedNotificationContext."
            )

        return RenderedEmailTemplate(
            recipient=recipient,
            subject="Backtest Capacity Constrained",
            body=(
                f"Dear {recipient},\n\n"
                f"Backtest capacity is currently constrained."
                f"Your backtest '{notification.context.backtest_id}' has been cancelled."
                f"Please take necessary actions.\n\n"
                "Best regards,\n"
                "Vega Team"
            ),
        )
