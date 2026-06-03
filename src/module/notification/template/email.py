from dataclasses import dataclass
from .base import NotificationTemplateEngine
from ..schema import (
    NotificationType,
    Notification,
    DeploymentCapacityConstrainedNotificationContext,
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
        if notification.type == NotificationType.DEPLOYMENT_CAPACITY_CONSTRAINED:
            return self._render_deployment_capacity_constrained(notification, recipient)
        raise ValueError(f"Unsupported notification type: {notification.type}")

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
