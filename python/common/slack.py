import requests

from common.config import ENVIRONMENT, SLACK_WEBHOOK_URL, SLACK_USERS_TO_ALERT


# Call this function to send messages to Slack channel. Use alert=True to tag users.
def send_to_channel(message, alert=False):
    if not SLACK_WEBHOOK_URL:
        # Slack url not found, exit
        return

    # Parse users and convert them to slack tag format
    alert_msg = ""
    if alert and len(SLACK_USERS_TO_ALERT) > 0:
        alert_list = ", ".join([f"<@{u}>" for u in SLACK_USERS_TO_ALERT])
        alert_msg = f"ALERT {alert_list}: "

    msg_object = {"text": f"Msg from {ENVIRONMENT}:\n{alert_msg}{message}"}
    requests.post(SLACK_WEBHOOK_URL, json=msg_object)
