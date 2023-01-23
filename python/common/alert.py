import requests
import os

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
SLACK_USERS_TO_ALERT = os.getenv("SLACK_USERS_TO_ALERT", "")

# Call this function to send messages to Slack channel. Use alert=True to tag users.
def send_alert(message, alert=False):

  if not SLACK_WEBHOOK_URL:
    # Slack url not found, exit
    return

  # Parse users and convert them to slack tag format
  alert_msg = ""
  if alert and SLACK_USERS_TO_ALERT:
    users = SLACK_USERS_TO_ALERT.split(",")
    alert_list = ", ".join([f"<@{u}>" for u in users])
    alert_msg = f"ALERT {alert_list}: "

  msg_object = {
    "text": f"{alert_msg}{message}"
  }
  requests.post(SLACK_WEBHOOK_URL, json=msg_object)
