import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import *
from dotenv import load_dotenv, find_dotenv
import base64

load_dotenv(find_dotenv())
api_key = os.getenv('SENDGRID_API_KEY')


def build_attachment(csv):
    """Build attachment mock. Make sure your content is base64 encoded before passing into attachment.content.
    Another example: https://github.com/sendgrid/sendgrid-python/blob/HEAD/use_cases/attachment.md"""
    attachment = Attachment()
    with open(csv, 'rb') as file:
        b64data = base64.b64encode(file.read())
        file.close()

    attachment.file_content = str(b64data,'utf-8')
    attachment.file_type = "application/csv"
    attachment.file_name = os.path.split(csv)[1]
    attachment.disposition = "attachment"
    #attachment.content_id = "Balance Sheet"
    print(csv)
    return attachment


def send_mail(sender, receiver, subject, body, files=[]):
    message = Mail(
        from_email=sender,
        to_emails=receiver,
        subject=subject,
        html_content=body
    )
    for file in files:
        attachment = build_attachment(file)
        message.add_attachment(attachment)

    try:
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
    except Exception as e:
        print(e.message)

