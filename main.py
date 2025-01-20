import json
import threading
import time
from twilio import rest
import smtplib
import base64
from datetime import datetime, timedelta
from dba import DatabaseHandler

# Database connection
DB_PATH = "customer_data.db"
DATABASE_URL = ("postgresql://kbm:U3dJbrL87alo4yx511FNhgWH95S79vQy@dpg-cu30knggph6c73blba00-a.oregon-postgres"
                ".render.com/apis_1i9f")  # Update credentials
sid_encoded = "QUM0NmJkMTAwYTc4ZWM5NzBmYzk1MTYzYzZiZjBlMjhjOQ=="
auth_encode = "NGFkM2QwNmRlNjYzMWVkYmNjY2QyNjhiNDQ2MmE5M2M="
sender_mail_id = "balamurugankarikalan.96@gmail.com"
app_password = "uasipygwgwkmzkhz"


def get_due_events():
    db_handler = DatabaseHandler(DATABASE_URL)
    due_customers = db_handler.fetch_due_customers()
    print(due_customers)
    return due_customers


def update_event_status(event_id, frequency):
    new_last_sent = datetime.now()
    next_due = new_last_sent + timedelta(days=int(frequency.split(" ")[0]))  # Example for frequency of 2 days
    db_handler = DatabaseHandler(DATABASE_URL)
    db_handler.update_customer_schedule(event_id, new_last_sent, next_due)


def send_whatsapp_message(whatsapp_number, msg):
    try:
        print(f"Sending WhatsApp message to {whatsapp_number}: Event - {msg}")
        client = rest.Client(base64.b64decode(sid_encoded).decode('utf-8'),
                             base64.b64decode(auth_encode).decode('utf-8'))
        message = client.messages.create(
            from_='whatsapp:+14155238886',
            body=msg,
            to=f'whatsapp:{whatsapp_number}'
        )
        time.sleep(1)  # Simulate API delay
        return True
    except Exception as e:
        print("Got exception at send whatsapp message", e)
        return False


def send_email_alert(receiver_email_id, message):
    try:
        print(f"Sending email alert to {receiver_email_id}: Event - {message}")
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.starttls()
        s.login(sender_mail_id, app_password)
        text = f"Subject: Unleash Nft Email Alert\n\n{message}"
        s.sendmail(sender_mail_id, receiver_email_id, text)
        s.quit()
        return True
    except Exception as e:
        print("Got Exception while sending email", e)


def worker_thread(event):
    event_id, whatsapp_number, email_id, event_name, event_types, contract_address, token_id, api_key, frequency, last_sent = event
    msg = collect_current_pricing(contract_address, token_id, api_key)
    # status = send_whatsapp_message(whatsapp_number, msg)
    status = send_email_alert(email_id, msg)
    if status:
        update_event_status(event_id, frequency)


def collect_current_pricing(contract_address, token_id, api_key, ):
    import requests
    # contract_address = '0x856b38bf1e2e367f747dd4d3951dda8a35f1bf60'
    # token_id = "8326903"
    # api_key = '2rOpRIb5TEcRLcexOw3Am4cR7SI7r9S2lf7Ahi1r'
    url = (f"https://api.unleashnfts.com/api/v2/nft/liquify/price_estimate?blockchain=ethereum&contract_address"
           f"={contract_address}&token_id={token_id}")

    headers = {
        "accept": "application/json",
        "x-api-key": api_key
    }

    response = requests.get(url, headers=headers)
    data = json.loads(response.text)['data']
    msg = "No data found"
    if data is not None:
        msg = (f"token_id:{data[0]['token_id']}\n"
               f"collection_name:{data[0]['collection_name']}\n"
               f"price_estimate:{data[0]['price_estimate']}\n"
               f"price_estimate_lower_bound:{data[0]['price_estimate_lower_bound']}\n"
               f"price_estimate_upper_bound:{data[0]['price_estimate_upper_bound']}\n")
    return msg


def scheduler():
    while True:
        due_events = get_due_events()
        for event in due_events:
            threading.Thread(target=worker_thread, args=(event,)).start()
        time.sleep(60)  # Scheduler interval


if __name__ == "__main__":
    print("Scheduler started...")
    scheduler()
