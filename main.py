from urllib import request
from bs4 import BeautifulSoup
from time import asctime, gmtime, sleep
import sqlite3
import schedule  
import os
import sendgrid
from sendgrid.helpers.mail import Email, Mail, Content, Personalization

context = {
    'poll_freq_secs': int(os.environ['poll_freq_secs']),
    'sendgrid_apikey': os.environ['sendgrid_apikey'],
    'from_email': os.environ['from_email'],
    'to_email': os.environ['to_email']
}


def read_alerts(path):
    res = request.urlopen(path)
    doc = res.read()

    tree = BeautifulSoup(doc, 'lxml')
    tree.find(id='ctl00_MasterPlaceHolder1_DataList1').select('div')
    alert_list = []
    alerts = tree.select('table[id="ctl00_MasterPlaceHolder1_DataList1"] > tr > td > div')
    for alert in alerts:
        alert = alert.select('span')
        title = ''.join(x.text for x in alert[0:4])
        text = alert[4].text
        alert_list.append((title,text))
    return alert_list

def store_alerts(conn, gmnow, alerts):
    new_alerts = []
    for (title, content) in alerts:
        cursor = conn.execute(
                "SELECT * from alerts WHERE title=? AND content=?",
                (title, content))
        if not cursor.fetchone():
            new_alerts.append((title, content))
            conn.execute(
                "INSERT INTO alerts VALUES (?, ?, ?)",
                (gmnow, title, content))
    return new_alerts

def get_conn():
    conn = sqlite3.connect("alerts.db")
    conn.execute("CREATE TABLE IF NOT EXISTS alerts (date text, title text, content text)")
    return conn

def read_new_alerts():
    alerts = read_alerts("http://www.psbbmillenniumschool.org/psbbomr/VirtualSchool/WebSiteAlerts.aspx")
    conn = get_conn()
    new_alerts = store_alerts(conn, asctime(gmtime()), alerts)
    conn.commit()
    conn.close()
    return new_alerts

def build_email(context, subject, message):
    mail = Mail(
        from_email=Email(context['from_email']),
        subject=subject,
        content=Content('text/plain', message))

    personalization = Personalization()
    for to_addr in context['to_email'].split(';'):
        personalization.add_to(Email(to_addr))
    mail.add_personalization(personalization)

    return mail

def send_email(subject, message):
    sg = sendgrid.SendGridAPIClient(apikey=context['sendgrid_apikey'])
    mail = build_email(context, subject, message)
    response = sg.client.mail.send.post(request_body=mail.get())

def send_alerts():
    print('Look for new alerts...')
    new_alerts = read_new_alerts()
    print('New alerts found:', len(new_alerts))
    for (title, content) in new_alerts:
        send_email(title, content)

def main():
    print("Scheduling alert job...")
    schedule.every(context['poll_freq_secs']).seconds.do(send_alerts)
    while True:
        schedule.run_pending()  
        sleep(5)

main()

