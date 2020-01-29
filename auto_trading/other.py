from Backtest import config
import smtplib, ssl 

def send_email(message):
    port = 465 # for SSL
    password = config.password
    smtp_server = "smtp.gmail.com"
    sender_email = config.sender_email  # Enter your address
    if type(receiver_email) is not list:
        raise TypeError("receiver_email is not type of list. Covert it to list") 
    receiver_email = config.receiver_email  # Enter receiver address

    # msg = MIMEText("""body""")
    # msg['To'] = ", ".join(receiver_email)
    # msg['Subject'] = "subject line"
    # msg['From'] = sender_email

    context = ssl.create_default_context() # Create a secure SSL context
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        for email in receiver_email:
            server.sendmail(sender_email, email, message)