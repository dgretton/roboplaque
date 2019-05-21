import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_message(recipient, subject, message_body):
    #http://stackoverflow.com/questions/882712/sending-html-email-using-python

    

    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['To'] = recipient

    # Record the MIME types of both parts - text/plain and text/html.
    part1 = MIMEText(message_body, 'plain')

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(part1)

    # Send the message via local SMTP server.
    mail = smtplib.SMTP('smtp.gmail.com', 587)

    mail.ehlo()

    mail.starttls()

    mail.login('hamiltonstarlet', '>vmRZBmos6')
    mail.sendmail('random_sender_email', recipient, msg.as_string())
    mail.quit()


def summon_erika(subject, message):
    send_message("alden.debenedictis@gmail.com", subject, message)
    send_message("16266270107@tmomail.net", subject, message)
  
def summon_dana(subject, message):
    send_message("dana.gretton@gmail.com", subject, message)
    send_message("dgretton@mit.edu", subject, message)

def summon_devteam(subject, message):
    summon_erika(subject, message) 
    summon_dana(subject, message)

# usage

# send to an email address
# send_message("erika.debene@gmail.com", 'subject', 'blahblahfragment')

# send to a phone's email address (send an SMS)
# send_message("16266270107@tmomail.net", 'new subject', 'testing message')

#text_erika('Arduino problem', 'arduino has failed to respond in x minutes')
