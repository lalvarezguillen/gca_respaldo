from __future__ import print_function
import json
import datetime
import traceback
import os
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import zipfile
import dropbox
import pyodbc


with open("dropbox_credentials.json", "r") as f:
    CREDENTIALS = json.load(f)

def upload_backup(filename):
    print("uploading the backup...")
    dbx = dropbox.Dropbox(CREDENTIALS["api_key"])
    f = open(filename, "rb")
    dbx.files_upload(f.read(), "/{}".format(filename))

def zip_backup():
    print("Zipping the backups...")
    filename = "respaldo_bases_de_datos_{}.zip".format(datetime.now().strftime("%d-%m-%Y"))
    print(filename)
    with zipfile.ZipFile(filename, "w", zipfile.ZIP_DEFLATED) as respaldo:
        respaldo.write("auto_a.bak")
        respaldo.write("auto_c.bak")
        respaldo.write("auto_n.bak")
    os.remove("auto_a.bak")
    os.remove("auto_c.bak")
    os.remove("auto_n.bak")

    return filename


def get_last_file():
    dbx = dropbox.Dropbox(CREDENTIALS["api_key"])
    return dbx.files_list_folder("").entries[-1]

def get_file_url(file):
    dbx = dropbox.Dropbox(CREDENTIALS["api_key"])
    return dbx.sharing_create_shared_link(file.path_lower).url.replace(
        "dl=0", "dl=1")

def get_backup_url():
    backup = get_last_file()
    url = get_file_url(backup)
    return url


def generate_email_content(sender, recipient):
    message = MIMEMultipart()
    backup_date = datetime.now().strftime("%d/%m/%Y")

    url = get_backup_url()
    content = """
    Se Completo el respaldo de las bases de datos.
    El enlace de descarga es el siguiente:
    {}
    """.format(url)

    message["Subject"] = "Respaldo de base de datos {}".format(backup_date)
    message["To"] = "GCA <{}>".format(recipient)
    message["From"] = "GCA Servidor <{}>".format(sender)
    message.attach(MIMEText(content))
    return message

def mail_backup_link(recipient="adujmovic@gcautopartes.com.ve"):
    sender_mail = "webmaster@gcautopartes.com.ve"
    conn = smtplib.SMTP("ceres.calcanet.com:587")
    conn.ehlo()
    conn.starttls()
    conn.ehlo()
    conn.login(sender_mail, "El14delnovas")

    message = generate_email_content(sender_mail, recipient)

    conn.sendmail(sender_mail, recipient, message.as_string())


def main():
    print("Creating the backup...")
    connection_string = ("driver={SQL Server}; server=vgc\\SQLEXPRESS; database={db_name};"
                         " trusted_connection=true; UID={uid};PWD={pwd}")

    params = [
        {
            "db_name": "AUTO_A",
            "uid": CREDENTIALS["uid"],
            "pwd": CREDENTIALS["pwd"],
            "output": os.path.join(os.getcwd(), "auto_a.bak")
        },
        {
            "db_name": "AUTO_C",
            "uid": CREDENTIALS["uid"],
            "pwd": CREDENTIALS["pwd"],
            "output": os.path.join(os.getcwd(), "auto_c.bak")
        },
        {
            "db_name": "AUTO_N",
            "uid": CREDENTIALS["uid"],
            "pwd": CREDENTIALS["pwd"],
            "output": os.path.join(os.getcwd(), "auto_n.bak")
        }
    ]

    for db_params in params:
        print("Backing up {}...".format(db_params["db_name"]))
        con = pyodbc.connect(connection_string.format(**db_params), autocommit=True)
        c = con.cursor()
        c.execute("backup database ? to disk=?", (db_params["db_name"], db_params["output"]))

    #Compress the backups into a zip, named with the backup date
    file = zip_backup()
    #Upload the resulting file to dropbox
    upload_backup(file)
    mail_backup_link()


if __name__ == "__main__":
    main()
