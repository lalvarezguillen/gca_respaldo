from __future__ import print_function
import json
import re
import datetime
import os
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.mime.text import MIMEText
import zipfile
import dropbox
import pyodbc


with open("dropbox_credentials.json", "r") as f:
    CONFIG = json.load(f)
DBX = dropbox.Dropbox(CONFIG["api_key"])


def upload_backup(filename):
    print("uploading the backup...")
    with open(filename, "rb") as backup_bin:
        DBX.files_upload(backup_bin.read(), "/{}".format(filename))


def zip_backup():
    print("Zipping the backups...")
    filename = "respaldo_bases_de_datos_{}.zip".format(datetime.datetime.now().strftime("%d-%m-%Y"))
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
    return DBX.files_list_folder("").entries[-1]


def file_is_backup(filename):
    backup_regex = r'respaldo_bases_de_datos_\d\d-\d\d-\d\d\d\d.zip'
    return bool(re.match(backup_regex, filename))


def delete_old_backups():
    files = DBX.files_list_folder("").entries
    if len(files) <= 7:
        return
    old_files = [_file for _file in files[:-7]]
    for _file in old_files:
        if file_is_backup(_file.name):
            DBX.files_delete(_file.path_lower)


def get_file_url(dbx_file):
    return DBX.sharing_create_shared_link(dbx_file.path_lower) \
              .url.replace("dl=0", "dl=1")


def get_backup_url():
    backup = get_last_file()
    url = get_file_url(backup)
    return url


def generate_email_content(sender, recipient):
    message = MIMEMultipart()
    backup_date = datetime.datetime.now().strftime("%d/%m/%Y")
    url = get_backup_url()
    content = ("Se Completo el respaldo de las bases de datos."
               " El enlace de descarga es el siguiente:"
               " {}").format(url)

    message["Subject"] = "Respaldo de base de datos {}".format(backup_date)
    message["To"] = "GCA <{}>".format(recipient)
    message["From"] = "GCA Servidor <{}>".format(sender)
    message.attach(MIMEText(content))
    return message


def mail_backup_link(recipient=CONFIG['antonio_email']):
    sender_mail = CONFIG['mailserver_user']
    conn = smtplib.SMTP(CONFIG['mailserver_address'])
    conn.ehlo()
    conn.starttls()
    conn.ehlo()
    conn.login(CONFIG['mailserver_user'], CONFIG['mailserver_pass'])

    message = generate_email_content(sender_mail, recipient)
    conn.sendmail(sender_mail, recipient, message.as_string())


def main():
    print("Creating the backup...")
    connection_string = ("driver={SQL Server}; server=vgc\\SQLEXPRESS; database={db_name};"
                         " trusted_connection=true; UID={uid};PWD={pwd}")

    params = [
        {
            "db_name": "AUTO_A",
            "uid": CONFIG["uid"],
            "pwd": CONFIG["pwd"],
            "output": os.path.join(os.getcwd(), "auto_a.bak")
        },
        {
            "db_name": "AUTO_C",
            "uid": CONFIG["uid"],
            "pwd": CONFIG["pwd"],
            "output": os.path.join(os.getcwd(), "auto_c.bak")
        },
        {
            "db_name": "AUTO_N",
            "uid": CONFIG["uid"],
            "pwd": CONFIG["pwd"],
            "output": os.path.join(os.getcwd(), "auto_n.bak")
        }
    ]

    for db_params in params:
        print("Backing up {}...".format(db_params["db_name"]))
        con = pyodbc.connect(connection_string.format(**db_params), autocommit=True)
        c = con.cursor()
        c.execute("backup database ? to disk=?", (db_params["db_name"], db_params["output"]))

    #Compress the backups into a zip, named with the backup date
    filename = zip_backup()
    #Upload the resulting file to dropbox
    upload_backup(filename)
    mail_backup_link()
    delete_old_backups()


if __name__ == "__main__":
    main()
