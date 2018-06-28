"""

"""


import datetime
import email
import imaplib
import re
import sys

import click


class EmailDownloaderError(Exception):
    """An error was encountered during the matching of the criteria or attachment download/upload process."""


class _DateType(click.ParamType):
    """Initialise a custom click type to be used to validate a date provided at command line input."""

    name = "Date (YYYY-MM-DD)"

    def convert(self, value, param, ctx):
        """Check that the input is a valid FTP connection string on a standard regex pattern.

        This method is called implicitly by click and shouldn't be invoked directly.

        Positional Arguments:
        1. value (string): the value that is to be validated / converted
        2. param (unknown): (unknown as not documented by click). This value should be passed to the second parameter of
        the fail() method
        3. ctx (unknown): (unknown as not documented by click). This value should be passed to the third parameter of
        the fail() method

        """
        try:
            return datetime.datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            self.fail("Incorrect date format, should be YYYY-MM-DD")


class TitanFlowManager(object):
    def __init__(self, imap_ssl_host, username, password, fetch_one, match_date_received, email_subject, email_sender,
                 filename_pattern, archive_folder, load_date):
        """

        """
        self.imap_ssl_host = imap_ssl_host
        self.username = username
        self.password = password
        self.fetch_one = fetch_one
        self.match_date_received = match_date_received
        self.email_subject = re.compile(email_subject)
        self.email_sender = re.compile(email_sender)
        self.filename_pattern = re.compile(filename_pattern)
        self.archive_folder = archive_folder
        self.load_date = load_date

        from datalake import utilities
        self.acquire_program = utilities.AcquireProgram()
        self.logger = self.acquire_program.logger

    def _raise_if_not_ok(self, response, message):
        if response != "OK":
            raise EmailDownloaderError(message[0].decode())

    def archive_uids(self, imap, uids):
        """

        """
        imap.select()  # get out of readonly mode for the moving
        for uid in uids:
            self._raise_if_not_ok(*imap.uid("COPY", uid, self.archive_folder))
            self._raise_if_not_ok(*imap.uid("STORE", uid, "+FLAGS", "(\Deleted)"))
        imap.expunge()

    def get_attachments(self, imap):
        """

        """
        if self.archive_folder is not None:
            self._raise_if_not_ok(*imap.select(self.archive_folder, readonly=True))
        imap.select(readonly=True)
        if self.match_date_received:
            on_value = (self.load_date - datetime.timedelta(days=1)).strftime("%d-%b-%Y")
            uids = imap.uid("search", "ON", on_value)
        else:
            uids = imap.uid("search", None, "ALL")[1][0].split()
        uids.reverse()
        attachments_found = False
        for uid in uids:
            raw_mail = imap.uid("fetch", uid, "(RFC822)")[1][0][1]
            mail = email.message_from_bytes(raw_mail, _class=email.message.EmailMessage)
            if self.email_subject.match(mail["Subject"]) and self.email_sender.match(mail["From"]):
                for attachment in mail.iter_attachments():
                    if self.filename_pattern.match(attachment["Content-Description"]):
                        attachments_found = True
                        yield uid, attachment
                if self.fetch_one and attachments_found:
                    return
        if not attachments_found:
            raise EmailDownloaderError("0 attachments were found matching the criteria.")

    def run(self):
        """"""
        with imaplib.IMAP4_SSL(self.imap_ssl_host) as imap:
            imap.login(self.username, self.password)
            attachments = self.get_attachments(imap)
            uids_to_move = set()
            for uid, attachment in attachments:
                self.upload(attachment)
                uids_to_move.add(uid)
            if self.archive_folder is not None:
                self.archive_uids(imap, uids_to_move)

    def upload(self, attachment):
        """

        """
        blob_name = self.acquire_program.get_blob_name("{TITAN_DATA_SET_NAME}_{TITAN_LOAD_DATE}_{file_name}",
                                                       file_name=attachment["Content-Description"])
        self.logger.info("Uploading attachment...")
        self.acquire_program.create_blob_from_bytes(attachment.get_payload(decode=True), blob_name=blob_name)


@click.command()
@click.option("-h", "--imap-ssl-hostname", required=True, help="")
@click.option("-u", "--username", required=True, help="")
@click.option("-p", "--password", required=True, help="")
@click.option("-f", "--fetch-one", type=bool, required=True, help="")
@click.option("-m", "--match-date-received", type=bool, default=False, help="")
@click.option("-e", "--email-subject", default=".*", help="")
@click.option("-s", "--email-sender", default=".*", help="")
@click.option("-n", "--filename-pattern", default=".*", help="")
@click.option("-a", "--archive-folder", help="")
@click.option("-l", "--load-date", type=_DateType(), help="")
def main(imap_ssl_host, username, password, fetch_one, match_date_received, email_subject, email_sender,
         filename_pattern, archive_folder, load_date):
    """

    """
    if load_date is None:
        load_date = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
    yyyy, mm, dd = str(load_date).split("-")
    filename_pattern = filename_pattern.replace("YYYY", yyyy).replace("MM", mm).replace("DD", dd)
    flow_manager = TitanFlowManager(imap_ssl_host, username, password, fetch_one, match_date_received, email_subject,
                                    email_sender, filename_pattern, archive_folder, load_date)
    try:
        flow_manager.run()
    except Exception as error:
        flow_manager.logger.exception(error)
        sys.exit("ERROR ENCOUNTERED - CHECK LOGS")
