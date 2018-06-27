"""

"""


import datetime
import email
import imaplib
import sys

import click


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
                 filename_pattern, archive_mailbox):
        """

        """
        self.imap_ssl_host = imap_ssl_host
        self.username = username
        self.password = password
        self.fetch_one = fetch_one
        self.match_date_received = match_date_received
        self.email_subject = email_subject
        self.email_sender = email_sender
        self.filename_pattern = filename_pattern
        self.archive_mailbox = archive_mailbox

        from datalake import utilities
        self.acquire_program = utilities.AcquireProgram()
        self.logger = self.acquire_program.logger

    def run(self):
        """"""
        pass

    def upload(self, ):
        """

        """
        pass


@click.command()
@click.option("-h", "--imap-ssl-hostname", required=True, help="")
@click.option("-u", "--username", required=True, help="")
@click.option("-p", "--password", required=True, help="")
@click.option("-f", "--fetch-one", type=bool, required=True, help="")
@click.option("-m", "--match-date-received", type=bool, default=False, help="")
@click.option("-e", "--email-subject", default=".*", help="")
@click.option("-s", "--email-sender", default=".*", help="")
@click.option("-n", "--filename-pattern", default=".*", help="")
@click.option("-a", "--archive-mailbox", help="")
@click.option("-l", "--load-date", type=_DateType(), help="")
def main(imap_ssl_host, username, password, fetch_one, match_date_received, email_subject, email_sender,
         filename_pattern, archive_mailbox, load_date):
    """

    """
    if load_date is None:
        load_date = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
    yyyy, mm, dd = str(load_date).split("-")
    filename_pattern = filename_pattern.replace("YYYY", yyyy).replace("MM", mm).replace("DD", dd)
    flow_manager = TitanFlowManager(imap_ssl_host, username, password, fetch_one, match_date_received, email_subject,
                                    email_sender, filename_pattern, archive_mailbox)
    try:
        flow_manager.run()
    except Exception as error:
        flow_manager.logger.exception(error)
        sys.exit("ERROR ENCOUNTERED - CHECK LOGS")
