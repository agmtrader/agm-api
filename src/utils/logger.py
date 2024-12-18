from rich.logging import RichHandler
from rich.console import Console
from rich.theme import Theme
import logging

class Logger:
    def __init__(self):
        custom_theme = Theme({
            "info": "cyan",
            "warning": "yellow",
            "error": "bold red",
            "critical": "bold white on red",
        })
        self.console = Console(theme=custom_theme)
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
            datefmt="[%X]",
            handlers=[RichHandler(console=self.console, rich_tracebacks=True)]
        )
        self.logger = logging.getLogger("rich")

        # Suppress other logs
        logging.getLogger('werkzeug').setLevel(logging.ERROR)
        logging.getLogger('flask').setLevel(logging.ERROR)
        logging.getLogger('requests').setLevel(logging.ERROR)
        logging.getLogger('connectionpool').setLevel(logging.ERROR)
        logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)
        logging.getLogger('googleapiclient.discovery').setLevel(logging.ERROR)
        logging.getLogger('google_auth_httplib2').setLevel(logging.ERROR)
        logging.getLogger('feedparser').setLevel(logging.ERROR)
        logging.getLogger('nltk').setLevel(logging.ERROR)
        logging.getLogger('chardet').setLevel(logging.ERROR)
        logging.getLogger('sqlalchemy').setLevel(logging.ERROR)

    def info(self, message):
        self.logger.debug(f"[blue]{message}[/blue]", extra={'markup': True})

    def success(self, message):
        self.logger.debug(f"[green]{message}[/green]", extra={'markup': True})

    def warning(self, message):
        self.logger.warning(f"[yellow]{message}[/yellow]\n", extra={'markup': True})

    def announcement(self, message, type='info'):
        if type == 'info':
            self.logger.info(f"\n[blue][bold]{message}[/bold][/blue]", extra={'markup': True})
        elif type == 'success':
            self.logger.info(f"[bold][green]{message}[/green][/bold]\n", extra={'markup': True})
        else:
            raise ValueError("Invalid type. Choose 'info' or 'success'.")

    def error(self, message):
        self.logger.error(f"[red]{message}[/red]\n", extra={'markup': True})

logger = Logger()
