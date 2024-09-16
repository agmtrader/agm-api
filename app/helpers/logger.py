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

    def info(self, message):
        self.logger.info(f"[blue]{message}[/blue]", extra={'markup': True})

    def success(self, message):
        self.logger.info(f"[green]{message}[/green]\n", extra={'markup': True})

    def error(self, message):
        self.logger.error(f"[red]{message}[/red]", extra={'markup': True})

logger = Logger()