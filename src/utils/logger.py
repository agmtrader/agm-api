from rich.logging import RichHandler
from rich.console import Console
from rich.theme import Theme
import logging

class Logger:
    def __init__(self):

        custom_theme = Theme({
            "primary": "#F26C0D",
            "secondary": "#2571A5",
            "warning": "#FFB300",
            "success": "#00C853",
            "error": "#FF4B4B",
            "white": "#FFFFFF",
            "bold primary": "#F26C0D",
            "bold secondary": "#2571A5",
            "bold warning": "#FFB300",
            "bold success": "#00C853",
            "bold error": "#FF4B4B",
            "bold white": "#FFFFFF",
        })

        self.console = Console(
            theme=custom_theme,
            color_system="truecolor"
        )
        
        logging.basicConfig(
            level=logging.DEBUG,
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
        logging.getLogger('geventwebsocket').setLevel(logging.ERROR)

    def info(self, message):
        self.logger.debug(f"[white]{message}[/white]", extra={'markup': True})

    def success(self, message):
        self.logger.debug(f"[success]{message}[/success]", extra={'markup': True})

    def announcement(self, message, type='info'):
        if type == 'info':
            self.logger.info(f"[bold secondary]{message}[/bold secondary]", extra={'markup': True})
        elif type == 'success':
            self.logger.info(f"[bold primary]{message}[/bold primary]\n", extra={'markup': True})
        else:
            raise ValueError("Invalid type. Choose 'info' or 'success'.")
        
    def warning(self, message):
        self.logger.warning(f"[bold warning]{message}[/bold warning]", extra={'markup': True})

    def error(self, message):
        self.logger.error(f"[bold error on white]{message}[/bold error on white]", extra={'markup': True})


logger = Logger()
