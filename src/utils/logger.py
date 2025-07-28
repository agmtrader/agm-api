from rich.logging import RichHandler
from rich.console import Console
from rich.theme import Theme
import logging
import os

class Logger:
    def __init__(self):
        # Check if we're in development mode (default to True if not set)
        self.dev_mode = os.getenv('DEV_MODE', 'true').lower() in ('true', '1', 'yes')
        
        if self.dev_mode:
            # Development mode: Use Rich logging with colors and formatting
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
        else:
            # Production mode: Use plain logging without Rich formatting
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
        
        self.logger = logging.getLogger("rich" if self.dev_mode else "agm-api")

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
        logging.getLogger('pytz').setLevel(logging.ERROR)
        logging.getLogger('ib_insync').setLevel(logging.ERROR)

    def info(self, message):
        if self.dev_mode:
            self.logger.debug(f"[white]{message}[/white]", extra={'markup': True})
        else:
            self.logger.info(message)

    def success(self, message):
        if self.dev_mode:
            self.logger.debug(f"[success]{message}[/success]", extra={'markup': True})
        else:
            self.logger.info(f"SUCCESS: {message}")

    def announcement(self, message, type='info'):
        if self.dev_mode:
            if type == 'info':
                self.logger.info(f"[bold secondary]{message}[/bold secondary]", extra={'markup': True})
            elif type == 'success':
                self.logger.info(f"[bold primary]{message}[/bold primary]\n", extra={'markup': True})
            else:
                raise ValueError("Invalid type. Choose 'info' or 'success'.")
        else:
            if type == 'info':
                self.logger.info(f"INFO: {message}")
            elif type == 'success':
                self.logger.info(f"SUCCESS: {message}")
            else:
                raise ValueError("Invalid type. Choose 'info' or 'success'.")
        
    def warning(self, message):
        if self.dev_mode:
            self.logger.warning(f"[bold warning]{message}[/bold warning]", extra={'markup': True})
        else:
            self.logger.warning(message)

    def error(self, message):
        if self.dev_mode:
            self.logger.error(f"[on white][error]{message}[/error][/on white]", extra={'markup': True})
        else:
            self.logger.error(message)


logger = Logger()
