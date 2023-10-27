import logging
from bqlogger.log_config import (BQNT_DEBUG_LOG_FORMAT,
                                 _bqlogger_file_name,
                                 BQNT_LIBRARY_LOGGERS)
import tkinter as tk
from tkinter.scrolledtext import ScrolledText
import pandas as pd
import os
import tempfile


class LogMonitor(logging.Handler):

    def __init__(self, level=logging.INFO):
        super().__init__()
        # order matters
        self._configure_logging(level)
        self._configure_ui()
        self._load_file()

    def _configure_logging(self, level):
        self._formatter = logging.Formatter(BQNT_DEBUG_LOG_FORMAT)
        self.setFormatter(self._formatter)
        self.setLevel(level)
        self._logdir = os.environ.get('BLOOMBERG_LOG_DIR',
                                      tempfile.gettempdir())
        self._logfile = _bqlogger_file_name
        self._add_self_to_bqloggers(level)

    def _configure_ui(self):
        self._win = tk.Tk()
        self._win.title(self._logfile)
        self._frame = tk.Frame(master=self._win)
        self._frame.pack(fill='both', expand='yes')
        self._text = ScrolledText(master=self._frame,
                                  wrap=tk.WORD,
                                  bg='black',
                                  fg='white')
        self._text.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)

        self._text.tag_config('INFO', foreground='white')
        self._text.tag_config('DEBUG', foreground='gray')
        self._text.tag_config('WARNING', foreground='orange')
        self._text.tag_config('ERROR', foreground='red')
        self._text.tag_config('CRITICAL', foreground='red')

        self._win.bind('<Control-c>', self._copy)

    def _load_file(self):
        try:
            with open(os.path.join(self._logdir, self._logfile)) as f:
                for line in f:
                    self._text.insert(tk.END, line)
                    self._text.yview(tk.END)
        except Exception:
            pass

    def _copy_to_clipboard(self, text):
        df = pd.DataFrame([text])
        df.to_clipboard(index=False, header=False)

    def _copy(self, event=None):
        text = self._text.get("sel.first", "sel.last")
        if text:
            self._copy_to_clipboard(text)

    def _add_self_to_bqloggers(self, level):
        for module_name in BQNT_LIBRARY_LOGGERS:
            logger = logging.getLogger(module_name)
            logger.setLevel(level)
            logger.addHandler(self)

    def emit(self, record):
        msg = self.format(record)
        self._text.insert(tk.END, msg + '\n', record.levelname)
        self._text.yview(tk.END)
