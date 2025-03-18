#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# plantdb - Data handling tools for the ROMI project
#
# Copyright (C) 2018-2019 Sony Computer Science Laboratories
# Authors: D. Colliaux, T. Wintz, P. Hanappe
#
# This file is part of plantdb.
#
# plantdb is free software: you can redistribute it
# and/or modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.
#
# plantdb is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with plantdb.  If not, see
# <https://www.gnu.org/licenses/>.
# ------------------------------------------------------------------------------
import logging
import sys

from colorlog import ColoredFormatter

# Define a set of log levels by retrieving all existing log level names from Python's logging module,
# excluding specific levels ("FATAL" and "WARN") which are not standard or redundant.
LOG_LEVELS = set(logging._nameToLevel.keys()) - {"FATAL", "WARN"}

# Set the default logging level to "INFO".
DEFAULT_LOG_LEVEL = 'INFO'

# Define the log message format for non-colored logs.
# Includes the log level name, the logger name, the line number, and the log message itself.
LOG_FMT = "{levelname:<8} [{name}] l.{lineno} {message}"

# Define the log message format for colored logs.
# The color is dynamically applied using `log_color` and `bg_blue` and reset after styling.
COLOR_LOG_FMT = "{log_color}{levelname:<8}{reset} {bg_blue}[{name}]{reset} {message}"

# Create a standard logging formatter instance with the non-colored log format.
# Uses Python's advanced `{}` string formatting style (specified by `style="{"`).
FORMATTER = logging.Formatter(
    LOG_FMT,
    style="{",
)

# Create a colored logging formatter instance for enhanced log readability in terminal outputs.
# Applies colors for log levels, resets the style after application, and uses the same `{}` style formatting.
COLORED_FORMATTER = ColoredFormatter(
    COLOR_LOG_FMT,
    datefmt=None,  # No date is included in the log format.
    reset=True,  # Automatically reset styles applied to the log after each log message.
    style='{',  # Use the `{}` style of string formatting.
)


def get_console_handler():
    """Creates and configures a console handler for logging that outputs to the standard output stream.

    This handler uses a specific formatter for colored log messages.

    Returns
    -------
    logging.StreamHandler
        The configured console logging handler with a colored formatter.
    """
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(COLORED_FORMATTER)
    return console_handler


def get_file_handler(log_file):
    """Creates and configures a file handler for logging.

    This function initializes a logging file handler, sets its level, and applies a predefined formatter to it.
    The file handler writes log messages to the specified file in write mode.
    The log level determines the severity of messages that are captured by the handler.

    Parameters
    ----------
    log_file : str or pathlib.Path
        A path to the log file where log messages will be written.

    Returns
    -------
    logging.FileHandler
        The configured logging file handler for capturing log messages.
    """
    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setFormatter(FORMATTER)
    return file_handler


def get_dummy_logger(logger_name, log_level=DEFAULT_LOG_LEVEL):
    """Creates and configures a dummy logger with a console handler.

    This function generates a logger instance associated with the specified name and log level.
    The logger is configured with a console handler to allow output of log statements to the console.
    It is typically used for basic logging setups where no file-based or external configurations are required.

    Parameters
    ----------
    logger_name : str
        A name to use for the logger.
    log_level : int or str, optional
        A logging level to set for the logger. Defaults to ``DEFAULT_LOG_LEVEL``.

    Returns
    -------
    logging.Logger
        The configured logger instance with a console handler attached.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    logger.addHandler(get_console_handler())
    return logger


def get_logger(logger_name, log_file=None, log_level=DEFAULT_LOG_LEVEL):
    """Get a logger with a specific name and log level for console output.

    This function retrieves an existing logger instance with the specified name, or creates a new one if none exists.

    Parameters
    ----------
    name : str
        A name to use for the logger.
        Typically derived from the module or component that generates the logs.
    log_file : str or pathlib.Path, optional
        A path to the file where log messages should be written.
        Defaults to ``None``, in which case no file handler is added.
    log_level : int or str, optional
        A logging level to set for the logger. Defaults to ``DEFAULT_LOG_LEVEL``.

    Returns
    -------
    logging.Logger
        The configured logger instance ready to log messages with the specified settings.
    """
    logger_name = logger_name.split(".")[-1]
    if not logging.getLogger(logger_name).hasHandlers():
        return _get_logger(logger_name, log_file=log_file, log_level=log_level)
    return logging.getLogger(logger_name)


def _get_logger(logger_name, log_file=None, log_level=DEFAULT_LOG_LEVEL):
    """Creates and configures a logger instance with specified settings.

    This function sets up a logger with a given name, associates it with a console
    handler, and optionally with a file handler. It sets the desired log level and
    ensures the logger does not propagate messages to its parent logger. The logger
    is returned to the caller for usage.

    Parameters
    ----------
    logger_name : str
        A name to use for the logger.
    log_file : str or pathlib.Path, optional
        A path to the file where log messages should be written.
        Defaults to ``None``, in which case no file handler is added.
    log_level : int or str, optional
        A logging level to set for the logger. Defaults to ``DEFAULT_LOG_LEVEL``.

    Returns
    -------
    logger : logging.Logger
        The configured logger instance ready to log messages with the specified settings.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    logger.addHandler(get_console_handler())
    if log_file is not None:
        logger.addHandler(get_file_handler(log_file))

    # with this pattern, it's rarely necessary to propagate the error up to parent
    logger.propagate = False

    return logger