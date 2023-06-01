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
from pathlib import Path

from colorlog import ColoredFormatter

LOGLEV = ["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

LOGGING_CFG = """
    [loggers]
    keys=root

    [logger_root]
    handlers=console
    qualname={0}
    level={1}

    [handlers]
    keys=console

    [handler_console]
    class=logging.StreamHandler
    formatter=color
    level={1}
    stream  : ext://sys.stdout

    [formatters]
    keys=color

    [formatter_color]
    class=colorlog.ColoredFormatter
    format=%(log_color)s%(levelname)-8s%(reset)s %(bg_blue)s[%(name)s]%(reset)s %(message)s
    datefmt=%m-%d %H:%M:%S
    """


def get_logging_config(name='root', log_level='INFO'):
    """Return the logging configuration.

    Parameters
    ----------
    name : str
        The name of the logger.
        Defaults to `'root'`.
    log_level : {'CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'}
        A valid logging level.
        Defaults to `'INFO'`.
    """
    return LOGGING_CFG.format(name, log_level)


def configure_logger(name, log_path="", log_level='INFO'):
    """Return a configured logger.

    Parameters
    ----------
    name : str
        The name of the logger.
    log_path : str
        A file path to save the log.
        Defaults to `''`.
    log_level : {'CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET'}
        A valid logging level.
        Defaults to `'INFO'`.
    """
    colored_formatter = ColoredFormatter(
        "%(log_color)s%(levelname)-8s%(reset)s %(bg_blue)s[%(name)s]%(reset)s %(message)s",
        datefmt=None,
        reset=True,
        style='%',
    )
    simple_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(lineno)d: %(message)s")

    # create console handler:
    console = logging.StreamHandler()
    console.setFormatter(colored_formatter)

    logger = logging.getLogger(name)
    logger.addHandler(console)
    logger.setLevel(getattr(logging, log_level))

    if log_path is not None and log_path != "":
        # create file handler:
        fh = logging.FileHandler(Path(log_path) / f'{name}.log', mode='w')
        fh.setFormatter(simple_formatter)
        logger.addHandler(fh)

    return logger
