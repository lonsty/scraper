#!/usr/bin/env python3
# @Author: eilianxiao
# @Date: Dec 27 03:59 2020
import typer

from scraper.cnu import cnu_command

if __name__ == '__main__':
    typer.run(cnu_command)
