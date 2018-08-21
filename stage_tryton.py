#!/usr/bin/python3
# -*- coding: iso-8859-15 -*-

import argparse
import os
import sys

def parse_commandline():
    options = {}

    parser = argparse.ArgumentParser(description='Create and/or populate trytond database.')

    parser.add_argument('-u', '--url', default='sqlite://', help='URI to connect to the sql database')
    parser.add_argument('-c', '--create-db', dest="create", action='store_true', help='create tryton sql database')
    parser.add_argument('-p', '--populate-db', dest="populate", action='store_true', help='populate tryton sql database with data')
    parser.add_argument('-d', '--directory', default=os.getcwd(), dest="datadir", nargs='?', help='directory where is stored data for populate database')

    options = parser.parse_args()

    return options

if __name__ == '__main__':
    if __package__ is None:
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

options = parse_commandline()

if options.create:
    import create_tryton
    create_tryton.create(options.url)

if options.populate:
    import populate_admon
    populate_admon.populate(options.url, options.datadir)
