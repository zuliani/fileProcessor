#!/usr/bin/python -O

'''
Created on Jul 23, 2012

@author: Marco Zuliani
         marco.zuliani@gmail.com

    Copyright 2012, Marco Zuliani

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''

import argparse
import os
import sys
import re

VERSION = '1.0'

# defaults
DEFAULT_nameFormat = '/N/_new./E/'
DEFAULT_verbose = 1

# errors
ERROR_INPUT_PATH_DOES_NOT_EXIST = -1
ERROR_INVALID_REGEX = -2

def processFile( filename, args ):

    if args.verbosity > 1:
        print 'Processing', filename

def run( args ):

    # check input path
    if not os.path.isdir( args.inputPath ):
        print 'Error: the input path', args.inputPath, 'does not exist'
        sys.exit( ERROR_INPUT_PATH_DOES_NOT_EXIST )

    # check if output path needs to be created
    if not os.path.exists( args.outputPath ):
        os.makedirs( args.outputPath )

    try:
        pattern = re.compile( args.fileFilter )
    except:
        print 'The regular expression', args.fileFilter, 'is invalid'
        print 'Sorry dude. You could be hitting on of these two bugs: http://bugs.python.org/issue2537 or http://bugs.python.org/issue214033'
        sys.exit( ERROR_INVALID_REGEX )

    # check if we do not need to recurse or not
    if args.recursive:

        for dirname, dirnames, filenames in os.walk( args.inputPath ):

            for filename in filenames:

                if pattern.search( filename ):
                    processFile( os.path.join( dirname, filename ), args )

    else:

        for item in os.listdir( args.inputPath ):

            filename = os.path.join( args.inputPath, item )
            if os.path.isfile( filename ) and pattern.search( filename ):
                processFile( filename, args )

if __name__ == "__main__":

    parser = argparse.ArgumentParser( description = 'Process a set of files applying a given function to each of them' )

    parser.add_argument( '-i', '--inputPath',
                         type = str,
                         action = 'store',
                         help = 'the folder to crawl',
                         required = True )

    parser.add_argument( '-o', '--outputPath',
                         type = str,
                         action = 'store',
                         help = 'the output folder (default is the same as the input)',
                         default = None,
                         required = False )

    parser.add_argument( '-f', '--fileFilter',
                         type = str,
                         action = 'store',
                         help = 'regular expression to filter the input files',
                         required = True )

    parser.add_argument( '-n', '--nameFormat',
                         type = str,
                         action = 'store',
                         help = 'name format: /N/ indicates the original name, /E/ the original extensions (default is ' + DEFAULT_nameFormat + ')',
                         default = None )

    parser.add_argument( '-r', '--recursive',
                         action = 'store_true',
                         help = 'recurse inside the folders' )

    parser.add_argument( '-p', '--parallel',
                         action = 'store_true',
                         help = 'process the files in parallel. Note that the suggested value for verbose is 0.' )

    parser.add_argument( '-v', '--verbosity',
                         type = int,
                         choices = [0, 1, 2],
                         action = 'store',
                         default = DEFAULT_verbose,
                         help = 'increase output verbosity. 0 is no output, 1 is output from the function applied to the files, 2 is output from %(prog)s (default is ' + str( DEFAULT_verbose ) + ')' )

    parser.add_argument( '--version',
                         action = 'version',
                         version = '%(prog)s ' + str( VERSION ) )

    args = parser.parse_args()

    if args.outputPath == None:
        args.outputPath = args.inputPath
        if args.verbosity > 0:
            print 'Defaulting output path to', args.outputPath

    if args.nameFormat == None:
        args.nameFormat = DEFAULT_nameFormat
        if args.verbosity > 0:
            print 'Defaulting output name format to', args.nameFormat

    # let
    run( args )
