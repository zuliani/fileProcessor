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

VERSION = '0.1'

# defaults
DEFAULT_varMarker = '$'
DEFAULT_varPrefix = 'FP_'
DEFAULT_varBaseName = 'BASENAME'
DEFAULT_varExtension = 'EXTENSION'
DEFAULT_varInFile = 'IN'
DEFAULT_varOutFile = 'OUT'

#DEFAULT_nameFormat = DEFAULT_varMarker + '{' + DEFAULT_varPrefix + DEFAULT_varBaseName + '}_new' + DEFAULT_varMarker + '{' + DEFAULT_varPrefix + DEFAULT_varExtension + '}'
DEFAULT_nameFormat = ''
DEFAULT_verbose = 1

# globals

# this must be in accordance with the defaults
VAR_REG_EX_FOR_NAME_FORMAT = '\$\{FP_[\w\d]*\}'
VAR_REG_EX_FOR_NAME_FORMAT_COMPILED = re.compile( VAR_REG_EX_FOR_NAME_FORMAT )

# errors
ERROR_INPUT_PATH_DOES_NOT_EXIST = -1
ERROR_INVALID_REGEX = -2
ERROR_INVALID_NAME_FORMAT_LABEL = -3
ERROR_INVALID_NAME_FORMAT = -4

def generateOutputFilename( baseName, extension, nameFormat ):

    if nameFormat == '':
        outputFilename = baseName + extension
    else:

        # detect all the matches to the format variables    
        matchIterator = VAR_REG_EX_FOR_NAME_FORMAT_COMPILED.finditer( nameFormat )

        # and replace them with the corresponding value
        outputFilename = ''
        begin = 0
        oneMatchFound = False
        for m in matchIterator:

            oneMatchFound = True

            outputFilename += nameFormat[begin:m.start()]
            begin = m.end()

            # get the match
            label = nameFormat[m.start() + len( DEFAULT_varPrefix ) + 2:m.end() - 1]

            if label == DEFAULT_varBaseName:
                outputFilename += baseName
            elif label == DEFAULT_varExtension:
                outputFilename += extension
            else:
                print 'The label', label, 'for the format of the output name is invalid'
                sys.exit( ERROR_INVALID_NAME_FORMAT_LABEL )

        if oneMatchFound:
            outputFilename += nameFormat[m.end():]
        else:
            outputFilename = nameFormat

    return outputFilename

def processFile( filename, args ):

    # decompose the input filename
    dirName, name = os.path.split( filename )
    baseName, ext = os.path.splitext( name )

    # generate the output filename
    outputFilename = generateOutputFilename( baseName, ext, args.nameFormat )
    outputFilename = os.path.join( args.outputPath, outputFilename )

    if args.verbosity > 1:
        print 'Processing', filename, ' -> ', outputFilename

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
        print 'Sorry dude. You could be hitting on of these two bugs: http: // bugs.python.org / issue2537 or http: // bugs.python.org / issue214033'
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

    parser = argparse.ArgumentParser( description = 'Process a set of files applying a given function to each of them.',
                                      epilog = 'Note that string containing variables should be included in SINGLE QUOTES in order to avoid bash expansion.' )

    parser.add_argument( '-i', '--inputPath',
                         type = str,
                         action = 'store',
                         help = 'the folder to crawl',
                         required = True )

    parser.add_argument( '-o', '--outputPath',
                         type = str,
                         action = 'store',
                         help = 'the output folder ( default is the same as the input )',
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
                         help = 'name format convenience variables: ' + DEFAULT_varMarker + '{' + DEFAULT_varPrefix + DEFAULT_varBaseName + '} indicates the original file basename, ' + DEFAULT_varMarker + '{' + DEFAULT_varPrefix + DEFAULT_varExtension + '} the original extensions ( default is ' + DEFAULT_nameFormat + ' ). If empty the same name and extension of the input file will be used.',
                         default = DEFAULT_nameFormat )

    parser.add_argument( '-c', '--command',
                         type = str,
                         action = 'store',
                         help = 'the command to apply to the list of files. Note that ' + DEFAULT_varMarker + '{' + DEFAULT_varPrefix + DEFAULT_varInFile + '} denotes the input file, whereas ' + DEFAULT_varMarker + '{' + DEFAULT_varPrefix + DEFAULT_varOutFile + '} denotes the output file.',
                         default = None,
                         required = True )

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
                         help = 'increase output verbosity. 0 is no output, 1 is output from the function applied to the files, 2 is output from %(prog)s ( default is ' + str( DEFAULT_verbose ) + ' )' )

    parser.add_argument( '--version',
                         action = 'version',
                         version = ' % ( prog )s ' + str( VERSION ) )

    args = parser.parse_args()

    if args.outputPath == None:
        args.outputPath = args.inputPath
        if args.verbosity > 0:
            print 'Defaulting output path to', args.outputPath

    # let'd go !
    run( args )
