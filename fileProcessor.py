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
import multiprocessing
import subprocess
import csv
import Queue

VERSION = '0.1'

# levels of verbosity
VERBOSE_NONE = 0x00
VERBOSE_EXEC = 0x01
VERBOSE_FILE_PROCESSOR = 0x01 << 1
VERBOSE_FILE_PROCESSOR_DEBUG = 0x01 << 2

# defaults
DEFAULT_varMarker = '$'
DEFAULT_varPrefix = 'FP_'
DEFAULT_varBaseName = 'BASENAME'
DEFAULT_varExtension = 'EXTENSION'
DEFAULT_varInFile = 'IN'
DEFAULT_varOutFile = 'OUT'

#DEFAULT_nameFormat = DEFAULT_varMarker + '{' + DEFAULT_varPrefix + DEFAULT_varBaseName + '}_new' + DEFAULT_varMarker + '{' + DEFAULT_varPrefix + DEFAULT_varExtension + '}'
DEFAULT_nameFormat = ''
DEFAULT_verbose = VERBOSE_FILE_PROCESSOR
#DEFAULT_logFilename = './fileProcessorLog.csv'
DEFAULT_logFilename = None
DEFAULT_fileFilter = None

# globals

# this must be in accordance with the defaults
VAR_REG_EX_FOR_NAME_FORMAT = '\$\{FP_[\w\d]*\}'
VAR_REG_EX_FOR_NAME_FORMAT_COMPILED = re.compile( VAR_REG_EX_FOR_NAME_FORMAT )

# errors
ERROR_INPUT_PATH_DOES_NOT_EXIST = -1
ERROR_INVALID_REGEX = -2
ERROR_INVALID_NAME_FORMAT_LABEL = -3
ERROR_INVALID_COMMAND_FORMAT_LABEL = -4
ERROR_GENERIC_EXCEPTION = -5

# this class essentially implements the behavior of a static variable
class generateMatchIteratorStatic( object ):
    def __init__( self ):
        self._matchIterator = None
    def __call__( self, regEx, nameFormat ):
        self.compute( regEx, nameFormat )
        return self._matchIterator
    def __iter( self ):
        return self._matchIterator
    def compute( self, regEx, nameFormat ):
        self._matchIterator = regEx.finditer( nameFormat )

generateMatchIterator = generateMatchIteratorStatic()

# see http://stackoverflow.com/questions/287871/print-in-terminal-with-colors-using-python
class Colors:
    EXEC = '\033[94m'
    FILE_PROCESSOR = '\033[92m'
    FILE_PROCESSOR_DEBUG = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

    def disable( self ):
        self.EXEC = ''
        self.FILE_PROCESSOR = ''
        self.FILE_PROCESSOR_DEBUG = ''
        self.FAIL = ''
        self.ENDC = ''

def generateCommand( inOutPair, args ):

    # detect all the matches to the format variables    
    matchIterator = generateMatchIterator( VAR_REG_EX_FOR_NAME_FORMAT_COMPILED, args.command )

    # and replace them with the corresponding value
    cmd = ''
    begin = 0
    for m in matchIterator:

        cmd += args.command[begin:m.start()]
        begin = m.end()

        # get the match
        label = args.command[m.start() + len( DEFAULT_varPrefix ) + 2:m.end() - 1]

        if label == DEFAULT_varInFile:
            cmd += "\"" + inOutPair[0] + "\""
        elif label == DEFAULT_varOutFile:
            cmd += "\"" + inOutPair[1] + "\""
        else:
            return None

    cmd += args.command[m.end():]

    return cmd

def generateOutputFilename( filename, args ):

    # decompose the input filename
    dirName, name = os.path.split( filename )
    baseName, extension = os.path.splitext( name )

    if args.nameFormat == None:
        outputFilename = baseName + extension
    else:

        # detect all the matches to the format variables    
        matchIterator = generateMatchIterator( VAR_REG_EX_FOR_NAME_FORMAT_COMPILED, args.nameFormat )

        # and replace them with the corresponding value
        outputFilename = ''
        begin = 0
        oneMatchFound = False
        for m in matchIterator:

            oneMatchFound = True

            outputFilename += args.nameFormat[begin:m.start()]
            begin = m.end()

            # get the match
            label = args.nameFormat[m.start() + len( DEFAULT_varPrefix ) + 2:m.end() - 1]

            if label == DEFAULT_varBaseName:
                outputFilename += baseName
            elif label == DEFAULT_varExtension:
                outputFilename += extension
            else:
                print 'The label', label, 'for the format of the output name is invalid'
                sys.exit( ERROR_INVALID_NAME_FORMAT_LABEL )

        if oneMatchFound:
            outputFilename += args.nameFormat[m.end():]
        else:
            outputFilename = args.nameFormat

    return os.path.join( args.outputPath, outputFilename )

def worker( inOutPair, args, outputQueue ):

    if args.verbosity & VERBOSE_FILE_PROCESSOR:
        print Colors.FILE_PROCESSOR + 'Processing' + Colors.ENDC, inOutPair[0], Colors.FILE_PROCESSOR + ' -> ' + Colors.ENDC, inOutPair[1]

    cmd = generateCommand( inOutPair, args )
    if args.verbosity & VERBOSE_FILE_PROCESSOR_DEBUG:
        print Colors.FILE_PROCESSOR_DEBUG + 'Executing' + Colors.ENDC, cmd

    proc = subprocess.Popen( [cmd], shell = True, stdout = subprocess.PIPE, stderr = subprocess.PIPE )
    output, errors = proc.communicate()

    outputQueue.put( ( proc, cmd, output, errors ) )
    if args.verbosity & VERBOSE_EXEC:
        print Colors.EXEC + output + Colors.ENDC

def run( args ):

    # check input path
    if not os.path.isdir( args.inputPath ):
        print Colors.FAIL + 'Error: the input path', args.inputPath, 'does not exist' + Colors.ENDC
        sys.exit( ERROR_INPUT_PATH_DOES_NOT_EXIST )

    # check if output path needs to be created
    if not os.path.exists( args.outputPath ):
        os.makedirs( args.outputPath )

    if args.fileFilter:
        try:
            pattern = re.compile( args.fileFilter )
        except:
            print Colors.FAIL + 'The regular expression', args.fileFilter, 'is invalid' + Colors.ENDC
            print Colors.FAIL + 'Sorry dude. You could be hitting on of these two bugs: http://bugs.python.org/issue2537 or http://bugs.python.org/issue214033' + Colors.ENDC
            sys.exit( ERROR_INVALID_REGEX )
    else:
        pattern = None

    # check if we do not need to recurse or not
    inOutPairs = []
    if args.recursive:

        for dirname, dirnames, filenames in os.walk( args.inputPath ):

            for filename in filenames:

                if not pattern or pattern.search( filename ):

                    inputFilename = os.path.join( dirname, filename )
                    outputFilename = generateOutputFilename( inputFilename, args )
                    inOutPair = ( inputFilename, outputFilename )
                    inOutPairs.append( inOutPair )

    else:

        for item in os.listdir( args.inputPath ):

            inputFilename = os.path.join( args.inputPath, item )
            if os.path.isfile( inputFilename ) and ( not pattern or pattern.search( inputFilename ) ):
                outputFilename = generateOutputFilename( inputFilename, args )
                inOutPair = ( inputFilename, outputFilename )
                inOutPairs.append( inOutPair )

    if args.verbosity & VERBOSE_FILE_PROCESSOR:
        print Colors.FILE_PROCESSOR + 'Processing' + Colors.ENDC, str( len( inOutPairs ) ), Colors.FILE_PROCESSOR + 'files' + Colors.ENDC

    # spawn the jobs
    if args.parallel:
        outputQueue = multiprocessing.Queue()
        jobs = []
        for p in inOutPairs:
            process = multiprocessing.Process( target = worker, args = ( p, args, outputQueue, ) )
            jobs.append( process )
            process.start()

        for j in jobs:
            j.join()

    else:
        outputQueue = Queue.Queue()
        for p in inOutPairs:
            worker( p, args, outputQueue )

    # produce the log file
    if args.logFilename:
        fOut = open( args.logFilename, 'wb' )
        writer = csv.writer( fOut, delimiter = ',', quotechar = '"', quoting = csv.QUOTE_ALL )
        writer.writerow( ( '# command', 'stdout', 'stderr' ) )
        while not outputQueue.empty():
            writer.writerow( ( outputQueue.get()[1], outputQueue.get()[2], outputQueue.get()[3] ) )
        fOut.close()

if __name__ == "__main__":

    parser = argparse.ArgumentParser( description = 'Process a set of files applying a given function to each of them.',
                                      epilog = 'Note that string containing variables should be included in SINGLE QUOTES in order to avoid bash expansion.' )

    parser.add_argument( '-i', '--inputPath',
                         type = str,
                         action = 'store',
                         help = 'the folder to traverse',
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
                         help = 'regular expression used to filter the input files (default is ' + str( DEFAULT_fileFilter ) + ')',
                         default = DEFAULT_fileFilter,
                         required = False )

    parser.add_argument( '-n', '--nameFormat',
                         type = str,
                         action = 'store',
                         help = 'name format convenience variables: ' + DEFAULT_varMarker + '{' + DEFAULT_varPrefix + DEFAULT_varBaseName + '} indicates the original file basename, ' + DEFAULT_varMarker + '{' + DEFAULT_varPrefix + DEFAULT_varExtension + '} the original extensions. If empty the same name and extension of the input file will be used.',
                         default = DEFAULT_nameFormat,
                         required = False )

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
                         help = 'process the files in parallel. In this case the suggested verbosity value is ' + str( VERBOSE_NONE ) )


    parser.add_argument( '-l', '--logFilename',
                         type = str,
                         action = 'store',
                         help = 'log CSV file (default is ' + str( DEFAULT_logFilename ) + ')',
                         default = DEFAULT_logFilename,
                         required = False )

    parser.add_argument( '-v', '--verbosity',
                         type = int,
                         action = 'store',
                         default = DEFAULT_verbose,
                         help = 'bit field value to specify the output verbosity: ' + str( VERBOSE_NONE ) + ' is no output, ' + str( VERBOSE_EXEC ) + ' is output from the function applied to the files, ' + str( VERBOSE_FILE_PROCESSOR ) + ' is output from %(prog)s, ' + str( VERBOSE_FILE_PROCESSOR_DEBUG ) + ' is further debug info ( default is ' + str( DEFAULT_verbose ) + ' )' )

    parser.add_argument( '--version',
                         action = 'version',
                         version = ' %(prog)s ' + str( VERSION ) )

    args = parser.parse_args()

    if args.outputPath == None:
        args.outputPath = args.inputPath
        if args.verbosity & VERBOSE_FILE_PROCESSOR:
            print Colors.FILE_PROCESSOR + 'Defaulting output path to' + Colors.ENDC, args.outputPath

    # let'd go !
    run( args )
