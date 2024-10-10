import sys
import os
import time
import pandas as pd
import geopandas as gpd
import array
import struct
import re

from shapely.geometry import Polygon
import logging
import logging.handlers
import gzip
import shutil
import math


class hrapCoord(object):
    def __init__(self, column=None, row=None):
        self.column = column
        self.row = row


class LatLong(object):
    def __init__(self, lat=None, long=None):
        self.latitude = lat
        self.longitude = long


class geoXmrg:
    def __init__(self, minimum_lat_lon, maximum_lat_lon, data_multiplier=0.01):
        self.logger = logging.getLogger()

        self.fileName = ''
        self.lastErrorMsg = ''
        self.headerRead = False

        self.earthRadius = 6371.2
        self.startLong = 105.0
        self.startLat = 60.0
        self.xmesh = 4.7625
        self.meshdegs = (self.earthRadius * (1.0 + math.sin(math.radians(self.startLat)))) / self.xmesh

        self._minimum_lat_lon = minimum_lat_lon
        self._maximum_lat_lon = maximum_lat_lon
        self._data_multiplier = data_multiplier

    """
      Function: Reset
      Purpose: Prepares the xmrgFile object for reuse. Resets various variables and closes the currently open file object.
      Parameters: None
      Return: None
      """

    def Reset(self):
        self.fileName = ''
        self.lastErrorMsg = ''
        self.xmrgFile.close()

    def uncompress(self, file_name: str):
        directory, xmrg_filename = os.path.split(file_name)
        xmrg_filename, xmrg_extension = os.path.splitext(xmrg_filename)
        # Is the file compressed? If so, we want to uncompress it to a file for use.
        # The reason for not working with the GzipFile object directly is it is not compatible
        # with the array.fromfile() functionality.
        if xmrg_extension == 'gz':
            self.compressedFilepath = file_name
            try:
                self.fileName = xmrg_filename
                with gzip.GzipFile(file_name, 'rb') as zipFile, open(self.fileName, mode='wb') as self.xmrgFile:
                    shutil.copyfileobj(zipFile, self.xmrgFile)
            except (IOError, Exception) as e:
                raise e
        return
    def openFile(self, filePath):
        '''
        Purpose: Attempts to open the file given in the filePath string. If the file is compressed using gzip, this will uncompress
          the file as well.

        :param filePath: is a string with the full path to the file to open.
        :return:
        '''
        self.fileName = filePath
        self.compressedFilepath = ''
        try:
            self.uncompress(self.fileName)
            '''
            # Is the file compressed? If so, we want to uncompress it to a file for use.
            # The reason for not working with the GzipFile object directly is it is not compatible
            # with the array.fromfile() functionality.
            if (self.fileName.rfind('gz') != -1):
                self.compressedFilepath = self.fileName
                # SPlit the filename from the extension.
                parts = self.fileName.split('.')
                if sys.version_info[0] < 3:
                    try:
                        zipFile = gzip.GzipFile(filePath, 'rb')
                        contents = zipFile.read()
                    except Exception as e:
                        raise e
                    else:
                        self.fileName = parts[0]
                        self.xmrgFile = open(self.fileName, mode='wb')
                        self.xmrgFile.writelines(contents)
                        self.xmrgFile.close()
                else:
                    try:
                        self.fileName = parts[0]
                        with gzip.GzipFile(filePath, 'rb') as zipFile, open(self.fileName, mode='wb') as self.xmrgFile:
                            shutil.copyfileobj(zipFile, self.xmrgFile)
                    except (IOError, Exception) as e:
                        if self.logger:
                            self.logger.error("Does not appear to be valid gzip file. Attempting normal open.")
                            self.logger.exception(e)
            '''
            self.xmrgFile = open(self.fileName, mode='rb')
        except Exception as e:
            self.logger.exception(e)
            raise e

    """
   Function: cleanUp
   Purpose: Called to delete the XMRG file that was just worked with. Can delete the uncompressed file and/or 
    the source compressed file. 
   Parameters:
     deleteFile if True, will delete the unzipped binary file.
     deleteCompressedFile if True, will delete the compressed file the working file was extracted from.
    """

    def cleanUp(self, deleteFile, deleteCompressedFile):
        self.xmrgFile.close()
        if (deleteFile):
            os.remove(self.fileName)
        if (deleteCompressedFile and len(self.compressedFilepath)):
            os.remove(self.compressedFilepath)
        return

    """
    Function: readFileHeader
    Purpose: For the open file, reads the header. Call this function first before attempting to use readRow or readAllRows.
      If you don't the file pointer will not be at the correct position.
    Parameters: None
    Returns: True if successful, otherwise False.
    """

    def readFileHeader(self):
        try:
            # Determine if byte swapping is needed.
            # From the XMRG doc:
            # FORTRAN unformatted records have a 4 byte integer at the beginning and
            # end of each record that is equal to the number of 4 byte words
            # contained in the record.  When reading xmrg files through C using the
            # fread function, the user must account for these extra bytes at the
            # beginning and end of each  record.

            # Original header is as follows
            # 4 byte integer for num of 4 byte words in record
            # int representing HRAP-X coord of southwest corner of grid(XOR)
            # int representing HRAP-Y coord of southwest corner of grid(YOR)
            # int representing HRAP grid boxes in X direction (MAXX)
            # int representing HRAP grid boxes in Y direction (MAXY)
            header = array.array('I')
            # read 6 bytes since first int is the header, next 4 ints are the grid data, last int is the tail.
            header.fromfile(self.xmrgFile, 6)
            self.swapBytes = 0
            # Determine if byte swapping is needed
            if (header[0] != 16):
                self.swapBytes = 1
                header.byteswap()

            self.XOR = header[1]  # X Origin of the HRAP grid
            self.YOR = header[2]  # Y origin of the HRAP grid
            self.MAXX = header[3]  # Number of columns in the data
            self.MAXY = header[4]  # Number of rows in the data

            # reset the array
            header = array.array('I')
            # Read the fotran header for the next block of data. Need to determine which header type we'll be reading
            header.fromfile(self.xmrgFile, 1)
            if (self.swapBytes):
                header.byteswap()

            self.fileNfoHdrData = ''
            byteCnt = header[0]
            unpackFmt = ''
            hasDataNfoHeader = True
            srcFileOpen = False
            # Header for files written 1999 to present.
            if (byteCnt == 66):
                # The info header has the following layout
                # Operating system: char[2]
                # user id: char[8]
                # saved date: char[10]
                # saved time: char[10]
                # process flag: char[20]
                # valid date: char[10]
                # valid time: char[10]
                # max value: int
                # version number: float
                unpackFmt += '=2s8s10s10s8s10s10sif'
                # buf = array.array('B')
                # buf.fromfile(self.xmrgFile,66)
                # if( self.swapBytes ):
                #  buf.byteswap()

                buf = self.xmrgFile.read(66)

                self.fileNfoHdrData = struct.unpack(unpackFmt, buf)
                srcFileOpen = True
            # Files written June 1997 to 1999
            elif (byteCnt == 38):
                if (self.swapBytes):
                    unpackFmt += '>'
                unpackFmt += '=10s10s10s8s'
                buf = self.xmrgFile.read(38)
                self.fileNfoHdrData = struct.unpack(unpackFmt, buf)
                srcFileOpen = True

            # Files written June 1997 to 1999. I assume there was some bug for this since the source
            # code also was writing out an error message.
            elif byteCnt == 37:
                if self.swapBytes:
                    unpackFmt += '>'
                unpackFmt += '=10s10s10s8s'
                buf = self.xmrgFile.read(37)
                self.fileNfoHdrData = struct.unpack(unpackFmt, buf)
                srcFileOpen = True

            # Files written up to June 1997, no 2nd header.
            elif byteCnt == (self.MAXX * 2):
                if (self.swapBytes):
                    unpackFmt += '>'
                self.logger.info("Reading pre-1997 format")
                srcFileOpen = True
                # File does not have 2nd header, so we need to reset the file point to the point before we
                # did the read for the 2nd header tag.
                self.xmrgFile.seek(24, os.SEEK_SET)
                hasDataNfoHeader = False

            # Invalid byte count.
            else:
                self.lastErrorMsg = 'Header is unknown format, cannot continue.'
                return (False)

            # If the file we are reading was not a pre June 1997, we read the tail int,
            # should be equal to byteCnt
            if (hasDataNfoHeader):
                header = array.array('I')
                header.fromfile(self.xmrgFile, 1)
                if (self.swapBytes):
                    header.byteswap()
                if (header[0] != byteCnt):
                    self.lastErrorMsg = 'ERROR: tail byte cnt does not equal head.'
                    return (False)

            if (srcFileOpen):
                self.headerRead = True
                return (True)

        except Exception as E:
            import traceback
            self.lastErrorMsg = traceback.format_exc()

            if (self.logger != None):
                self.logger.error(self.lastErrorMsg)
            else:
                print(self.lastErrorMsg)

        return (False)

    """
    Function: readRecordTag
    Purpose: Reads the tag that surrounds each record in the file.
    Parameters: None
    Return: An integer dataArray with the tag data if read, otherwise None.
    """

    def readRecordTag(self):
        dataArray = array.array('I')
        dataArray.fromfile(self.xmrgFile, 1)
        if (self.swapBytes):
            dataArray.byteswap()
        # Verify the header for this row of data matches what the header specified.
        # We do MAXX * 2 since each value is a short.
        if (dataArray[0] != (self.MAXX * 2)):
            # self.lastErrorMsg = 'Trailing tag Byte count: %d for row: %d does not match header: %d.' % (
            # dataArray[0], row, self.MAXX)
            return (None)
        return (dataArray)

    """
      Function: readRow
      Purpose: Reads a single row from the file.
      Parameters: None'
      Returns: If successful a dataArray containing the row values, otherwise None.
      """

    def readRow(self):
        # Read off the record header
        tag = self.readRecordTag()
        if (tag == None):
            return (None)

        # Read a columns worth of data out
        dataArray = array.array('h')
        dataArray.fromfile(self.xmrgFile, self.MAXX)
        # Need to byte swap?
        if (self.swapBytes):
            dataArray.byteswap()

        # Read off the record footer.
        tag = self.readRecordTag()
        if (tag == None):
            return (None)

        return (dataArray)

    """
      Function: readAllRows
      Purpose: Reads all the rows in the file and stores them in a dataArray object. Data is stored in self.grid.
      Parameters: None
      Returns: True if succesful otherwise False.
    
      """

    def readAllRows(self):

        start_col = 0
        start_row = 0
        end_col = self.MAXX
        end_row = self.MAXY
        if self._minimum_lat_lon is not None and self._maximum_lat_lon is not None:
            llHrap = self.latLongToHRAP(self._minimum_lat_lon, True, True)
            urHrap = self.latLongToHRAP(self._maximum_lat_lon, True, True)
            start_row = llHrap.row
            start_col = llHrap.column
            end_row = urHrap.row
            end_col = urHrap.column

        grid = []
        # Create a integer numeric array(from numpy). Dimensions are MAXY and MAXX.
        for row in range(self.MAXY):
            dataArray = self.readRow()
            if (dataArray == None):
                return (False)
            if row >= start_row and row < end_row:
                for col in range(start_col, end_col):
                    val = dataArray[col] * self._data_multiplier
                    hrap = hrapCoord(self.XOR + col, self.YOR + row)
                    latlon = self.hrapCoordToLatLong(hrap)
                    latlon.longitude *= -1
                    # Build polygon points. Each grid point represents a 4km square, so we want to create a polygon
                    # that has each point in the grid for a given point.
                    hrapNewPt = hrapCoord(self.XOR + col, self.YOR + row + 1)
                    latlonUL = self.hrapCoordToLatLong(hrapNewPt)
                    latlonUL.longitude *= -1

                    hrapNewPt = hrapCoord(self.XOR + col + 1, self.YOR + row)
                    latlonBR = self.hrapCoordToLatLong(hrapNewPt)
                    latlonBR.longitude *= -1

                    hrapNewPt = hrapCoord(self.XOR + col + 1, self.YOR + row + 1)
                    latlonUR = self.hrapCoordToLatLong(hrapNewPt)
                    latlonUR.longitude *= -1

                    grid_polygon = Polygon([(latlon.longitude, latlon.latitude),
                                            (latlonUL.longitude, latlonUL.latitude),
                                            (latlonUR.longitude, latlonUR.latitude),
                                            (latlonBR.longitude, latlonBR.latitude),
                                            (latlon.longitude, latlon.latitude)])

                    grid.append([grid_polygon, val])
        data_frame = pd.DataFrame(grid, columns=['Grids', 'Precipitation'])
        geo_data_frame = gpd.GeoDataFrame(data_frame,
                                          geometry=data_frame.Grids)
        self._geo_data_frame = geo_data_frame.drop(columns=['Grids'])
        self._geo_data_frame.set_crs(epsg=4326, inplace=True)
        return (True)

    def save_to_file(self, filename):
        try:
            self._geo_data_frame.to_file(filename, driver="GeoJSON")
        except Exception as e:
            raise e

    """
      Function: inBBOX
      Purpose: Tests to see if the testLatLong is in the bounding box given by minLatLong and maxLatLong.
      Parameters:
        testLatLong is the lat/long pair we are testing.
        minLatLong is a latLong object representing the bottom left corner.
        maxLatLong is a latLong object representing the upper right corner.
      Returns:
        True if the testLatLong is in the bounding box, otherwise False.
      """

    def inBBOX(self, testLatLong, minLatLong, maxLatLong):
        inBBOX = False
        if ((testLatLong.latitude >= minLatLong.latitude and testLatLong.longitude >= minLatLong.longitude) and
                (testLatLong.latitude < maxLatLong.latitude and testLatLong.longitude < maxLatLong.longitude)):
            inBBOX = True
        return (inBBOX)

    """
    Function: hrapCoordToLatLong
    Purpose: Converts the HRAP grid point given in hrapPoint into a latitude and longitude.
    Parameters:  
      hrapPoint is an hrapPoint object that defines the row,col point we are converting.
    Returns:
      A LatLong() object with the converted data.
    """

    def hrapCoordToLatLong(self, hrapPoint):
        latLong = LatLong()

        x = hrapPoint.column - 401.0;
        y = hrapPoint.row - 1601.0;
        rr = x * x + y * y
        # gi = ((self.earthRadius * (1.0 + math.sin(self.tlat))) / self.xmesh)
        # gi *= gi
        # gi = ((self.earthRadius * (1.0 + math.sin(math.radians(self.startLat)))) / self.xmesh)
        gi = self.meshdegs * self.meshdegs
        # latLong.latitude = math.asin((gi - rr) / (gi + rr)) * self.raddeg
        latLong.latitude = math.degrees(math.asin((gi - rr) / (gi + rr)))

        # ang = math.atan2(y,x) * self.raddeg
        ang = math.degrees(math.atan2(y, x))

        if (ang < 0.0):
            ang += 360.0;
        latLong.longitude = 270.0 + self.startLong - ang;

        if (latLong.longitude < 0.0):
            latLong.longitude += 360.0;
        elif (latLong.longitude > 360.0):
            latLong.longitude -= 360.0;

        return (latLong)

    """
    Function: latLongToHRAP
    Purpose: Converts a latitude and longitude into an HRAP grid point.
    Parameters:  
      latLong is an latLong object that defines the point we are converting.
      roundToNearest specifies if we want to round the hrap point to the nearest integer value.
      adjustToOrigin specifies if we want to adjust the hrap point to the origin of the file.
    Returns:
      A LatLong() object with the converted data.
    """

    def latLongToHRAP(self, latLong, roundToNearest=False, adjustToOrigin=False):
        flat = math.radians(latLong.latitude)
        flon = math.radians(abs(latLong.longitude) + 180.0 - self.startLong)
        r = self.meshdegs * math.cos(flat) / (1.0 + math.sin(flat))
        x = r * math.sin(flon)
        y = r * math.cos(flon)
        hrap = hrapCoord(x + 401.0, y + 1601.0)

        # Bounds checking
        if (hrap.column > (self.XOR + self.MAXX)):
            hrap.column = self.XOR + self.MAXX
        if (hrap.row > (self.YOR + self.MAXY)):
            hrap.row = self.YOR + self.MAXY
        if (roundToNearest):
            hrap.column = int(hrap.column - 0.5)
            hrap.row = int(hrap.row - 0.5)
        if (adjustToOrigin):
            hrap.column -= self.XOR
            hrap.row -= self.YOR

        return (hrap)

    """
    Function: getCollectionDateFromFilename
    Purpose: Given the filename, this will return a datetime string in the format of YYYY-MM-DDTHH:MM:SS.
    Parameters:
      fileName is the xmrg filename to parse the datetime from.
    Return:
      A string representing the date and time in the form: YYYY-MM-DDTHH:MM:SS
    """

    def getCollectionDateFromFilename(self, fileName):
        # Parse the filename to get the data time.
        (directory, filetime) = os.path.split(fileName)
        (filetime, ext) = os.path.splitext(filetime)
        # Let's get rid of the xmrg verbage so we have the time remaining.
        # The format for the time on these files is MMDDYYY sometimes a trailing z or for some historical
        # files, the format is xmrg_MMDDYYYY_HRz_SE. The SE could be different for different regions, SE is southeast.
        # 24 hour files don't have the z, or an hour

        dateformat = "%m%d%Y%H"
        # Regexp to see if we have one of the older filename formats like xmrg_MMDDYYYY_HRz_SE
        fileParts = re.findall("xmrg_\d{8}_\d{1,2}", filetime)
        if (len(fileParts)):
            # Now let's manipulate the string to match the dateformat var above.
            filetime = re.sub("xmrg_", "", fileParts[0])
            filetime = re.sub("_", "", filetime)
        else:
            if (filetime.find('24hrxmrg') != -1):
                dateformat = "%m%d%Y"
            filetime = filetime.replace('24hrxmrg', '')
            filetime = filetime.replace('xmrg', '')
            filetime = filetime.replace('z', '')
        # Using mktime() and localtime() is a hack. The time package in python doesn't have a way
        # to convert a struct_time in UTC to epoch secs. So I just use the local time functions to do what
        # I want instead of brining in the calender package which has the conversion.
        secs = time.mktime(time.strptime(filetime, dateformat))
        # secs -= offset
        filetime = time.strftime("%Y-%m-%dT%H:00:00", time.localtime(secs))

        return (filetime)
