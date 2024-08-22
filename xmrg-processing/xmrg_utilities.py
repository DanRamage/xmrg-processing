import os
import re
from datetime import datetime, timedelta
import time
import logging.config
import requests

logger = logging.getLogger()

def get_collection_date_from_filename(fileName):
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
    if len(fileParts):
        # Now let's manipulate the string to match the dateformat var above.
        filetime = re.sub("xmrg_", "", fileParts[0])
        filetime = re.sub("_", "", filetime)
    else:
        if filetime.find('24hrxmrg') != -1:
            dateformat = "%m%d%Y"
        filetime = filetime.replace('24hrxmrg', '')
        filetime = filetime.replace('xmrg', '')
        filetime = filetime.replace('z', '')
    # Using mktime() and localtime() is a hack. The time package in python doesn't have a way
    # to convert a struct_time in UTC to epoch secs. So I just use the local time functions to do what
    # I want instead of brining in the calander package which has the conversion.
    secs = time.mktime(time.strptime(filetime, dateformat))
    # secs -= offset
    filetime = time.strftime("%Y-%m-%dT%H:00:00", time.localtime(secs))

    return filetime


def file_list_from_date_range(start_date_time, hour_count, xmrg_file_extension='gz'):
    """
    Function: file_list_from_date_range
    Purpose: Given the starting date and the number of hours in the past, this builds a list
     of the xmrg filenames.
    Parameters:
      start_date_time: A datetime object representing the starting time.
      hour_count: An integer for the number of previous hours we want to build file names for.
    Return:
      A list containing the filenames.
    """
    file_list = []
    for x in range(hour_count):
        hr = x + 1
        date_time = start_date_time - timedelta(hours=hr)
        try:
            file_name = build_filename(date_time, xmrg_file_extension)
        except Exception as e:
            logger.exception(e)
        else:
            file_list.append(file_name)

    return file_list

def build_filename(date_time, xmrg_file_ext):
    try:
        file_name = date_time.strftime('xmrg%m%d%Y%Hz')
        file_name = f"{file_name}.{xmrg_file_ext}"
        return file_name
    except Exception as e:
        raise e


def http_download_file(download_url, file_name, destination_directory):
    start_time = time.time()
    remote_filename_url = os.path.join(download_url, file_name)
    logger.info("Downloading file: %s" % (remote_filename_url))
    try:
        r = requests.get(remote_filename_url, stream=True)
    except (requests.HTTPError, requests.ConnectionError, Exception) as e:
        logger.exception(e)
    else:
        if r.status_code == 200:
            dest_file = os.path.join(destination_directory, file_name)
            logger.info(f"Saving to file: {dest_file}")
            try:
                with open(dest_file, 'wb') as xmrg_file:
                    for chunk in r:
                        xmrg_file.write(chunk)
                    logger.info(f"Downloaded file: {dest_file} in {time.time() - start_time} seconds.")
            except IOError as e:
                if logger:
                    logger.exception(e)
            return dest_file
        else:
            logger.error(f"Unable to download file: {remote_filename_url}")
    return None

def download_files(file_list: str, destination_directory: str, download_url: str):
    downloaded_files = []
    for file_name in file_list:
        downloaded_files.append(http_download_file(download_url, file_name, destination_directory))
    return downloaded_files