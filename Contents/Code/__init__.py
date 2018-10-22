############################################################################
# This plugin will allow external calls, that the plugin can then handle
# See TODO doc for more details
#
# Made by
# dane22 & digitalhigh...Plex Community members
#
############################################################################

from __future__ import print_function

import StringIO
import datetime
import glob
import os
import sys
import threading
import time
import xml.etree.ElementTree as ET
from zipfile import ZipFile, ZIP_DEFLATED

import pychromecast
from helpers import PathHelper
from helpers.system import SystemHelper
from helpers.variable import pms_path
from pychromecast.controllers.media import MediaController
from pychromecast.controllers.plex import PlexController
from subzero.lib.io import FileIO

import log_helper
from CustomContainerOriginal import MediaContainer, DeviceContainer, CastContainer, ZipObject, StatusContainer, \
    MetaContainer
from CustomContainer import AnyContainer
from flex_container import FlexContainer, JsonContainer
from lib import Plex

UNICODE_MAP = {
    65535: 'ucs2',
    1114111: 'ucs4'
}

META_TYPE_IDS = {
    1: "movie",
    2: "show",
    3: "season",
    4: "episode",
    8: "artist",
    9: "album",
    10: "track",
    12: "extra",
    13: "photo",
    15: "playlist",
    18: "collection"
}

TAG_TYPE_ARRAY = {
    1: "genre",
    4: "director",
    5: "writer",
    6: "actor"
}


META_TYPE_NAMES = dict(map(reversed, META_TYPE_IDS.items()))

DEFAULT_CONTAINER_SIZE = 100000
DEFAULT_CONTAINER_START = 0
DATE_STRUCTURE = "%Y-%m-%d %H:%M:%S"
pms_path = pms_path()
Log.Debug("New PMS Path iss '%s'" % pms_path)
dbPath = os.path.join(pms_path, "Plug-in Support", "Databases", "com.plexapp.plugins.library.db")
Log.Debug("Setting DB path to '%s'" % dbPath)
os.environ['LIBRARY_DB'] = dbPath
os.environ["PMS_PATH"] = pms_path

os_platform = False
path = None
# Dummy Imports for PyCharm

# import Framework.context
# from Framework.api.objectkit import ObjectContainer, DirectoryObject
# from Framework.docutils import Plugin, HTTP, Log, Request
# from Framework.docutils import Data

NAME = 'Flex TV'
VERSION = '1.1.106'
APP_PREFIX = '/applications/Cast'
CAST_PREFIX = '/chromecast'
STAT_PREFIX = '/stats'
ICON = 'flextv.png'
ICON_CAST = 'icon-cast.png'
ICON_CAST_AUDIO = 'icon-cast_audio.png'
ICON_CAST_VIDEO = 'icon-cast_video.png'
ICON_CAST_GROUP = 'icon-cast_group.png'
ICON_CAST_REFRESH = 'icon-cast_refresh.png'
ICON_PLEX_CLIENT = 'icon-plex_client.png'
TEST_CLIP = 'test.mp3'
PLUGIN_IDENTIFIER = "com.plexapp.plugins.FlexTV"


# Start function
def Start():
    Plugin.AddViewGroup("Details", viewMode="InfoList", mediaType="items")
    distribution = None
    libraries_path = os.path.join(pms_path, "Plug-ins", "FlexTV.bundle", "Contents", "Libraries")
    loaded = insert_paths(distribution, libraries_path)
    if loaded:
        Log.Debug("Paths should be loaded!")
        os.environ["Loaded"] = "True"
    else:
        Log.Debug("Unable to load paths")
        os.environ["Loaded"] = "False"
    ObjectContainer.title1 = NAME
    DirectoryObject.thumb = R(ICON)
    HTTP.CacheTime = 5
    if Data.Exists('device_json') is not True:
        UpdateCache()

    ValidatePrefs()
    CacheTimer()
    RestartTimer()



def CacheTimer():
    mins = 60
    update_time = mins * 60
    Log.Debug("Cache timer started, updating in %s minutes, man", mins)
    threading.Timer(update_time, CacheTimer).start()
    UpdateCache()


def RestartTimer():
    hours = 4
    restart_time = hours * 60 * 60
    Log.Debug("Restart timer started, plugin will re-start in %s hours.", hours)
    threading.Timer(restart_time, DispatchRestart).start()


def UpdateCache():
    Log.Debug("UpdateCache called")
    scan_devices()


@handler(APP_PREFIX, NAME)
@handler(CAST_PREFIX, NAME)
@handler(STAT_PREFIX, NAME)
@route(APP_PREFIX + '/MainMenu')
def MainMenu(Rescanned=False):
    """
    Main menu
    and stuff
    """
    Log.Debug("**********  Starting MainMenu  **********")
    title = NAME + " - " + VERSION
    if Data.Exists('last_scan'):
        title = NAME + " - " + Data.Load('last_scan')

    oc = ObjectContainer(
        title1=title,
        no_cache=True,
        no_history=True,
        title_bar="Flex TV",
        view_group="Details")

    if Rescanned is True:
        oc.message = "Rescan complete!"

    #
    do = DirectoryObject(
        title="Rescan Devices",
        thumb=R(ICON_CAST_REFRESH),
        key=Callback(Rescan))

    oc.add(do)

    do = DirectoryObject(
        title="Advanced",
        thumb=R(ICON_CAST_REFRESH),
        key=Callback(AdvancedMenu))

    oc.add(do)

    do = DirectoryObject(
        title="Devices",
        thumb=R(ICON_CAST),
        key=Callback(Resources))

    oc.add(do)

    do = DirectoryObject(
        title="Broadcast",
        thumb=R(ICON_CAST_AUDIO),
        key=Callback(Broadcast))

    oc.add(do)

    do = DirectoryObject(
        title="Stats",
        thumb=R(ICON_PLEX_CLIENT),
        key=Callback(Statmenu))

    oc.add(do)

    return oc


@route(APP_PREFIX + '/ValidatePrefs')
def ValidatePrefs():
    """
    Called by the framework every time a user changes the prefs
    We add this dummy function, to avoid errors in the log
    and stuff.
    """

    dependencies = ["helpers"]
    log_helper.register_logging_handler(dependencies, level="DEBUG")
    return


####################################
# These are our cast endpoints
@route(APP_PREFIX + '/devices')
@route(CAST_PREFIX + '/devices')
def Devices():
    """

    Endpoint to scan LAN for cast devices
    """
    Log.Debug('Recieved a call to fetch cast devices')
    # Grab our response header?
    casts = fetch_devices()
    mc = MediaContainer()
    for cast in casts:
        Log.Debug("Cast type is " + cast['type'])
        if (cast['type'] == 'cast') | (cast['type'] == 'audio') | (cast['type'] == 'group'):
            dc = CastContainer(cast)
            mc.add(dc)

    return mc


@route(APP_PREFIX + '/clients')
@route(CAST_PREFIX + '/clients')
def Clients():
    """
    Endpoint to scan LAN for cast devices
    """
    Log.Debug('Recieved a call to fetch all devices')
    # Grab our response header?
    casts = fetch_devices()

    mc = MediaContainer()
    for cast in casts:
        dc = CastContainer(cast)
        mc.add(dc)

    return mc


@route(APP_PREFIX + '/resources')
@route(CAST_PREFIX + '/resources')
def Resources():
    """
    Endpoint to scan LAN for cast devices
    """
    Log.Debug('Recieved a call to fetch devices')
    # Grab our response header?
    casts = fetch_devices()

    oc = ObjectContainer(
        no_cache=True,
        no_history=True,
        view_group="Details")

    for cast in casts:
        cast_type = cast['type']
        icon = ICON_CAST
        if cast_type == "audio":
            icon = ICON_CAST_AUDIO
        if cast_type == "cast":
            icon = ICON_CAST_VIDEO
        if cast_type == "group":
            icon = ICON_CAST_GROUP
        if cast['app'] == "Plex Client":
            icon = ICON_PLEX_CLIENT
        do = DirectoryObject(
            title=cast['name'],
            duration=cast['status'],
            tagline=cast['uri'],
            summary=cast['app'],
            key=Callback(Status, input_name=cast['name']),
            thumb=R(icon))
        oc.add(do)

    return oc


@route(APP_PREFIX + '/rescan')
@route(CAST_PREFIX + '/rescan')
def Rescan():
    """
    Endpoint to scan LAN for cast devices
    """
    Log.Debug('Recieved a call to rescan devices')
    # Grab our response header?
    UpdateCache()
    return MainMenu(True)


@route(CAST_PREFIX + '/play')
def Play():
    """
    Endpoint to play media.
    """
    Log.Debug('Recieved a call to play media.')
    params = ['Clienturi', 'Contentid', 'Contenttype', 'Serverid', 'Serveruri',
              'Username', 'Transienttoken', 'Queueid', 'Version', 'Primaryserverid',
              'Primaryserveruri', 'Primaryservertoken']
    values = sort_headers(params, False)
    status = "Missing required headers and stuff"
    msg = status

    if values is not False:
        Log.Debug("Holy crap, we have all the headers we need.")
        client_uri = values['Clienturi'].split(':')
        host = client_uri[0]
        port = int(client_uri[1])
        pc = False
        msg = "No message received"
        if 'Serverid' in values:
            servers = fetch_servers()
            for server in servers:
                if server['id'] == values['Serverid']:
                    Log.Debug("Found a matching server!")
                    values['Serveruri'] = server['uri']
                    values['Version'] = server['version']

        try:
            cast = pychromecast.Chromecast(host, port)
            cast.wait()
            values['Type'] = cast.cast_type
            pc = PlexController(cast)
            cast.register_handler(pc)
            Log.Debug("Sending values to play command: " + JSON.StringFromObject(values))
            pc.play_media(values, log_data)
        except pychromecast.LaunchError, pychromecast.PyChromecastError:
            Log.Debug('Error connecting to host.')
            status = "Error"
        finally:
            if pc is not False:
                status = "Success"

    oc = MediaContainer({
        'Name': 'Playback Status',
        'Status': status,
        'Message': msg
    })

    return oc


@route(CAST_PREFIX + '/cmd')
def Cmd():
    """
    Media control command(s).

    Plex-specific commands use the format:


    Required params:
    Uri
    Cmd
    Vol(If setting volume, otherwise, ignored)

    Where <COMMAND> is one of:
    PLAY (resume)
    PAUSE
    STOP
    STEPFORWARD
    STEPBACKWARD Need to test, not in PHP cast app)
    PREVIOUS
    NEXT
    MUTE
    UNMUTE
    VOLUME - also requires an int representing level from 0-100

    """
    Log.Debug('Recieved a call to control playback')
    params = sort_headers(['Uri', 'Cmd', 'Val'], False)
    status = "Missing paramaters"
    response = "Error"

    if params is not False:
        uri = params['Uri'].split(":")
        cast = pychromecast.Chromecast(uri[0], int(uri[1]))
        cast.wait()
        pc = PlexController(cast)
        Log.Debug("Handler namespace is %s" % pc.namespace)
        cast.register_handler(pc)

        Log.Debug("Handler namespace is %s" % pc.namespace)

        cmd = params['Cmd']
        Log.Debug("Command is " + cmd)

        if cmd == "play":
            pc.play()
        if cmd == "pause":
            pc.pause()
        if cmd == "stop":
            pc.stop()
        if cmd == "next":
            pc.next()
        if (cmd == "offset") & ('Val' in params):
            pc.seek(params["Val"])
        if cmd == "previous":
            pc.previous()
        if cmd == "volume.mute":
            pc.mute(True)
        if cmd == "volume.unmute":
            pc.mute(False)
        if (cmd == "volume") & ('Val' in params):
            pc.set_volume(params["Val"])
        if cmd == "volume.down":
            pc.volume_down()
        if cmd == "volume.up":
            pc.volume_up()

        cast.disconnect()
        response = "Command successful"

    oc = ObjectContainer(
        title1=response,
        title2=status,
        no_cache=True,
        no_history=True)
    return oc


@route(CAST_PREFIX + '/audio')
def Audio():
    """
    Endpoint to cast audio to a specific device.
    """

    Log.Debug('Recieved a call to play an audio clip.')
    params = ['Uri', 'Path']
    values = sort_headers(params, True)
    status = "Missing required headers"
    if values is not False:
        Log.Debug("Holy crap, we have all the headers we need.")
        client_uri = values['Uri'].split(":")
        host = client_uri[0]
        port = int(client_uri[1])
        path = values['Path']
        try:
            cast = pychromecast.Chromecast(host, port)
            cast.wait()
            mc = cast.media_controller
            mc.play_media(path, 'audio/mp3', )
        except pychromecast.LaunchError, pychromecast.PyChromecastError:
            Log.Debug('Error connecting to host.')
        finally:
            Log.Debug("We have a cast")
            status = "Playback successful"

    oc = ObjectContainer(
        title1=status,
        no_cache=True,
        no_history=True)

    return oc


@route(CAST_PREFIX + '/broadcast/test')
def Test():
    values = {'Path': R(TEST_CLIP)}
    casts = fetch_devices()
    status = "Test successful!"
    try:
        for cast in casts:
            if cast['type'] == "audio":
                mc = MediaController()
                Log.Debug("We should be broadcasting to " + cast['name'])
                uri = cast['uri'].split(":")
                cast = pychromecast.Chromecast(uri[0], int(uri[1]))
                cast.wait()
                cast.register_handler(mc)
                mc.play_media(values['Path'], 'audio/mp3')

    except pychromecast.LaunchError, pychromecast.PyChromecastError:
        Log.Debug('Error connecting to host.')
        status = "Test failed!"
    finally:
        Log.Debug("We have a cast")

    oc = ObjectContainer(
        title1=status,
        no_cache=True,
        no_history=True)

    return oc


@route(CAST_PREFIX + '/broadcast')
def Broadcast():
    """
    Send audio to *all* cast devices on the network
    """
    Log.Debug('Recieved a call to broadcast an audio clip.')
    params = ['Path']
    values = sort_headers(params, True)
    status = "No clip specified"
    if values is not False:
        do = False
        casts = fetch_devices()
        disconnect = []
        controllers = []
        try:
            for cast in casts:
                if cast['type'] == "audio":
                    mc = MediaController()
                    Log.Debug("We should be broadcasting to " + cast['name'])
                    uri = cast['uri'].split(":")
                    cast = pychromecast.Chromecast(uri[0], int(uri[1]))
                    cast.wait()
                    cast.register_handler(mc)
                    controllers.append(mc)
                    disconnect.append(cast)

            for mc in controllers:
                mc.play_media(values['Path'], 'audio/mp3', )

        except pychromecast.LaunchError, pychromecast.PyChromecastError:
            Log.Debug('Error connecting to host.')
        finally:
            for cast in disconnect:
                cast.disconnect()
            Log.Debug("We have a cast")

    else:
        do = DirectoryObject(
            title='Test Broadcast',
            tagline="Send a test broadcast to audio devices.",
            key=Callback(Test))
        status = "Foo"

    oc = ObjectContainer(
        title1=status,
        no_cache=True,
        no_history=True)

    if do is not False:
        oc.add(do)

    return oc


####################################
# These are our /stat prefixes
@route(STAT_PREFIX + '/tag')
def All():
    mc = build_tag_container("all")
    return mc


@route(STAT_PREFIX + '/tag/actor')
def Actor():
    mc = build_tag_container("actor")
    return mc


@route(STAT_PREFIX + '/tag/director')
def Director():
    mc = build_tag_container("director")
    return mc


@route(STAT_PREFIX + '/tag/writer')
def Writer():
    mc = build_tag_container("writer")
    return mc


@route(STAT_PREFIX + '/tag/genre')
def Genre():
    mc = build_tag_container("genre")
    return mc


@route(STAT_PREFIX + '/tag/country')
def Country():
    mc = build_tag_container("country")
    return mc


@route(STAT_PREFIX + '/tag/mood')
def Mood():
    mc = build_tag_container("mood")
    return mc


@route(STAT_PREFIX + '/tag/autotag')
def Autotag():
    mc = build_tag_container("autotag")
    return mc


@route(STAT_PREFIX + '/tag/collection')
def Collection():
    mc = build_tag_container("collection")
    return mc


@route(STAT_PREFIX + '/tag/similar')
def Similar():
    mc = build_tag_container("similar")
    return mc


@route(STAT_PREFIX + '/tag/year')
def Year():
    mc = build_tag_container("year")
    return mc


@route(STAT_PREFIX + '/tag/contentRating')
def ContentRating():
    mc = build_tag_container("contentRating")
    return mc


@route(STAT_PREFIX + '/tag/studio')
def Studio():
    mc = build_tag_container("studio")
    return mc


# Rating (Reviews)
@route(STAT_PREFIX + '/tag/score')
def Score():
    mc = build_tag_container("score")
    return mc


@route(STAT_PREFIX + '/library')
def Library():
    mc = MediaContainer()
    mi = []
    Log.Debug("Here's where we fetch some library stats.")
    sections = {}
    recs = query_library_stats()
    sizes = query_library_sizes()
    records = recs[0]
    sec_counts = recs[1]
    for record in records:
        section = record["sectionTitle"]
        if section not in sections:
            sections[section] = []
        del (record["sectionTitle"])
        sections[section].append(dict(record))

    for name in sections:
        Log.Debug("Looping through section '%s'" % name)
        sec_id = sections[name][0]["section"]
        sec_type = sections[name][0]["sectionType"]
        section_types = {
            1: "movie",
            2: "show",
            3: "music",
            4: "photo",
            8: "music",
            13: "photo"
        }
        if sec_type in section_types:
            sec_type = section_types[sec_type]

        item_count = 0
        play_count = 0
        playable_count = 0
        section_children = []
        for record in sections[name]:
            item_count += record["totalItems"]
            if record['playCount'] is not None:
                play_count += record['playCount']
            if record["type"] in ["episode", "track", "movie"]:
                playable_count = record["totalItems"]

            item_type = str(record["type"]).capitalize()
            record_data = {
                "totalItems": record["totalItems"]
            }
            vc = AnyContainer(record_data, item_type, False)
            jc = {item_type: record_data}

            if record["lastViewedAt"] is not None:
                last_item = {
                    "title": record['title'],
                    "grandparentTitle": record['grandparentTitle'],
                    "art": record['art'],
                    "thumb": record['thumb'],
                    "ratingKey": record['ratingKey'],
                    "lastViewedAt": record['lastViewedAt'],
                    "username": record['username'],
                    "userId": record['userId']
                }
                li = AnyContainer(last_item, "lastViewed", False)
                vc.add(li)
                if os.environ['ENC_TYPE'] == 'json':
                    jc[item_type]['lastItem'] = last_item

            if os.environ['ENC_TYPE'] == 'json':
                section_children.append(jc)
            else:
                section_children.append(vc)

        section_data = {
            "title": name,
            "id": sec_id,
            "totalItems": item_count,
            "playableItems": playable_count,
            "playCount": play_count,
            "type": sec_type
        }

        for sec_size in sizes:
            if sec_size['section_id'] == sec_id:
                Log.Debug("Found a matching section size...foo")
                section_data['mediaSize'] = sec_size['size']

        sec_unique_played = sec_counts.get(str(sec_id)) or None
        if sec_unique_played is not None:
            Log.Debug("Hey, we got the unique count")
            section_data["watchedItems"] = sec_unique_played["viewedItems"]
        ac = AnyContainer(section_data, "Section", "False")
        bc = section_data
        for child in section_children:
            if os.environ['ENC_TYPE'] != 'json':
                ac.add(child)

        if os.environ['ENC_TYPE'] == 'json':
            bc["Sections"] = section_children
            mi.append(bc)
        else:
            mc.add(ac)

    if os.environ['ENC_TYPE'] == 'json':
        return JsonContainer(mi)
    else:
        return mc


@route(STAT_PREFIX + '/library/growth')
def Growth():
    headers = sort_headers(["Interval", "Start", "End", "Container-Size", "Container-Start", "Type"])
    records = query_library_growth(headers)
    total_array = {}
    for record in records:
        dates = str(record["addedAt"])[:-9].split("-")

        year = str(dates[0])
        month = str(dates[1])
        day = str(dates[2])

        year_array = total_array.get(year) or {}
        month_array = year_array.get(month) or {}
        day_array = month_array.get(day) or []
        day_array.append(record)

        month_array[day] = day_array
        year_array[month] = month_array
        total_array[year] = year_array

    mc = MediaContainer()
    grand_total = 0
    types_all = {}
    for y in range(0000, 3000):
        y = str(y)
        year_total = 0
        if y in total_array:
            types_year = {}
            Log.Debug("Found a year %s" % y)
            year_container = FlexContainer("Year", {"value": y})
            year_array = total_array[y]
            Log.Debug("Year Array: %s" % JSON.StringFromObject(year_array))
            month_total = 0
            for m in range(1, 12):
                m = str(m).zfill(2)
                if m in year_array:
                    types_month = {}
                    Log.Debug("Found a month %s" % m)
                    month_container = FlexContainer("Month", {"value": m})
                    month_array = year_array[m]
                    for d in range(1, 32):
                        d = str(d).zfill(2)
                        if d in month_array:
                            types_day = {}
                            Log.Debug("Found a day %s" % d)
                            day_container = FlexContainer("Day", {"value": d}, False)
                            records = month_array[d]
                            for record in records:
                                ac = FlexContainer("Added", record, False)
                                record_type = record["type"]
                                temp_day_count = types_day.get(record_type) or 0
                                temp_month_count = types_month.get(record_type) or 0
                                temp_year_count = types_year.get(record_type) or 0
                                temp_all_count = types_all.get(record_type) or 0
                                types_day[record_type] = temp_day_count + 1
                                types_month[record_type] = temp_month_count + 1
                                types_year[record_type] = temp_year_count + 1
                                types_all[record_type] = temp_all_count + 1
                                day_container.add(ac)
                            month_total += day_container.size()
                            day_container.set("totalAdded", day_container.size())
                            for rec_type in types_day:
                                day_container.set("%sCount" % rec_type, types_day.get(rec_type))
                            month_container.add(day_container)
                    year_total += month_total
                    month_container.set("totalAdded", month_total)
                    for rec_type in types_month:
                        month_container.set("%sCount" % rec_type, types_month.get(rec_type))
                    year_container.add(month_container)
            year_container.set("totalAdded", year_total)
            for rec_type in types_year:
                year_container.set("%sCount" % rec_type, types_year.get(rec_type))
            grand_total += year_total
            mc.add(year_container)
    return mc


@route(STAT_PREFIX + '/library/popular')
def Popular():
    headers = sort_headers(["foo"])
    results = query_library_popular()
    if os.environ['ENC_TYPE'] == 'json':
        Log.Debug("RETURNING JSON")
        mc = JsonContainer(results)

    else:
        mc = MediaContainer()

        for section in results:
            sc = FlexContainer(section)
            for record in results[section]:
                me = FlexContainer("Media", record, False)
                sc.add(me)
            mc.add(sc)

    return mc


@route(STAT_PREFIX + '/user')
def User():
    headers = sort_headers(["Type", "Userid", "Username", "Container-start", "Container-Size", "Device", "Title"])
    container_start = int(headers.get("Container-Start") or DEFAULT_CONTAINER_START)
    container_size = int(headers.get("Container-Size") or DEFAULT_CONTAINER_SIZE)
    container_max = container_start + container_size
    users_data = query_user_stats(headers)

    if os.environ['ENC_TYPE'] == 'json':
        Log.Debug("Returning JSON data")
        return JsonContainer(users_data)

    else:
        Log.Debug("Returning XML")
        mc = MediaContainer()
        if users_data is not None:
            users = users_data[0]
            devices = users_data[1]
            device_names = []
            for user, records in users.items():
                last_active = datetime.datetime.strptime("1900-01-01 00:00:00", DATE_STRUCTURE)
                uc = FlexContainer("User", {"userName": user}, False)
                sc = FlexContainer("Views")
                i = 0
                for record in records:
                    viewed_at = datetime.datetime.fromtimestamp(record["lastViewedAt"])
                    if viewed_at > last_active:
                        last_active = viewed_at
                    if i >= container_max:
                        break
                    if i >= container_start:
                        vc = FlexContainer("View", record, False)
                        if "deviceName" in record:
                            if record["deviceName"] not in device_names:
                                device_names.append(record["deviceName"])
                        sc.add(vc)
                uc.add(sc)
                uc.set("lastSeen", last_active)
                dp = FlexContainer("Devices", None, False)
                chrome_data = None
                if user in devices:
                    for device in devices[user]:
                        if device["deviceName"] in device_names:
                            if device["deviceName"] != "Chrome":
                                Log.Debug("Found a device for %s" % user)
                                dc = FlexContainer("Device", device)
                                dp.add(dc)
                            else:
                                chrome_bytes = 0
                                if chrome_data is None:
                                    chrome_data = device
                                else:
                                    chrome_bytes = device["totalBytes"] + chrome_data.get("totalBytes") or 0
                                chrome_data["totalBytes"] = chrome_bytes
                if chrome_data is not None:
                    dc = FlexContainer("Device", chrome_data)
                    dp.add(dc)
                uc.add(dp)
                mc.add(uc)

        Log.Debug("Still alive, returning data")

        return mc


####################################
# Finally, utility prefixes (logs, restart)
@route(APP_PREFIX + '/logs')
@route(CAST_PREFIX + '/logs')
@route(STAT_PREFIX + '/logs')
def DownloadLogs():
    buff = StringIO.StringIO()
    zip_archive = ZipFile(buff, mode='w', compression=ZIP_DEFLATED)
    paths = get_log_paths()
    if (paths[0] is not False) & (paths[1] is not False):
        logs = sorted(glob.glob(paths[0] + '*')) + [paths[1]]
        for path in logs:
            Log.Debug("Trying to read path: " + path)
            data = StringIO.StringIO()
            data.write(FileIO.read(path))
            zip_archive.writestr(os.path.basename(path), data.getvalue())

        zip_archive.close()

        return ZipObject(buff.getvalue())

    Log.Debug("No log path found, foo.")
    return ObjectContainer(
        no_cache=True,
        title1="No logs found",
        no_history=True,
        view_group="Details")


@route(APP_PREFIX + '/statmenu')
def Statmenu():
    Log.Debug("Building stats menu.")
    oc = ObjectContainer(
        no_cache=True,
        no_history=True,
        view_group="Details")

    do = DirectoryObject(
        title="Library",
        thumb=R(ICON_CAST_AUDIO),
        key=Callback(Library))

    oc.add(do)
    return oc


@route(CAST_PREFIX + '/status')
@route(CAST_PREFIX + '/resources/status')
def Status(input_name=False):
    """
    Fetch player status
    TODO: Figure out how to parse and return additional data here
    """
    uri = "FOOBAR"
    name = "FOOBAR"
    show_all = False
    Log.Debug('Trying to get cast device status here')
    for key, value in Request.Headers.items():
        Log.Debug("Header key %s is %s", key, value)
        if key in ("X-Plex-Clienturi", "Clienturi"):
            Log.Debug("We have a client URI")
            uri = value

        if key in ("X-Plex-Clientname", "Clientname"):
            Log.Debug("X-Plex-Clientname: " + value)
            name = value

    if input_name is not False:
        name = input_name
    if uri == name:
        show_all = True

    chromecasts = fetch_devices()
    devices = []

    for chromecast in chromecasts:
        cast = False
        if show_all is not True:
            if chromecast['name'] == name:
                Log.Debug("Found a matching chromecast: " + name)
                cast = chromecast

            if chromecast['uri'] == uri:
                Log.Debug("Found a matching uri:" + uri)
                cast = chromecast
        else:
            cast = chromecast

        if cast is not False:
            devices.append(cast)

    do = ""

    if len(devices) != 0:
        for device in devices:
            Log.Debug("We have set a chromecast here.")
            uris = device['uri'].split(":")
            host = uris[0]
            port = uris[1]
            Log.Debug("Host and port are %s and %s", host, port)
            cast = pychromecast.Chromecast(host, int(port))
            Log.Debug("Waiting for device")
            cast.wait(2)
            app_id = cast.app_id
            meta_dict = False
            if app_id == "9AC194DC":
                pc = PlexController(cast)
                cast.register_handler(pc)
                plex_status = pc.plex_status()
                raw_status = {
                    'state': plex_status['state'],
                    'volume': plex_status['volume'],
                    'muted': plex_status['muted']
                }
                meta_dict = plex_status['meta']
            else:
                raw_status = {"state": "idle"}

            Log.Debug("Did we get it?!?! %s", raw_status)

            do = StatusContainer(
                dict=raw_status
            )
            if meta_dict is not False:
                mc = MetaContainer(
                    dict=meta_dict
                )

                do.add(mc)

    return do


@route(APP_PREFIX + '/advanced')
def AdvancedMenu(header=None, message=None):
    oc = ObjectContainer(header=header or "Internal stuff, pay attention!", message=message, no_cache=True,
                         no_history=True,
                         replace_parent=False, title2="Advanced")

    oc.add(DirectoryObject(
        key=Callback(TriggerRestart),
        title="Restart the plugin",
    ))

    return oc


@route(APP_PREFIX + '/advanced/restart/trigger')
def TriggerRestart():
    DispatchRestart()
    oc = ObjectContainer(
        title1="restarting",
        no_cache=True,
        no_history=True,
        title_bar="Chromecast",
        view_group="Details")

    do = DirectoryObject(
        title="Rescan Devices",
        thumb=R(ICON_CAST_REFRESH),
        key=Callback(Rescan))

    oc.add(do)

    do = DirectoryObject(
        title="Devices",
        thumb=R(ICON_CAST),
        key=Callback(Resources))

    oc.add(do)

    do = DirectoryObject(
        title="Broadcast",
        thumb=R(ICON_CAST_AUDIO),
        key=Callback(Broadcast))

    oc.add(do)

    return oc


@route(APP_PREFIX + '/advanced/restart/execute')
def Restart():
    Plex[":/plugins"].restart(PLUGIN_IDENTIFIER)


####################################
# These functions are for cast-related stuff
def fetch_devices():
    if not Data.Exists('device_json'):
        Log.Debug("No cached data exists, re-scanning.")
        casts = scan_devices()

    else:
        Log.Debug("Returning cached data")
        casts_string = Data.Load('device_json')
        casts = JSON.ObjectFromString(casts_string)

    token = False
    for key, value in Request.Headers.items():
        Log.Debug("Header key %s is %s", key, value)
        if key in ("X-Plex-Token", "Token"):
            Log.Debug("We have a Token")
            token = value

    if token:
        port = os.environ.get("PLEXSERVERPORT")
        if port is None:
            port = "32400"
        url = Network.Address
        if url is None:
            url = "localhost"

        try:
            myurl = "http://" + url + ":" + port + "/clients?X-Plex-Token=" + token
        except TypeError:
            myurl = False
            pass

        if myurl:
            Log.Debug("Gonna connect to %s" % myurl)
            req = HTTP.Request(myurl)
            req.load()
            if hasattr(req, 'content'):
                client_data = req.content
                root = ET.fromstring(client_data)
                for device in root.iter('Server'):
                    local_item = {
                        "name": device.get('name'),
                        "uri": device.get('host') + ":" + str(device.get('port')),
                        "status": "n/a",
                        "type": device.get('product'),
                        "app": "Plex Client",
                        "id": device.get('machineIdentifier')
                    }
                    casts.append(local_item)

    return casts


def fetch_servers():
    token = False
    for key, value in Request.Headers.items():
        Log.Debug("Header key %s is %s", key, value)
        if key in ("X-Plex-Token", "Token"):
            Log.Debug("We have a Token")
            token = value

    servers = []

    if token:
        port = os.environ.get("PLEXSERVERPORT")
        url = Network.Address
        myurl = 'http://' + url + ':' + port + '/servers?X-Plex-Token=' + token
        Log.Debug("Gonna connect to %s" % myurl)
        req = HTTP.Request(myurl)
        req.load()
        client_data = req.content
        root = ET.fromstring(client_data)
        for device in root.iter('Server'):
            version = device.get("version").split("-")[0]
            local_item = {
                "name": device.get('name'),
                "uri": "http://" + device.get('host') + ":" + str(device.get('port')),
                "version": version,
                "id": device.get('machineIdentifier')
            }
            Log.Debug("Got me a server: %s" % local_item)
            servers.append(local_item)

    return servers


def scan_devices():
    Log.Debug("Re-fetching devices")
    casts = pychromecast.get_chromecasts(1, None, None, True)
    data_array = []
    for cast in casts:
        cast_item = {
            "uri": cast.uri,
            "name": cast.name,
            "status": cast.is_idle,
            "type": cast.cast_type,
            "app": cast.app_display_name,
            'id': cast.uri
        }
        data_array.append(cast_item)

    Log.Debug("Cast length is %s", str(len(data_array)))
    # if len(data_array) == 0:
    #     if Data.Exists('restarts') is not True:
    #         Data.Save('restarts', 1)
    #         Log.Debug("No cast devices found, we need to restart the plugin.")
    #         DispatchRestart()
    #     else:
    #         restart_count = Data.Load('restarts')
    #         if restart_count >= 5:
    #             Log.Debug("It's been an hour, trying to restart the plugin again")
    #             Data.Remove('restarts')
    #             DispatchRestart()
    #         else:
    #             Log.Debug("Avoiding a restart in case it's not me, but you.")
    #             restart_count += 1
    #             Data.Save('restarts', restart_count)
    #
    # else:
    #     Log.Debug("Okay, we have cast devices, no need to get all postal up in this mutha...")
    #     if Data.Exists('restarts'):
    #         Data.Remove('restarts')

    Log.Debug("Item count is " + str(len(data_array)))
    cast_string = JSON.StringFromObject(data_array)
    Data.Save('device_json', cast_string)
    last_scan = "Last Scan: " + time.strftime("%B %d %Y - %H:%M")
    Data.Save('last_scan', last_scan)
    return data_array


def player_string(values):
    request_id = values['Requestid']
    content_id = values['Contentid'] + '?own=1&window=200'  # key
    content_type = values['Contenttype']
    offset = values['Offset']
    server_id = values['Serverid']
    transcoder_video = values['Transcodervideo']
    # TODO: Make this sexy, see if we can just use the current server. I think so.
    server_uri = values['Serveruri'].split("://")
    server_parts = server_uri[1].split(":")
    server_protocol = server_uri[0]
    server_ip = server_parts[0]
    server_port = server_parts[1]
    # TODO: Look this up instead of send it?
    username = values['Username']
    true = "true"
    false = "false"
    request_array = {
        "type": 'LOAD',
        'requestId': request_id,
        'media': {
            'contentId': content_id,
            'streamType': 'BUFFERED',
            'contentType': content_type,
            'customData': {
                'offset': offset,
                'directPlay': true,
                'directStream': true,
                'subtitleSize': 100,
                'audioBoost': 100,
                'server': {
                    'machineIdentifier': server_id,
                    'transcoderVideo': transcoder_video,
                    'transcoderVideoRemuxOnly': false,
                    'transcoderAudio': true,
                    'version': '1.4.3.3433',
                    'myPlexSubscription': true,
                    'isVerifiedHostname': true,
                    'protocol': server_protocol,
                    'address': server_ip,
                    'port': server_port,
                    'user': {
                        'username': username
                    }
                },
                'containerKey': content_id
            },
            'autoplay': true,
            'currentTime': 0
        }
    }
    Log.Debug("Player String: " + JSON.StringFromObject(request_array))

    return request_array


####################################
# These functions are for stats stuff
def build_tag_container(selection):
    headers = sort_headers(["Container-Start", "Container-Size", "Type", "Section", "Include-Meta", "Meta-Size"])
    tag_options = ["actor", "director", "writer", "genre", "country", "mood", "similar", "autotag", "collection"]
    meta_options = ["year", "contentRating", "studio", "score"]
    records = []
    if selection in tag_options:
        records = query_tag_stats(selection, headers)
    if selection in meta_options:
        records = query_meta_stats(selection, headers)
    if selection == "all":
        records = query_tag_stats(selection, headers)
        records2 = query_meta_stats(selection, headers)
        records += records2

    Log.Debug("We have a total of %s records to process" % len(records))
    if os.environ['ENC_TYPE'] == 'json':
        return JsonContainer(records)
    else:
        out = MediaContainer()
        add_meta = False
        if "Include-Meta" in headers:
            add_meta = headers["Include-Meta"]
        else:
            if "Meta-Size" in headers:
                add_meta = True

        for tag_type in records:
            ttc = FlexContainer(tag_type["name"])
            tags = tag_type["children"]
            for tag in tags:
                tc = FlexContainer(tag["type"], None, False)
                tc.set("title", tag["name"])
                tc.set("totalItems", tag["count"])
                metas = tag["children"]
                for meta in metas:
                    me = FlexContainer(meta["type"])
                    me.set("type", meta["name"])
                    medias = meta["children"]
                    medias = sorted(medias, key=lambda i: i['added'], reverse=True)
                    itemCount = len(medias)
                    if "Meta-Size" in headers:
                        if len(medias) > headers["Meta-Size"]:
                            medias = medias[:headers["Meta-Size"]]
                    for media in medias:
                        mc = FlexContainer("Media", media)
                        me.add(mc)
                    if add_meta:
                        tc.add(me)
                    tc.set(meta["name"] + "Count", itemCount)
                ttc.add(tc)
            out.add(ttc)
        return out


def query_library_sizes():
    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    results = []

    if cursor is not None:
        query = """select sum(size), library_section_id, ls.name from media_items 
                    inner join library_sections as ls
                    on ls.id = library_section_id
                    group by library_section_id;"""

        for size, section_id, section_name in cursor.execute(query):
            dictz = {
                "size": size,
                "section_id": section_id,
                "section_name": section_name
            }
            results.append(dictz)

        close_connection(connection)

    return results


def query_tag_stats(selection, headers):

    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]

    tag_names = {
        "genre": 1,
        "collection": 2,
        "director": 4,
        "writer": 5,
        "actor": 6,
        "country": 8,
        "autotag": 207,
        "mood": 300,
        "similar": 305
    }

    tag_ids = {
        1: "genre",
        2: "collection",
        4: "director",
        5: "writer",
        6: "actor",
        8: "country",
        207: "autotag",
        300: "mood",
        305: "similar"
    }

    if selection == "all":
        stringz = []
        for tag_name in tag_names:
            stringz.append("tags.tag_type = %s" % tag_names[tag_name])
        selector = "WHERE (%s)" % " OR ".join(stringz)
    else:
        if selection not in tag_names:
            return []

        tag_type = tag_names[selection]
        selector = "WHERE tags.tag_type = %s" % tag_type

    section = headers.get("Section") or False
    if section:
        selector += " AND library_section = %s" % section

    entitlements = get_entitlements()
    selector += " AND lib_id IN %s" % entitlements
    meta_type = headers.get("Type") or False

    if meta_type:
        meta_id = False
        if meta_type in META_TYPE_NAMES:
            meta_id = META_TYPE_NAMES[meta_type]

        if meta_id:
            selector += " AND mt.metadata_type = %s" % meta_id

    if cursor is not None:
        query = """SELECT tags.tag, tags.tag_type, mt.id, mt.title, lib.name as library_section, mt1.title as parent_title,
                    mt2.title as grandparent_title, mt.metadata_type, mt.added_at, mt.year, lib.id as lib_id FROM taggings
                    LEFT JOIN tags ON tags.id = taggings.tag_id
                    INNER JOIN metadata_items AS mt
                    ON taggings.metadata_item_id = mt.id
                    LEFT JOIN metadata_items AS mt1
                    on mt1.id = mt.parent_id
                    LEFT JOIN metadata_items AS mt2
                    on mt2.id = mt1.parent_id
                    INNER JOIN library_sections as lib 
                    on mt.library_section_id = lib.id
                    %s
                    ORDER BY tags.tag_type, mt.metadata_type, library_section, tags.tag;
                    """ % selector

        records = {}
        Log.Debug("Query is '%s'" % query)

        for tag, tag_type, ratingkey, title, library_section, parent_title, \
                grandparent_title, meta_type, added_at, year, lib_id in cursor.execute(query):

            if tag_type in tag_ids:
                tag_title = tag_ids[tag_type]

            if meta_type in META_TYPE_IDS:
                meta_type = META_TYPE_IDS[meta_type]

            dicts = {
                "title": title,
                "ratingKey": ratingkey,
                "added": added_at,
                "thumb": "/library/metadata/" + str(ratingkey) + "/thumb",
                "art": "/library/metadata/" + str(ratingkey) + "/art",
                "year": year,
                "section": library_section,
                "sectionId": lib_id
            }

            if parent_title != "":
                dicts["parentTitle"] = parent_title

            if grandparent_title != "":
                dicts["grandparentTitle"] = grandparent_title

            if meta_type == "episode":
                dicts["banner"] = "/library/metadata/" + str(ratingkey) + "/banner/"

            tag_types = {}
            if tag_title in records:
                tag_types = records[tag_title]

            tags = {}
            if tag in tag_types:
                tags = tag_types[tag]

            meta_types = []
            if meta_type in tags:
                meta_types = tags[meta_type]

            meta_types.append(dicts)
            tags[meta_type] = meta_types
            tag_types[tag] = tags
            records[tag_title] = tag_types

        close_connection(connection)

        results = []

        container_size = int(headers.get("Container-Size") or 25)
        container_start = int(headers.get("Container-Start") or DEFAULT_CONTAINER_START)
        container_max = container_size + container_start
        Log.Debug("Container size is set to %s, start to %s" % (container_size, container_start))

        tag_type_count = 0
        for tag_type in records:
            tags = records[tag_type]
            tag_list = []
            tag_count = 0
            for tag in tags:
                meta_types = tags[tag]
                meta_type_list = []
                meta_count = 0
                for meta_type in meta_types:
                    meta_items = meta_types[meta_type]
                    meta_record = {
                        "name": meta_type,
                        "type": "meta",
                        "count": len(meta_items),
                        "children": meta_items
                    }
                    meta_count += len(meta_items)
                    meta_type_list.append(meta_record)
                meta_type_list = sorted(meta_type_list, key=lambda i: i['count'], reverse=True)
                tag_record = {
                    "name": tag,
                    "type": "tag",
                    "count": meta_count,
                    "children": meta_type_list
                }
                tag_count += meta_count
                tag_list.append(tag_record)
            tag_list = sorted(tag_list, key=lambda i: i['count'], reverse=True)
            if len(tag_list) >= container_max:
                tag_list = tag_list[container_start:container_size]
            else:
                tag_list = tag_list[container_start:]
            tag_type_record = {
                "name": tag_type,
                "type": "type",
                "count": len(tag_list),
                "children": tag_list
            }
            tag_type_count += tag_count
            results.append(tag_type_record)

        return results
    else:
        Log.Error("DB Connection error!")
        return None


def query_meta_stats(selection, headers):
    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    Log.Debug("Queryings meta stats for %s" % selection)
    entitlements = get_entitlements()
    selector = "WHERE lib.id IN %s" % entitlements

    sort_string = "ORDER BY mi.id"
    if selection != "all":
        selector = "WHERE mi.%s is not NULL AND mi.%s != ''" % (selection, selection)
        sort_string = "ORDER BY mi.%s" % selection

    section = headers.get("Section") or False
    if section:
        selector += " AND library_section = %s" % section

    meta_type = headers.get("Type") or False

    if meta_type:
        meta_id = False
        if meta_type in META_TYPE_NAMES:
            meta_id = META_TYPE_NAMES[meta_type]

        if meta_id:
            selector += " AND mt.metadata_type = %s" % meta_id

    if cursor is not None:
        query = """
            SELECT mi.title, mi.id, mi.year, mt1.title as parent_title, mt2.title as grandparent_title, 
            mi.content_rating, mi.studio, mi.tags_country, mi.rating, mi.added_at,
            lib.name as library_section, mi.library_section_id, mi.metadata_type from metadata_items as mi
            LEFT JOIN metadata_items AS mt1
                on mt1.id = mi.parent_id
            LEFT JOIN metadata_items AS mt2
                on mt2.id = mt1.parent_id
            INNER JOIN library_sections as lib
                on mi.library_section_id = lib.id
            %s
            %s
        """ % (selector, sort_string)

        Log.Debug("Query is '%s'" % query)
        records = {}
        record_types = ["year", "contentRating", "studio", "score"]
        for title, rating_key, year, parent_title, grandparent_title, contentRating, studio, country, score,\
                added_at, section, section_id, meta_type in cursor.execute(query):

            if meta_type in META_TYPE_IDS:
                meta_type = META_TYPE_IDS[meta_type]

            dicts = {
                "title": title,
                "ratingKey": rating_key,
                "year": year,
                "contentRating": contentRating,
                "studio": studio,
                "score": score,
                "sectionName": section,
                "sectionId": section_id,
                "added": added_at
            }

            if parent_title != "":
                dicts["parentTitle"] = parent_title

            if grandparent_title != "":
                dicts["grandparentTitle"] = grandparent_title

            if meta_type == "episode":
                dicts["banner"] = "/library/metadata/" + str(rating_key) + "/banner/"


            if (selection == "tags_country") | (selection == "all"):
                country_data = {}
                if 'country' in records:
                    country_data = records['country']
                countries = country.split("|")
                if ("USA" in countries) | ("United States" in countries):
                    countries = ["USA"]
                if "United Kingdom" in countries:
                    countries = ["United Kingdom"]
                for country_rec in countries:
                    dicts['country'] = country_rec
                    country_list = {}
                    if country_rec in country_data:
                        country_list = country_data[country_rec]
                    meta_list = []
                    if meta_type in country_list:
                        meta_list = country_list[meta_type]
                    meta_list.append(dicts)
                    country_list[meta_type] = meta_list
                    country_data[country_rec] = country_list
                records['country'] = country_data
                if selection == "all":
                    for record_type in record_types:
                        record_value = dicts[record_type]
                        if (record_value is not None) & (len(str(record_value))) > 0:
                            type_data = {}
                            if record_type in records:
                                type_data = records[record_type]
                            type_list = {}
                            if record_value in type_data:
                                type_list = type_data[record_value]
                            meta_list = []
                            if meta_type in type_list:
                                meta_list = type_list[meta_type]
                            meta_list.append(dicts)
                            type_list[meta_type] = meta_list
                            type_data[record_value] = type_list
                            records[record_type] = type_data
            else:
                record_value = dicts[selection]
                if len(str(record_value)) > 0:
                    type_data = {}
                    if selection in records:
                        type_data = records[selection]
                    type_list = {}
                    if record_value in type_data:
                        type_list = type_data[record_value]
                    meta_list = []
                    if meta_type in type_list:
                        meta_list = type_list[meta_type]
                    meta_list.append(dicts)
                    type_list[meta_type] = meta_list
                    type_data[record_value] = type_list
                    records[selection] = type_data

        close_connection(connection)

        results = []
        container_size = int(headers.get("Container-Size") or 25)
        container_start = int(headers.get("Container-Start") or DEFAULT_CONTAINER_START)
        container_max = container_size + container_start
        Log.Debug("Container size is set to %s, start to %s" % (container_size, container_start))

        tag_type_count = 0
        # country/rating/etc
        for tag_type in records:
            Log.Debug("Tag type is %s" % tag_type)
            tags = records[tag_type]
            tag_list = []
            tag_count = 0
            # 1922/TV-MA
            for tag in tags:

                Log.Debug("Tags.tag: %s %s" % (tag, JSON.StringFromObject(tags[tag])))
                meta_types = tags[tag]
                meta_type_list = []
                meta_count = 0
                for meta_type in meta_types:
                    meta_items = meta_types[meta_type]
                    meta_record = {
                        "name": str(meta_type),
                        "type": "meta",
                        "count": len(meta_items),
                        "children": meta_items
                    }
                    meta_count += len(meta_items)
                    meta_type_list.append(meta_record)
                meta_type_list = sorted(meta_type_list, key=lambda i: i['count'], reverse=True)
                tag_record = {
                    "name": str(tag),
                    "type": "tag",
                    "count": meta_count,
                    "children": meta_type_list
                }
                tag_count += meta_count
                tag_list.append(tag_record)
            tag_list = sorted(tag_list, key=lambda i: i['count'], reverse=True)
            if len(tag_list) >= container_max:
                tag_list = tag_list[container_start:container_size]
            else:
                tag_list = tag_list[container_start:]
            tag_type_record = {
                "name": str(tag_type),
                "type": "type",
                "count": len(tag_list),
                "children": tag_list
            }
            tag_type_count += tag_count
            results.append(tag_type_record)

        return results


def query_user_stats(headers):
    query_types = [1, 4, 10]
    if "Type" in headers:
        meta_type = headers.get("Type")
        if meta_type in META_TYPE_NAMES:
            meta_type = META_TYPE_NAMES[headers['Type']]
        if int(meta_type) == meta_type:
            query_types = [int(meta_type)]

    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]

    if cursor is not None:
        selectors = {}
        entitlements = get_entitlements()
        selectors["sm.metadata_type"] = ["IN", query_types]
        selectors["count"] = ["""!=""", 0]

        if len(headers.keys()) != 0:
            Log.Debug("We have headers...")
            selector_values = {
                "Userid": "sm.account_id",
                "Username": "accounts.name",
                "Device": "device_id"
            }

            for header_key, value in headers.items():
                if header_key in selector_values:
                    Log.Debug("Valid selector %s found" % header_key)
                    selector = selector_values[header_key]
                    selectors[selector] = ["""=""", value]

        query_selectors = []
        query_params = []
        for key, data in selectors.items():
            select_action = data[0]
            select_value = data[1]
            Log.Debug("Select Value is %s, action is %s" % (select_value, select_action))
            if isinstance(select_value, list):
                query_selector = "%s %s (%s)" % (key, select_action, ",".join('?' * len(select_value)))
                for sv in select_value:
                    query_params.append(sv)
            else:
                query_selector = "%s %s ?" % (key, select_action)
                query_params.append(select_value)

            query_selectors.append(query_selector)

        query_string = "WHERE " + " AND ".join(query_selectors)
        Log.Debug("Query string is '%s'" % query_string)

        # TODO: Add another method here to get the user's ID by Plex Token and only return their info?

        byte_query = """select accounts.name, sm.at, sm.metadata_type, sm.account_id,
                    devices.name AS device_name, devices.identifier AS device_id,
                    sb.bytes from statistics_media AS sm
                    INNER JOIN statistics_bandwidth as sb
                     ON sb.at = sm.at AND sb.account_id = sm.account_id AND sb.device_id = sm.device_id
                    INNER JOIN accounts
                     ON accounts.id = sm.account_id
                    INNER JOIN devices
                     ON devices.id = sm.device_id
                    %s
                    ORDER BY sm.at DESC;""" % query_string

        Log.Debug("Query1) is '%s'" % byte_query)
        Log.Debug("Query selectors: %s" % JSON.StringFromObject(query_params))
        results2 = {}

        for user_name, viewed_at, meta_type, user_id, device_name, device_id, data_bytes in cursor.execute(
                byte_query, query_params):

            user_array = {}
            if user_name in results2:
                user_array = results2[user_name]

            type_array = []
            if meta_type in type_array:
                type_array = type_array[meta_type]

            last_viewed = int(time.mktime(datetime.datetime.strptime(viewed_at, "%Y-%m-%d %H:%M:%S").timetuple()))
            meta_type = META_TYPE_IDS.get(meta_type) or meta_type
            dicts = {
                "userId": user_id,
                "userName": user_name,
                "lastViewedAt": last_viewed,
                "type": meta_type,
                "deviceName": device_name,
                "deviceId": device_id,
                "bytes": data_bytes
            }
            type_array.append(dicts)
            user_array[meta_type] = type_array
            results2[user_name] = user_array
        Log.Debug("Query1 completed.")

        query = """SELECT 
                        sm.account_id, sm.library_section_id, sm.grandparent_title, sm.parent_title, sm.title,
                        mi.id as rating_key, mi.tags_genre as genre, mi.tags_country as country, mi.year,
                        sm.viewed_at, sm.metadata_type, accounts.name, accounts.id as count
                    FROM metadata_item_views as sm
                    JOIN accounts
                    ON 
                    sm.account_id = accounts.id
                    LEFT JOIN metadata_items as mi
                    ON 
                        sm.title = mi.title 
                        AND mi.library_section_id = sm.library_section_id
                        AND mi.metadata_type = sm.metadata_type
                    %s
                    AND sm.library_section_id in %s                        
                    ORDER BY sm.viewed_at desc;""" % (query_string, entitlements)

        Log.Debug("Query2 is '%s'" % query)
        Log.Debug("Query selectors: %s" % JSON.StringFromObject(query_params))

        results = {}
        for user_id, library_section, grandparent_title, parent_title, title, \
            rating_key, genre, country, year, \
                viewed_at, meta_type, user_name, foo in cursor.execute(query, query_params):
            meta_type = META_TYPE_IDS.get(meta_type) or meta_type
            last_viewed = int(time.mktime(datetime.datetime.strptime(viewed_at, "%Y-%m-%d %H:%M:%S").timetuple()))
            user_array = []
            if user_name in results:
                user_array = results[user_name]

            dicts = {
                "userId": user_id,
                "userName": user_name,
                "title": title,
                "parentTitle": parent_title,
                "grandparentTitle": grandparent_title,
                "librarySection": library_section,
                "lastViewedAt": last_viewed,
                "type": meta_type,
                "ratingKey": rating_key,
                "thumb": "/library/metadata/" + str(rating_key) + "/thumb",
                "art": "/library/metadata/" + str(rating_key) + "/art",
                "year": year,
                "genre": genre,
                "country": country
            }

            if meta_type == "episode":
                dicts["banner"] = "/library/metadata/" + str(rating_key) + "/banner/"
            user_array.append(dicts)
            results[user_name] = user_array

        Log.Debug("Query2 completed")

        query3 = """SELECT sum(bytes), account_id, device_id,
                    accounts.name AS account_name,
                    devices.name AS device_name, devices.identifier AS machine_identifier
                    FROM statistics_bandwidth
                    INNER JOIN accounts
                    ON accounts.id = account_id
                    INNER JOIN devices
                    ON devices.id = device_id
                    GROUP BY account_id, device_id
                    """

        Log.Debug("Executing query 3 '%s'" % query3)

        device_results = {}
        for total_bytes, account_id, device_id, account_name, device_name, machine_identifier in cursor.execute(query3):
            user_array = []
            if account_name in device_results:
                user_array = device_results[account_name]

            device_dict = {
                "userId": account_id,
                "userName": account_name,
                "deviceId": device_id,
                "deviceName": device_name,
                "machineIdentifier": machine_identifier,
                "totalBytes": total_bytes
            }
            user_array.append(device_dict)
            device_results[account_name] = user_array
        close_connection(connection)
        Log.Debug("Connection closed.")
        output = {}
        for record_user, datas in results.items():
            user_array = []
            for data in datas:
                record_date = str(data["lastViewedAt"])[:6]
                record_type = data["type"]

                if record_user in results2:
                    if record_type in results2[record_user]:
                        for check in results2[record_user][record_type]:
                            check_date = str(check["lastViewedAt"])[:6]
                            if check_date == record_date:
                                for value in ["deviceName", "deviceId", "bytes"]:
                                    data[value] = check[value]

                user_array.append(data)
            output[record_user] = user_array
        return [output, device_results]
    else:
        Log.Error("DB Connection error!")
        return None


def query_library_stats():
    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    if cursor is not None:
        entitlements = get_entitlements()
        query = """SELECT
            FirstSet.library_section_id,
            FirstSet.metadata_type,    
            FirstSet.item_count,
            SecondSet.play_count,
            SecondSet.rating_key,
            SecondSet.title,
            SecondSet.grandparent_title,
            SecondSet.last_viewed,
            SecondSet.user_name,
            SecondSet.user_id,
            FirstSet.section_name,
            FirstSet.section_type
        FROM 
            (
                SELECT
                    mi.library_section_id,
                    mi.metadata_type,
                    ls.name AS section_name, ls.section_type,
                    count(mi.metadata_type) AS item_count
                FROM metadata_items AS mi
                INNER JOIN library_sections AS ls
                    ON mi.library_section_id = ls.id
                WHERE library_section_id IS NOT NULL
                GROUP BY library_section_id, metadata_type
            ) AS FirstSet
        LEFT JOIN
            (
                SELECT 
                    mi.id AS rating_key,
                    miv.title AS title,
                    miv.library_section_id,
                    miv.viewed_at AS last_viewed,
                    miv.metadata_type,
                    miv.grandparent_title AS grandparent_title,
                    count(miv.metadata_type) AS play_count,
                    accounts.name AS user_name, accounts.id AS user_id,
                    ls.name AS section_name, ls.section_type AS section_type,
                    max(viewed_at) AS last_viewed 
                FROM metadata_item_views AS miv
                INNER JOIN library_sections AS ls
                    ON miv.library_section_id = ls.id
                INNER JOIN metadata_items AS mi
                    ON mi.title = miv.title
                INNER JOIN accounts
                    ON miv.account_id = accounts.id
                AND
                    mi.metadata_type = miv.metadata_type             
                WHERE mi.library_section_id IS NOT NULL
                AND mi.library_section_id in %s
                GROUP BY miv.metadata_type
            ) AS SecondSet
        ON FirstSet.library_section_id = SecondSet.library_section_id AND FirstSet.metadata_type = SecondSet.metadata_type
        WHERE FirstSet.library_section_id in %s
        GROUP BY FirstSet.library_section_id, FirstSet.metadata_type
        ORDER BY FirstSet.library_section_id;""" % (entitlements, entitlements)

        Log.Debug("Querys is '%s'" % query)
        results = []
        for section, meta_type, item_count, play_count, ratingkey, title, \
            grandparent_title, last_viewed, user_name, user_id, sec_name, sec_type in cursor.execute(
            query):

            meta_type = META_TYPE_IDS.get(meta_type) or meta_type

            if last_viewed is not None:
                last_viewed = int(time.mktime(time.strptime(last_viewed, '%Y-%m-%d %H:%M:%S')))

            dicts = {
                "section": section,
                "totalItems": item_count,
                "playCount": play_count,
                "title": title,
                "grandparentTitle": grandparent_title,
                "lastViewedAt": last_viewed,
                "type": meta_type,
                "username": user_name,
                "userId": user_id,
                "sectionType": sec_type,
                "sectionTitle": sec_name,
                "ratingKey": ratingkey,
                "thumb": "/library/metadata/" + str(ratingkey) + "/thumb",
                "art": "/library/metadata/" + str(ratingkey) + "/art"
            }

            if meta_type == "episode":
                dicts["banner"] = "/library/metadata/" + str(ratingkey) + "/banner/"

            results.append(dicts)
        count_query = """
                        SELECT mi.total_items, miv.viewed_count, mi.metadata_type, mi.library_section_id
                        FROM (
                            SELECT count(metadata_type) AS total_items, metadata_type, library_section_id
                            FROM metadata_items
                            GROUP BY metadata_type, library_section_id
                        ) AS mi
                        INNER JOIN (
                            SELECT count(metadata_type) AS viewed_count, metadata_type, library_section_id FROM (
                                SELECT DISTINCT metadata_type, library_section_id, title, thumb_url
                                FROM metadata_item_views
                            ) AS umiv
                            GROUP BY metadata_type, library_section_id
                        ) AS miv
                        ON miv.library_section_id = mi.library_section_id AND miv.metadata_type = mi.metadata_type;
                        """
        sec_counts = {}
        for total_items, viewed_count, meta_type, section_id in cursor.execute(count_query):
            meta_type = META_TYPE_IDS.get(meta_type) or meta_type
            dicts = {
                "sectionId": section_id,
                "type": meta_type,
                "totalItems": total_items,
                "viewedItems": viewed_count
            }
            sec_counts[str(section_id)] = dicts
        close_connection(connection)
        return [results, sec_counts]
    else:
        Log.Error("Error connecting to DB!")


def query_library_growth(headers):
    container_size = int(headers.get("Container-Size") or DEFAULT_CONTAINER_SIZE)
    container_start = int(headers.get("Container-Start") or DEFAULT_CONTAINER_START)
    results = []
    interval = build_interval()
    start_date = interval[0]
    end_date = interval[1]

    Log.Debug("Okay, we should have start and end dates of %s and %s" % (start_date, end_date))
    entitlements = get_entitlements()
    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    if cursor is not None:
        Log.Debug("Ready to query!")
        query = """
            SELECT mi1.id, mi1.title, mi1.year, mi1.metadata_type, mi1.created_at, mi1.tags_genre AS genre, mi1.tags_country AS country, mi1.parent_id,
            mi2.title AS parent_title, mi2.parent_id AS grandparent_id, mi2.tags_genre AS parent_genre, mi2.tags_country AS parent_country,
            mi3.title AS grandparent_title, mi3.tags_genre AS grandparent_genre, mi3.tags_country AS grandparent_country, 
            mi1.library_section_id as section
            FROM metadata_items AS mi1
            LEFT JOIN metadata_items AS mi2
            ON mi1.parent_id = mi2.id
            LEFT JOIN metadata_items AS mi3
            ON mi2.parent_id = mi3.id
            WHERE mi1.created_at BETWEEN ? AND ?
            AND section in %s
            AND mi1.metadata_type IN (1, 4, 10)
            AND mi1.title NOT IN ("", "com.plexapp.agents")
            ORDER BY mi1.created_at DESC;
        """ % entitlements
        params = (start_date, end_date)
        Log.Debug("Query is '%s', params are '%s'" % (query, params))
        i = 0
        container_max = container_start + container_size
        for rating_key, title, year, meta_type, created_at, genres, country, \
            parent_id, parent_title, parent_genre, parent_country, \
                grandparent_id, grandparent_title, grandparent_genre, grandparent_country, section\
                    in cursor.execute(query, params):
            if i >= container_max:
                break

            if i >= container_start:
                meta_type = META_TYPE_IDS.get(meta_type) or meta_type
                dicts = {
                    "ratingKey": rating_key,
                    "title": title,
                    "parentTitle": parent_title,
                    "parentId": parent_id,
                    "parentGenre": parent_genre,
                    "parentCountry": parent_country,
                    "grandparentTitle": grandparent_title,
                    "grandparentId": grandparent_id,
                    "grandparentGenre": grandparent_genre,
                    "grandparentCountry": grandparent_country,
                    "year": year,
                    "thumb": "/library/metadata/" + str(rating_key) + "/thumb",
                    "art": "/library/metadata/" + str(rating_key) + "/art",
                    "type": meta_type,
                    "genres": genres,
                    "country": country,
                    "addedAt": created_at
                }
                results.append(dicts)
            i += 1
        close_connection(connection)
    return results


def query_library_popular():
    headers = sort_headers(["Container-Start", "Container-Size", "Type", "Section", "Start", "End", "Interval", "Sort"])

    container_size = int(headers.get("Container-Size") or 10000)
    container_start = int(headers.get("Container-Start") or DEFAULT_CONTAINER_START)

    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    Log.Debug("Querying most popular media")
    entitlements = get_entitlements()
    selector = "AND mi.library_section_id IN %s AND sm.title != ''" % entitlements
    sort = headers.get("Sort") or "Total"
    section = headers.get("Section") or False
    if section:
        selector += " AND mi.library_section_id = %s" % section

    meta_type = headers.get("Type") or False

    if meta_type:
        meta_id = False
        if meta_type in META_TYPE_NAMES:
            meta_id = META_TYPE_NAMES[meta_type]

        if meta_id:
            selector += " AND mi.metadata_type = %s" % meta_id

    interval = build_interval()
    start_date = interval[0]
    end_date = interval[1]

    results = {}
    if cursor is not None:
        query = """
            SELECT DISTINCT
                sm.library_section_id, sm.grandparent_title, sm.parent_title, sm.title, sm.viewed_at,
                mi.id as rating_key, sm.account_id, mi.metadata_type
            FROM metadata_item_views as sm
            LEFT JOIN metadata_items as mi
            ON 
                sm.title = mi.title 
                AND mi.library_section_id = sm.library_section_id
                AND mi.metadata_type = sm.metadata_type
            WHERE sm.viewed_at BETWEEN '%s' AND '%s'
            %s
            order by rating_key
            LIMIT %s
            OFFSET %s;
        """ % (start_date, end_date, selector, container_size, container_start)

        Log.Debug("Query is '%s'" % query)
        record_dict = {}
        for section_id, grandparent_title, parent_title, title, viewed_at, rating_key, account_id,\
                meta_type in cursor.execute(query):

            if meta_type in META_TYPE_IDS:
                meta_type = META_TYPE_IDS[meta_type]

            rec_count = 0
            user_list = []
            if str(rating_key) in record_dict:
                dicts = record_dict[str(rating_key)]
                rec_count = dicts['viewCount']
                user_list = dicts['userList']
            else:

                dicts = {
                    "title": title,
                    "parentTitle": parent_title,
                    "grandparentTitle": grandparent_title,
                    "type": meta_type,
                    "ratingKey": rating_key,
                    "metaType": meta_type,
                    "thumb": "/library/metadata/" + str(rating_key) + "/thumb",
                    "art": "/library/metadata/" + str(rating_key) + "/art"
                }

            if account_id not in user_list:
                user_list.append(account_id)

            rec_count += 1

            dicts["viewCount"] = rec_count
            dicts["userList"] = user_list
            dicts["userCount"] = len(user_list)

            if meta_type == "episode":
                dicts["banner"] = "/library/metadata/" + str(rating_key) + "/banner/"

            record_dict[str(rating_key)] = dicts

        close_connection(connection)

        meta_items = []
        for rating_key in record_dict:
            dicts = record_dict[rating_key]
            meta_items.append(dicts)
        if sort == "User":
            param = "userCount"
        else:
            param = "viewCount"
        meta_items = sorted(meta_items, key=lambda i: i[param], reverse=True)
        for item in meta_items:
            meta_type = item['metaType']
            meta_list = []
            if meta_type in results:
                meta_list = results[meta_type]
            del item['userList']
            del item['metaType']
            meta_list.append(item)
            results[meta_type] = meta_list

    return results


def fetch_cursor():
    cursor = None
    connection = None
    if os.environ["Loaded"]:
        import apsw
        Log.Debug("Shit, we got the librarys!")
        connection = apsw.Connection(os.environ['LIBRARY_DB'])
        cursor = connection.cursor()
    return [cursor, connection]


def close_connection(connection):
    if connection is not None:
        Log.Debug("Closing connection..")
        connection.close()
    else:
        Log.Debug("No connection to close!")


def vcr_ver():
    msvcr_map = {
        'msvcr120.dll': 'vc12',
        'msvcr130.dll': 'vc14'
    }
    try:
        import ctypes.util

        # Retrieve linked msvcr dll
        name = ctypes.util.find_msvcrt()

        # Return VC++ version from map
        if name not in msvcr_map:
            Log.Error('Unknown VC++ runtime: %r', name)
            return None

        return msvcr_map[name]
    except Exception as ex:
        Log.Error('Unable to retrieve VC++ runtime version: %s' % ex, exc_info=True)
        return None


def init_apsw():
    try:
        import apsw
    except ImportError:
        Log.Error("Shit, module not imported")
    pass


def insert_paths(distribution, libraries_path):
    # Retrieve system details
    system = SystemHelper.name()
    architecture = SystemHelper.architecture()

    if not architecture:
        Log.Debug('Unable to retrieve system architecture')
        return False

    Log.Debug('System: %r, Architecture: %r', system, architecture)

    # Build architecture list
    architectures = [architecture]

    if architecture == 'i686':
        # Fallback to i386
        architectures.append('i386')

    # Insert library paths
    found = False

    for arch in architectures + ['universal']:
        if insert_architecture_paths(libraries_path, system, arch):
            Log.Debug('Inserted libraries path for system: %r, arch: %r', system, arch)
            found = True

    # Display interface message if no libraries were found
    if not found:
        if distribution and distribution.get('name'):
            message = 'Unable to find compatible native libraries in the %s distribution' % distribution['name']
        else:
            message = 'Unable to find compatible native libraries'
        Log.Debug(message)

        # InterfaceMessages.add(60, '%s (system: %r, architecture: %r)', message, system, architecture)

    return found


def insert_architecture_paths(libraries_path, system, architecture):
    architecture_path = os.path.join(libraries_path, system, architecture)

    if not os.path.exists(architecture_path):
        Log.Debug("Arch path %s doesn't exist!!" % architecture_path)
        return False

    # Architecture libraries
    Log.Debug("inserting libs path")
    PathHelper.insert(libraries_path, system, architecture)

    # System libraries
    if system == 'Windows':
        # Windows libraries (VC++ specific)
        insert_paths_windows(libraries_path, system, architecture)
    else:
        # Darwin/FreeBSD/Linux libraries
        insert_paths_unix(libraries_path, system, architecture)

    return True


def insert_paths_unix(libraries_path, system, architecture):
    # UCS specific libraries
    ucs = UNICODE_MAP.get(sys.maxunicode)
    Log.Debug('UCS: %r', ucs)

    if ucs:
        Log.Debug("inserting UCS path")
        PathHelper.insert(libraries_path, system, architecture, ucs)

    # CPU specific libraries
    cpu_type = SystemHelper.cpu_type()
    page_size = SystemHelper.page_size()

    Log.Debug('CPU Type: %r', cpu_type)
    Log.Debug('Page Size: %r', page_size)

    if cpu_type:
        Log.Debug("Inserting CPU Type path")
        PathHelper.insert(libraries_path, system, architecture, cpu_type)

        if page_size:
            Log.Debug("Page Size path")
            PathHelper.insert(libraries_path, system, architecture, '%s_%s' % (cpu_type, page_size))

    # UCS + CPU specific libraries
    if cpu_type and ucs:
        Log.Debug("CPU + UCS path")
        PathHelper.insert(libraries_path, system, architecture, cpu_type, ucs)

        if page_size:
            Log.Debug("And page size")
            PathHelper.insert(libraries_path, system, architecture, '%s_%s' % (cpu_type, page_size), ucs)


def insert_paths_windows(libraries_path, system, architecture):
    vcr = SystemHelper.vcr_version() or 'vc12'  # Assume "vc12" if call fails
    ucs = UNICODE_MAP.get(sys.maxunicode)

    Log.Debug('VCR: %r, UCS: %r', vcr, ucs)

    # VC++ libraries
    Log.Debug("Inserting vcr path")
    PathHelper.insert(libraries_path, system, architecture, vcr)

    # UCS libraries
    if ucs:
        Log.Debug("Inserting UCS path")
        PathHelper.insert(libraries_path, system, architecture, vcr, ucs)


def get_entitlements():
    token = False
    allowed_keys = []

    for key, value in Request.Headers.items():
        Log.Debug("Header key %s is %s", key, value)
        if key in ("X-Plex-Token", "Token"):
            Log.Debug("We have a Token")
            token = value

    if token:
        server_port = os.environ.get("PLEXSERVERPORT")
        if server_port is None:
            server_port = "32400"
        server_host = Network.Address
        if server_host is None:
            server_host = "localhost"

        try:
            my_url = "http://%s:%s/library/sections?X-Plex-Token=%s" % (server_host, server_port, token)
        except TypeError:
            my_url = False
            pass

        if my_url:
            Log.Debug("Gonna touch myself at '%s'" % my_url)
            req = HTTP.Request(my_url)
            req.load()
            if hasattr(req, 'content'):
                client_data = req.content
                root = ET.fromstring(client_data)
                for section in root.iter('Directory'):
                    Log.Debug("Section?")
                    allowed_keys.append(section.get("key"))

    if len(allowed_keys) != 0:
        allowed_keys = "(" + ", ".join(allowed_keys) + ")"
        Log.Debug("Hey, we got the keys: %s" % allowed_keys)
    else:
        allowed_keys = "()"
        Log.Debug("No keys, try again.")

    return allowed_keys


####################################
# These functions are for utility stuff
def get_time_difference(time_start, time_end):
    time_diff = time_end - time_start
    return time_diff.total_seconds() / 60


def sort_headers(header_list, strict=False):
    returns = {}
    for key, value in Request.Headers.items():
        Log.Debug("Header key %s is %s", key, value)
        if key == "Accept":
            if value == "application/json":
                os.environ['ENC_TYPE'] = "json"
            else:
                os.environ['ENC_TYPE'] = "xml"

        for item in header_list:
            if key in ("X-Plex-" + item, item):
                Log.Debug("We have a " + item)
                value = unicode(value)
                is_int = False
                try:
                    test = int(value)
                    is_int = True
                except ValueError:
                    Log.Debug("Value is not a string")
                    pass
                else:
                    value = test

                returns[item] = value
                header_list.remove(item)

    if strict:
        len2 = len(header_list)
        if len2 == 0:
            Log.Debug("We have all of our values: " + JSON.StringFromObject(returns))
            return returns

        else:
            Log.Error("Sorry, parameters are missing.")
            for item in header_list:
                Log.Error("Missing " + item)
            return False
    else:
        return returns


def get_log_paths():
    # find log handler
    server_log_path = False
    plugin_log_path = False
    for handler in Core.log.handlers:
        if getattr(getattr(handler, "__class__"), "__name__") in (
                'FileHandler', 'RotatingFileHandler', 'TimedRotatingFileHandler'):
            plugin_log_file = handler.baseFilename
            if os.path.isfile(os.path.realpath(plugin_log_file)):
                plugin_log_path = plugin_log_file
                Log.Debug("Found a plugin path: " + plugin_log_path)

            if plugin_log_file:
                server_log_file = os.path.realpath(os.path.join(plugin_log_file, "../../Plex Media Server.log"))
                if os.path.isfile(server_log_file):
                    server_log_path = server_log_file
                    Log.Debug("Found a server log path: " + server_log_path)

    return [plugin_log_path, server_log_path]


def log_data(data):
    Log.Debug("Is there data?? " + JSON.StringFromObject(data))


def DispatchRestart():
    Thread.CreateTimer(1.0, Restart)


def build_interval():
    headers = sort_headers(["Start", "End", "Interval"])
    start_date = "1900-01-01 00:00:00"
    end_date = datetime.datetime.strftime(datetime.datetime.now(), DATE_STRUCTURE)
    if "Interval" in headers:
        interval = int(headers["Interval"])
        if "Start" in headers:
            start_check = headers.get("Start")
            valid = validate_date(start_check)
            if valid:
                Log.Debug("We have a vv start date, we'll use that.")
                end_date = datetime.datetime.strftime(datetime.datetime.strptime(
                    valid, DATE_STRUCTURE) - datetime.timedelta(days=interval), DATE_STRUCTURE)

        elif "End" in headers:
            end_check = headers.get("End")
            valid = validate_date(end_check)
            if valid:
                Log.Debug("We have an vv end date, we'll set interval from there.")
                start_date = datetime.datetime.strftime(datetime.datetime.strptime(
                    valid, DATE_STRUCTURE) + datetime.timedelta(days=interval), DATE_STRUCTURE)

        else:
            Log.Debug("No start or end params, going %s days from today." % interval)
            start_int = datetime.datetime.now()
            start_date = datetime.datetime.now().strftime(DATE_STRUCTURE)
            end_int = start_int - datetime.timedelta(days=interval)
            end_date = datetime.datetime.strftime(end_int, DATE_STRUCTURE)
            Log.Debug("start date is %s, end is %s" % (start_date, end_date))

    else:
        if "Start" in headers:
            start_check = headers.get("Start")
            valid = validate_date(start_check)
            if valid:
                Log.Debug("We have a start v2 date, we'll use that.")
                start_date = valid

        if "End" in headers:
            end_check = headers.get("End")
            valid = validate_date(end_check)
            if valid:
                Log.Debug("We have an end v2 dates, we'll set interval from there.")
                end_date = valid

    return [start_date, end_date]


def validate_date(date_text):
    valid = False

    full_date = str(date_text).split(" ")
    date_list = full_date[0].split("-")
    if len(date_list) == 3:
        Log.Debug("Date has YMD params, we're good")
    if len(date_list) == 2:
        Log.Debug("Date missing day param, adding")
        date_list.append("01")
    if len(date_list) == 1:
        Log.Debug("Date missing month and day, adding")
        date_list.append("01")
        date_list.append("01")
    date_param = "-".join(date_list)

    time_list = ["00", "00", "00"]
    if len(full_date) == 2:
        Log.Debug("Date appears to have a time param")
        time_list = full_date[1].split(":")
        if len(time_list) == 3:
            Log.Debug("Date has full time")
        if len(time_list) == 2:
            Log.Debug("Date is missing hours")
            time_list.append("00")
        if len(time_list) == 1:
            Log.Debug("Date has an hour param only")
            time_list.append("00")
            time_list.append("00")
    time_param = ":".join(time_list)

    date_check = "%s %s" % (date_param, time_param)
    Log.Debug("Date string built to %s" % date_check)

    try:
        datetime.datetime.strptime(date_check, DATE_STRUCTURE)
        valid = date_check
    except ValueError:
        pass

    if valid is False:
        Log.Error("Could not determine date structure for '%s" % date_text)

    return valid
