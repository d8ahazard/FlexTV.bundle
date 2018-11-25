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
import xmltodict
from monitor import Monitor

from zipfile import ZipFile, ZIP_DEFLATED

import pychromecast
from helpers import PathHelper
from helpers.system import SystemHelper
from helpers.variable import pms_path
from pychromecast.controllers.media import MediaController
from pychromecast.controllers.plex import PlexController
from subzero.lib.io import FileIO

import log_helper
from flex_container import FlexContainer
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


os_platform = False
path = None
# Dummy Imports for PyCharm

# import Framework.context
# from Framework.api.objectkit import ObjectContainer, DirectoryObject
# from Framework.docutils import Plugin, HTTP, Log, Request
# from Framework.docutils import Data

Dict['version'] = '1.1.106'
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
    test_path = sys.path[0].rstrip("\Shared")
    pms_path_name = pms_path()
    db_path = os.path.join(pms_path_name, "Plug-in Support", "Databases", "com.plexapp.plugins.library.db")
    Log.Debug("Setting DB path to '%s'" % db_path)
    os.environ['LIBRARY_DB'] = db_path
    os.environ["PMS_PATH"] = pms_path_name
    libraries_path = sys.path[0].rstrip("\Shared")
    loaded = insert_paths(distribution, libraries_path)
    if loaded:
        Log.Debug("Paths should be loaded!")
        os.environ["Loaded"] = "True"
    else:
        Log.Debug("Unable to load path")
        os.environ["Loaded"] = "False"
    ObjectContainer.title1 = NAME
    DirectoryObject.thumb = R(ICON)
    HTTP.CacheTime = 5
    if Data.Exists('device_json') is not True:
        UpdateCache()

    ValidatePrefs()
    CacheTimer()
    RestartTimer()


def CacheTimer(mins=10):
    Log.Debug("Cache timer started, updatings in %s minutes, man", mins)
    Thread.CreateTimer(mins, CacheTimer)
    UpdateCache()


def RestartTimer():
    hours = 4
    restart_time = hours * 60 * 60
    Log.Debug("Restart timer started, plugin will re-start in %s hours.", hours)
    Thread.CreateTimer(restart_time, DispatchRestart)


def UpdateCache():
    Log.Debug("UpdateCache calleds")
    if Data.Exists('last_cache'):
        last_scan = float(Data.Load('last_cache'))
        now = float(time.time())
        if now > last_scan:
            time_diff = now - last_scan
            time_mins = time_diff / 60
            if time_mins > 10:
                Log.Debug("Scanning devices, it's been %s minutes since our last scan." % time_mins)
                scan_devices()
            else:
                Log.Debug("Devices will be re-cached in %s minutes" % round(10 - time_mins))
        else:
            time_diff = last_scan - now
            time_mins = 10 - round(time_diff / 60)
            Log.Debug("Device scan set for %s minutes from now." % time_mins)

        Log.Debug("Diffs are %s and %s and %s." % (last_scan, now, time_diff))

    else:
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
    title = NAME + " - " + Dict['version']
    if Data.Exists('last_cache'):
        last_cache = Data.Load('last_cache')
        last_cache = float(last_cache)
        time_string = datetime.datetime.fromtimestamp(last_cache).strftime(DATE_STRUCTURE)
        title = "%s - %s - Last Scan: %s" % (NAME, Dict['version'], time_string)

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

    dependencies = ["helpers", "monitor"]
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
    Log.Debug('Fetchings /devices endpoint.')
    # Grab our response header?
    casts = fetch_devices()
    mc = FlexContainer()
    for cast in casts:
        Log.Debug("Cast type is " + cast['type'])
        if (cast['type'] == 'cast') | (cast['type'] == 'audio') | (cast['type'] == 'group'):
            dc = FlexContainer("Device", cast, show_size=False)
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

    mc = FlexContainer()
    for cast in casts:
        dc = FlexContainer("Device", cast, show_size=False)
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

    oc = FlexContainer(attributes={
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
    mc = FlexContainer()
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
            vc = FlexContainer(item_type, record_data, False)

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
                li = FlexContainer("lastViewed", last_item, False)
                vc.add(li)

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
            ac = FlexContainer("Section", section_data, False)
            bc = section_data
            for child in section_children:
                ac.add(child)
            mc.add(ac)

    return mc


@route(STAT_PREFIX + '/library/growth')
def Growth():
    headers = sort_headers(["Interval", "Start", "End", "Type"])
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

    mc = FlexContainer()
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
    results = query_library_popular()
    mc = FlexContainer()
    for section in results:
        sc = FlexContainer(section, limit=True)
        for record in results[section]:
            rec_users = {}
            if "users" in record:
                rec_users = record["users"]
                del record["users"]
                if "userName" in record:
                    del record["userName"]
                if "userId" in record:
                    del record["userId"]
            me = FlexContainer("Media", record, show_size=False)
            usc = FlexContainer("Users", show_size=False)
            view_total = 0
            for userName, userData in rec_users.items():
                vc = FlexContainer("Views")
                views = userData.get("views") or []
                views = sorted(views, key=lambda z: z['dateViewed'], reverse=True)
                if "views" in userData:
                    del userData["views"]
                uc = FlexContainer("User", userData, show_size=False)
                for view in views:
                    vsc = FlexContainer("View", view, show_size=False)
                    vc.add(vsc)
                uc.add(vc)
                uc.set("playCount", vc.size())
                view_total += vc.size()
                usc.add(uc)
            usc.set('userCount', usc.size())
            usc.set('playCount', view_total)
            me.add(usc)
            sc.add(me)
        mc.add(sc)

    return mc


@route(STAT_PREFIX + '/library/quality')
def Quality():
    results = query_library_quality()
    mc = FlexContainer()
    Log.Debug("Record: %s" % JSON.StringFromObject(results))
    for meta_type, records in results.items():
        me = FlexContainer("Meta")
        me.set("Type", meta_type)
        records = results[meta_type]
        for record in records:
            mi = FlexContainer("Media", record, limit=True)
            me.add(mi)

        mc.add(me)

    return mc


@route(STAT_PREFIX + '/system')
def System():
    Log.Debug("Querying system specs")
    headers = sort_headers(["Friendly"])
    friendly = headers.get("Friendly") or False
    mon = Monitor(friendly)
    mem_data = mon.get_memory()
    cpu_data = mon.get_cpu()
    hdd_data = mon.get_disk()
    net_data = mon.get_net()
    mc = FlexContainer("MediaContainer", show_size=False)
    mem_container = FlexContainer("Mem", mem_data, show_size=False)
    cpu_container = FlexContainer("Cpu", cpu_data, show_size=False)
    hdd_container = FlexContainer("Hdd", show_size=False)
    for disk_item in hdd_data:
        dc = FlexContainer("Disk", disk_item, show_size=False)
        hdd_container.add(dc)
    net_container = FlexContainer("Net", show_size=False)
    for nic in net_data:
        if_container = FlexContainer("Interface", nic, show_size=False)
        net_container.add(if_container)

    mc.add(mem_container)
    mc.add(cpu_container)
    mc.add(hdd_container)
    mc.add(net_container)
    return mc


@route(STAT_PREFIX + '/user')
def User():
    users = query_user_stats()

    Log.Debug("Returning XML")
    mc = FlexContainer()
    if users is not None:
        for user in users:
            user_meta = user['meta']
            user_devices = user['devices']
            del user['meta']
            del user['devices']
            uc = FlexContainer("User", user, False)
            sc = FlexContainer("Views")
            for meta, items in user_meta.items():
                vc = FlexContainer(meta, limit=True)
                for item in items:
                    ic = FlexContainer("Meta", item)
                    vc.add(ic)

                sc.add(vc)
            uc.add(sc)
            chrome_data = None
            dp = FlexContainer("Devices", None, False, limit=True)

            for device in user_devices:
                if device["deviceName"] != "Chrome":
                    dc = FlexContainer("Device", device, False)
                    dp.add(dc)
                else:
                    chrome_bytes = 0
                    if chrome_data is None:
                        chrome_data = device
                    else:
                        chrome_bytes = device["totalBytes"] + chrome_data.get("totalBytes") or 0
                    chrome_data["totalBytes"] = chrome_bytes

            if chrome_data is not None:
                dc = FlexContainer("Device", chrome_data, False)
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
@route("/stats/sessions")
def Status():
    """
    Fetch player status
    TODO: Figure out how to parse and return additional data here
    """
    show_all = True
    headers = sort_headers(["Clienturi", "Clientname"])
    uri = headers.get("Clienturi") or False
    name = headers.get("Clientname") or False
    if uri | name:
        show_all = False

    chromecasts = fetch_devices()
    devices = []
    cast_devices = []

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
            if cast['type'] in ['cast', 'audio', 'group']:
                cast_devices.append(cast)
            else:
                devices.append(cast)

    session_statuses = get_session_status()

    mc = FlexContainer()

    if len(cast_devices):
        for device in cast_devices:
            uris = device['uri'].split(":")
            host = uris[0]
            port = uris[1]
            cast = False
            try:
                cast = pychromecast.Chromecast(host, int(port), timeout=3, tries=1)
            except Exception:
                Log.Error("Unable to connecct to device.")

            if cast:
                Log.Debug("Waiting for devices.")
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
                    if 'title' in meta_dict:
                        delements = []
                        i = 0
                        for session in session_statuses:
                            if (meta_dict['title'] == session['Video']['title']) & (host == session['address']):
                                delements.append(i)
                                meta_dict = session['Video']
                                del session['Video']
                                for key, value in session.items():
                                    raw_status[key] = value
                            i += 1
                        delements.reverse()
                        for rem in delements:
                            del session_statuses[rem]
                else:
                    raw_status = {"state": "idle"}

                del device['status']
                do = FlexContainer("Device", attributes=device, show_size=False)
                for key, value in raw_status.items():
                    do.set(key, value)
                if meta_dict:
                    md = FlexContainer("Meta", meta_dict, show_size=False)
                    do.add(md)
                mc.add(do)

    if len(devices):
        for device in devices:
            del device['status']
            do = FlexContainer("Device", attributes=device, show_size=False)
            meta_dict = False
            delements = []
            i = 0
            for session in session_statuses:
                if session['machineIdentifier'] == device['id']:
                    delements.append(i)
                    Log.Debug("Session Match.")
                    meta_dict = session['Video']
                    del session['Video']
                    for key, value in session.items():
                        do.set(key, value)
                i += 1

            for delement in delements:
                del session_statuses[delement]

            if meta_dict:
                md = FlexContainer("Meta", meta_dict, show_size=False)
                do.add(md)
            else:
                do.set('state', "idle")
            mc.add(do)

    if len(session_statuses):
        for session in session_statuses:
            meta_dict = session['Video']
            del session['Video']
            so = FlexContainer("Device", attributes=session, show_size=False)
            md = FlexContainer("Meta", meta_dict, show_size=False)
            so.add(md)
            mc.add(so)

    return mc


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
    Log.Debug("Item count is " + str(len(data_array)))
    cast_string = JSON.StringFromObject(data_array)
    Data.Save('device_json', cast_string)
    last_cache = float(time.time())
    Data.Save('last_cache', last_cache)
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
    headers = sort_headers(["Type", "Section", "Include-Meta", "Meta-Size"])
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
    media_container = FlexContainer()
    add_meta = False
    if "Include-Meta" in headers:
        add_meta = headers["Include-Meta"].capitalize()
    else:
        if "Meta-Size" in headers:
            add_meta = True
    Log.Debug("Add meta is %s" % add_meta)
    for tag_type in records:
        tag_type_container = FlexContainer(tag_type["name"], limit=True)
        tags = tag_type["children"]
        for tag in tags:
            tag_container = FlexContainer(tag["type"], show_size=False)
            tag_container.set("title", tag["name"])
            tag_container.set("totalItems", tag["count"])
            metas = tag["children"]
            for meta in metas:
                meta_type_container = FlexContainer(meta["type"])
                meta_type_container.set("type", meta["name"])
                medias = meta["children"]
                medias = sorted(medias, key=lambda i: i['added'], reverse=True)
                item_count = len(medias)
                if "Meta-Size" in headers:
                    if len(medias) > headers["Meta-Size"]:
                        medias = medias[:headers["Meta-Size"]]
                    for media in medias:
                        media_item_container = FlexContainer("Media", media)
                        meta_type_container.add(media_item_container)
                tag_container.set(meta["name"] + "Count", item_count)
                if add_meta:
                    Log.Debug("Adding meta!!")
                    tag_container.add(meta_type_container)

            tag_type_container.add(tag_container)
        media_container.add(tag_type_container)
    return media_container


def query_library_sizes():
    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    results = []

    if cursor is not None:
        query = """SELECT sum(size), library_section_id, ls.name FROM media_items 
                    INNER JOIN library_sections AS ls
                    ON ls.id = library_section_id
                    GROUP BY library_section_id;"""

        for size, section_id, section_name in cursor.execute(query):
            dictz = {
                "size": size,
                "section_id": section_id,
                "section_name": section_name
            }
            results.append(dictz)

        close_connection(connection)

    return results


def query_users():
    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    users = {}

    if cursor is not None:
        query = """SELECT name, id from accounts;"""

        for name, id in cursor.execute(query):
            users[str(id)] = name

        close_connection(connection)
    return users


def query_library_quality():
    headers = sort_headers(["Container-Start", "Container-Size", "Type", "Section", "Sort"])
    container_start = headers.get("Container-Start") or 0
    container_size = headers.get("Container-Size") or 1000
    entitlements = get_entitlements()
    query_limit = "LIMIT %s, %s" % (container_start, container_size)

    section = headers.get("Section") or False
    sort = headers.get("Sort") or "DESC"

    query_selector = "AND md.library_section_id in %s" % entitlements
    type_selector = "(1, 4, 10)"
    if "Type" in headers:
        meta_type = headers.get("Type")
        if meta_type in META_TYPE_NAMES:
            meta_type = META_TYPE_NAMES[meta_type]
        if int(meta_type) == meta_type:
            type_selector = "(%s)" % meta_type
    query_selector += " AND md.metadata_type IN %s" % type_selector
    if section:
        query_selector += " AND md.library_section_id == section"

    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]
    results = {}

    if cursor is not None:
        query = """
            select md.title, md3.title as grandparentTitle, 
            md.id as ratingKey, mi.width, mi.height, mi.size as fileSize, mi.duration, mi.bitrate, mi.container, mi.video_codec as videoCodec,
            mi.audio_codec as audioCodec, mi.display_aspect_ratio as aspectRatio, mi.frames_per_second as framesPerSecond,
            mi.audio_channels as audioChannels, md.library_section_id as sectionId, md.metadata_type as metaType,
            ls.name as sectionName from media_items as mi
            inner join metadata_items as md
            on mi.metadata_item_id = md.id
            left join metadata_items as md2
            on md.parent_id = md2.id
            left join metadata_items as md3
            on md2.parent_id = md3.id
            inner join library_sections as ls
            on md.library_section_id = ls.id
            where md.library_section_id is not null
            %s
            order by mi.width %s, mi.height %s, mi.bitrate %s, mi.audio_channels %s, md.title desc
            %s;
            
        """ % (query_selector, sort, sort, sort, sort, query_limit)

        Log.Debug("Query is %s" % query)
        for row in cursor.execute(query):
            descriptions = cursor.getdescription()
            i = 0
            dictz = {}
            meta_type = "unknown"
            for title, foo in descriptions:
                value = row[i]
                if title == "ratingKey":
                    dictz["art"] = "/library/metadata" + str(value) + "/art"
                    dictz["thumb"] = "/library/metadata" + str(value) + "/thumb"

                dictz[title] = row[i]
                if title == "metaType":
                    meta_type = META_TYPE_IDS.get(value) or value
                    dictz[title] = meta_type

                i += 1
            meta_list = results.get(meta_type) or []
            if meta_type == "episode":
                dictz["banner"] = "/library/metadata/" + str(dictz["ratingKey"]) + "/banner/"
            meta_list.append(dictz)
            results[meta_type] = meta_list

        close_connection(connection)
    Log.Debug("No, really, sssss    : %s" % JSON.StringFromObject(results))
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
        for title, rating_key, year, parent_title, grandparent_title, contentRating, studio, country, score, \
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


def query_user_stats():
    headers = sort_headers(["Type", "Userid", "Username", "Container-Start", "Container-Size", "Devicename",
                            "Deviceid", "Title", "Start", "End", "Interval"])

    entitlements = get_entitlements()
    user_name_selector = headers.get("Username") or False
    user_id_selector = headers.get("Userid") or False
    device_name_selector = headers.get("Devicename") or False
    device_id_selector = headers.get("Deviceid") or False

    type_selector = "(1, 4, 10)"
    if "Type" in headers:
        meta_type = headers.get("Type")
        if meta_type in META_TYPE_NAMES:
            meta_type = META_TYPE_NAMES[meta_type]
        if int(meta_type) == meta_type:
            type_selector = "(%s)" % meta_type

    query_string = "WHERE sm.metadata_type IN %s" % type_selector
    if user_name_selector:
        query_string += " AND user_name='%s'" % user_name_selector

    if user_id_selector:
        query_string += " AND user_id='%s'" % user_name_selector

    if device_name_selector:
        query_string += " AND device_name='%s'" % user_name_selector

    if device_id_selector:
        query_string += " AND device_id='%s'" % user_name_selector

    interval = build_interval()
    start_date = interval[0]
    end_date = interval[1]
    query_string += " AND sm.at between ? AND ?"

    conn = fetch_cursor()
    cursor = conn[0]
    connection = conn[1]

    if cursor is not None:

        byte_query = """
                    SELECT accounts.name as user_name, sm.at, sm.metadata_type, accounts.id as user_id,
                    devices.name AS device_name, devices.identifier AS device_id, sb.bytes from statistics_media AS sm
                    INNER JOIN statistics_bandwidth as sb
                     ON sb.at = sm.at AND sb.account_id = sm.account_id AND sb.device_id = sm.device_id
                    INNER JOIN accounts
                     ON user_id = sm.account_id
                    INNER JOIN devices
                     ON devices.id = sm.device_id
                    %s
                    ORDER BY sm.at DESC;
                    """ % query_string
        params = (start_date, end_date)
        Log.Debug("Media stats/device query is '%s', params are %s" % (byte_query, params))
        user_results = {}
        user_dates = {}
        meta_count = 0
        for user_name, viewed_at, meta_type, user_id, device_name, device_id, data_bytes in cursor.execute(
                byte_query, params):
            if user_name not in user_results:
                user_results[user_name] = {}
            user_dict = user_results.get(user_name)

            meta_type = META_TYPE_IDS.get(meta_type) or meta_type
            item_list = user_dict.get(meta_type) or []
            last_active = user_dates.get(user_name) or 0

            last_viewed = int(time.mktime(datetime.datetime.strptime(viewed_at, "%Y-%m-%d %H:%M:%S").timetuple()))

            if last_viewed > last_active:
                last_active = last_viewed

            user_dates[user_name] = last_active

            dicts = {
                "userId": user_id,
                "userName": user_name,
                "lastViewedAt": last_viewed,
                "type": meta_type,
                "deviceName": device_name,
                "deviceId": device_id,
                "bytes": data_bytes
            }

            meta_count += 1
            item_list.append(dicts)
            user_results[user_name][meta_type] = item_list
            user_results[user_name]['lastSeen'] = last_active

        Log.Debug("Query results sorted, found %s records." % meta_count)

        query_string = "WHERE sm.metadata_type IN %s" % type_selector
        if user_name_selector:
            query_string += " AND user_name='%s'" % user_name_selector

        if user_id_selector:
            query_string += " AND user_id='%s'" % user_name_selector

        if device_name_selector:
            query_string += " AND device_name='%s'" % user_name_selector

        if device_id_selector:
            query_string += " AND device_id='%s'" % user_name_selector

        query_string += " AND sm.library_section_id in %s" % entitlements
        query_string += " AND sm.viewed_at BETWEEN ? AND ?"

        query = """
            SELECT sm.account_id as user_id, sm.library_section_id, sm.grandparent_title,
            sm.parent_title, sm.title, mi.id as rating_key, mi.tags_genre as genre, mi.tags_country as country, mi.year,
            sm.viewed_at, sm.metadata_type, accounts.name as user_name
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
            ORDER BY sm.viewed_at desc;
            """ % query_string

        params = (start_date, end_date)
        Log.Debug("Meta query is '%s', params are %s" % (query, params))

        view_results = {}
        meta_count = 0
        for user_id, library_section, grandparent_title, parent_title, title, rating_key, genre, country, year, \
                viewed_at, meta_type, user_name in cursor.execute(query, params):
            meta_type = META_TYPE_IDS.get(meta_type) or meta_type
            last_viewed = int(time.mktime(datetime.datetime.strptime(viewed_at, "%Y-%m-%d %H:%M:%S").timetuple()))

            user_dict = view_results.get(user_name) or {}
            view_meta_list = user_dict.get(meta_type) or []

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

            meta_count += 1
            view_meta_list.append(dicts)
            user_dict[meta_type] = view_meta_list
            view_results[user_name] = user_dict

        Log.Debug("Meta query completed, %s records retrieved." % meta_count)

        query3 = """
                    SELECT SUM(sb.bytes), sb.account_id AS user_id, devices.identifier, accounts.name AS user_name,
                    devices.name AS device_name, devices.identifier AS machine_identifier
                    FROM statistics_bandwidth AS sb
                    INNER JOIN accounts
                    ON accounts.id = sb.account_id
                    INNER JOIN devices
                    ON devices.id = sb.device_id
                    GROUP BY account_id, device_id;
                    """

        Log.Debug("Device query is '%s'" % query3)

        device_results = {}
        for total_bytes, user_id, device_id, user_name, device_name, machine_identifier in cursor.execute(query3):
            user_list = device_results.get(user_name) or []

            device_dict = {
                "userId": user_id,
                "userName": user_name,
                "deviceId": device_id,
                "deviceName": device_name,
                "machineIdentifier": machine_identifier,
                "totalBytes": total_bytes
            }
            user_list.append(device_dict)
            device_results[user_name] = user_list
        close_connection(connection)
        Log.Debug("Connection closed.")
        
        output = []
        container_start = headers.get("Container-Start") or DEFAULT_CONTAINER_START
        container_size = headers.get("Container-Size") or DEFAULT_CONTAINER_SIZE
        Log.Debug("Container starts size are %s and %s" % (container_start, container_size))
        for record_user, type_dict in view_results.items():
            user_id = False
            user_meta_results = {}
            user_dict = user_results.get(record_user) or {}
            device_list = device_results.get(record_user) or []
            last_seen = user_dict.get('lastSeen') or "NEVER"
            for meta_type, meta in type_dict.items():
                meta_list = user_meta_results.get(meta_type) or []
                for meta_record in meta:
                    user_meta_list = user_dict.get(meta_type) or []
                    record_date = str(meta_record["lastViewedAt"])[:6]
                    for check in user_meta_list:
                        check_date = str(check["lastViewedAt"])[:6]
                        if check_date == record_date:
                            for value in ["deviceName", "deviceId", "bytes"]:
                                meta_record[value] = check[value]
                    user_id = meta_record['userId']
                    del meta_record['userName']
                    del meta_record['userId']
                    meta_record['lastViewedAt'] = datetime.datetime.fromtimestamp(meta_record['lastViewedAt']).strftime(DATE_STRUCTURE)
                    meta_list.append(meta_record)
                meta_list = sorted(meta_list, key=lambda i: i['lastViewedAt'], reverse=True)
                user_meta_results[meta_type] = meta_list
            device_list = sorted(device_list, key=lambda i: i['totalBytes'], reverse=True)

            device_list = device_list[container_start:container_size]
            for meta_type, truncate in user_meta_results.items():
                truncate = truncate[container_start:container_size]
                user_meta_results[meta_type] = truncate
            last_seen = datetime.datetime.fromtimestamp(last_seen).strftime("%Y-%m-%d")
            user_record = {
                "meta": user_meta_results,
                "devices": device_list,
                "userName": record_user,
                "userId": user_id,
                "lastSeen": last_seen
            }

            output.append(user_record)

        output = sorted(output, key=lambda i: i['lastSeen'], reverse=True)
        return output
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
            grandparent_id, grandparent_title, grandparent_genre, grandparent_country, section \
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
    headers = sort_headers(["Type", "Section", "Start", "End", "Interval", "Sort"])

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
            SELECT
                sm.library_section_id as sectionId, sm.[index] as number, mi2.title as parentTitle,
                sm.title, sm.viewed_at as lastViewed, mi.id as ratingKey, sm.account_id as userId, accounts.name as userName,
                mi.metadata_type as metaType, mi2.id as parentId, mi2.metadata_type as parentMetaType, mi2.[index] as parentIndex,
                mi3.title as grandparentTitle, mi3.id as grandparentId, mi3.metadata_type as grandparentMetaType
            FROM metadata_item_views as sm
            INNER JOIN metadata_items as mi
                ON 
                mi.guid = sm.guid
                AND mi.title = sm.title
                and mi.library_section_id = sm.library_section_id
            LEFT JOIN metadata_items as mi2
                ON
                mi.parent_id = mi2.id
            LEFT JOIN metadata_items as mi3
                ON
                mi2.parent_id = mi3.id
            INNER JOIN accounts
                ON accounts.id = sm.account_id
                WHERE sm.viewed_at BETWEEN '%s' AND '%s'
            %s
            order by ratingKey, lastViewed;
        """ % (start_date, end_date, selector)

        Log.Debug("Query is '%s'" % query)
        results = {}
        for row in cursor.execute(query):
            descriptions = cursor.getdescription()
            count = 0
            record = {"playCount": 0, "users": {}}
            for key, foo in descriptions:
                value = row[count]
                record[key] = value

                if key == "metaType":
                    meta_type = META_TYPE_IDS.get(value) or value
                    record[key] = meta_type

                count += 1

            # Done building "item", now do stuff
            rating_key = record['ratingKey']
            user_name = record['userName']
            user_id = record['userId']
            viewed_at = record['lastViewed']

            record = results.get(rating_key) or record
            record_last = record['lastViewed']
            if record_last > viewed_at:
                last_viewed = record_last
            else:
                last_viewed = viewed_at
            view_count = record.get("playCount") or 0
            view_count += 1
            record["playCount"] = view_count

            users_dict = record.get('users') or {}
            user_record = users_dict.get(user_name) or {
                "userName": user_name,
                "userId": user_id
            }
            user_views = user_record.get("views") or []
            user_views.append({"dateViewed": viewed_at})
            user_record["views"] = user_views
            users_dict[user_name] = user_record
            record['users'] = users_dict
            record['lastViewed'] = last_viewed

            subkeys = ["parent", "grandparent"]

            for key in subkeys:
                # Build/check parent, grandparents
                if record[key + "Id"] is not None:
                    sub_id = record[key + "Id"]
                    sub_record = results.get(sub_id) or {
                        "ratingKey": record[key + 'Id'],
                        "title": record[key + 'Title'],
                        "lastViewed": last_viewed
                    }
                    sub_last = sub_record['lastViewed']
                    if sub_last > last_viewed:
                        last_viewed = sub_last
                    sub_record['lastViewed'] = last_viewed
                    sub_count = sub_record.get('playCount') or 0
                    sub_users_dict = sub_record.get('users') or {}
                    sub_user_record = sub_users_dict.get(user_name) or {
                        "userName": user_name,
                        "userId": user_id
                    }
                    sub_user_views = sub_user_record.get("views") or []
                    sub_user_views.append({"dateViewed": viewed_at})
                    sub_user_record["views"] = sub_user_views
                    sub_users_dict[user_name] = sub_user_record
                    sub_record['users'] = sub_users_dict
                    sub_view_count = sub_user_record.get("playCount") or 0
                    sub_view_count += 1
                    sub_user_record["playCount"] = sub_view_count
                    sub_users_dict[user_name] = sub_user_record
                    sub_count += 1
                    sub_meta_type = META_TYPE_IDS.get(record[key + 'MetaType']) or "unknown"
                    sub_record["playCount"] = sub_count
                    sub_record["metaType"] = sub_meta_type
                    if sub_meta_type == "album":
                        sub_record["artist"] = record[key + 'Title']
                        sub_record["artistKey"] = record[key + 'Id']
                    elif sub_meta_type == "season":
                        sub_record["show"] = record[key + 'Title']
                        sub_record["seriesKey"] = record[key + 'Id']
                        sub_record["index"] = record[key + 'Index']

                    results[sub_id] = sub_record

            results[rating_key] = record

        close_connection(connection)

    # Now sort by meta type
    sorted_media = {}
    for rating_key, media in results.items():

        remove_items = ["grandparentMetaType", "parentMetaType", "number", "parentIndex"]
        for remove in remove_items:
            if remove in media:
                del media[remove]

        meta_type = media["metaType"]
        meta_list = sorted_media.get(meta_type) or []
        media['userCount'] = len(media['users'])
        media["art"] = "/library/metadata" + str(rating_key) + "/art"
        media["key"] = "/library/metadata" + str(rating_key) + "/thumb"

        if meta_type == "episode":
            media["banner"] = "/library/metadata/" + str(rating_key) + "/banner/"

        playCount = 0
        for user, data in media["users"].items():
            playCount += len(data["views"])
        media['playCount'] = playCount
        meta_list.append(media)
        sorted_media[meta_type] = meta_list

    results = {}
    for meta_type, list_item in sorted_media.items():
        sort_keys = ["userCount", "playCount", "title"]
        if sort in sort_keys:
            param = sort
        else:
            param = "userCount"
        Log.Debug("Sorting stuff by %s" % param)

        sort_reverse = param != "title"
        list_item = sorted(list_item, key=lambda z: z[param], reverse=sort_reverse)
        sort_param_count = {}
        resort = True
        for item in list_item:
            sort_param_count[str(item[param])] = 1
            if len(sort_param_count) > 1:
                resort = False
                break

        if resort:
            if param == "userCount":
                param = "playCount"
            else:
                param = "title"
            Log.Debug("All %s items have same sort param, sorting by %s." % (meta_type, param))
            sort_reverse = param != "title"
            list_item = sorted(list_item, key=lambda i: i[param], reverse=sort_reverse)
        results[meta_type] = list_item

    return results


def fetch_cursor():
    cursor = None
    connection = None
    if os.environ["Loaded"]:
        import apsw
        connection = apsw.Connection(os.environ['LIBRARY_DB'])
        cursor = connection.cursor()
    return [cursor, connection]


def close_connection(connection):
   Log.Debug("No. I don't wanna.")


def vcr_ver():
    msvcr_map = {
        'msvcr120.dll': 'vc12',
        'msvcr130.dll': 'vc14'
    }
    try:
        import ctypes.util
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


def get_session_status():
    sessions = []
    token = False
    include_extra = False
    for key, value in Request.Headers.items():
        if key in ("X-Plex-Token", "Token"):
            Log.Debug("We have a Token")
            token = value
        if key in ("X-Plex-IncludeExtra", "IncludeExtra"):
            check = value
            if (check == True) | (check == "true"):
                include_extra = True

    if token:
        server_port = os.environ.get("PLEXSERVERPORT")
        if server_port is None:
            server_port = "32400"
        server_host = Network.Address
        if server_host is None:
            server_host = "localhost"

        try:
            my_url = "http://%s:%s/status/sessions?X-Plex-Token=%s" % (server_host, server_port, token)
        except TypeError:
            my_url = False
            pass

        if my_url:
            Log.Debug("Fetching status froms '%s'" % my_url)
            req = HTTP.Request(my_url)
            req.load()
            if hasattr(req, 'content'):
                client_data = req.content
                parsed_dict = xmltodict.parse(client_data)
                reply_dict = un_attribute(parsed_dict)
                mc = reply_dict['MediaContainer']
                sections = mc.get('Video') or []
                if type(sections) == dict:
                    sections = [sections]
                for session in sections:
                    client_dict = session.get('Player') or {}
                    del session['Player']
                    keep = ["Video', ""Media", "Genre", "User", "Country", "Session", "TranscodeSession"]
                    delete = []
                    if include_extra is False:
                        Log.Debug("Filtering")
                        for key in session:
                            if (key not in keep) & (type(session[key]) is not unicode):
                                delete.append(key)
                    for key in delete:
                        Log.Debug("Should be deleting key %s" % key)
                        del session[key]

                    client_dict['Video'] = session
                    sessions.append(client_dict)
    return sessions


def un_attribute(xml_dict):
    dict_out = {}
    for key, value in xml_dict.items():
        fixed = key.replace("@", "")
        if type(value) is unicode:
            value = value
        elif type(value) is list:
            new_list = []
            for item in value:
                new_list.append(un_attribute(item))
            value = new_list
        else:
            value = un_attribute(value)
        dict_out[fixed] = value
    return dict_out


####################################
# These functions are for utility stuff
def get_time_difference(time_start, time_end):
    time_diff = time_end - time_start
    return time_diff.total_seconds() / 60


def sort_headers(header_list, strict=False):
    returns = {}
    for key, value in Request.Headers.items():

        for item in header_list:
            if key in ("X-Plex-" + item, item):
                value = unicode(value)
                try:
                    test = int(value)
                except ValueError:
                    Log.Debug("Value is not a string.")
                    pass
                else:
                    value = test
                Log.Debug("Value for %s is '%s'" % (item, value))

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
    start_date = False
    end_date = False
    interval = False

    if "End" in headers:
        end_check = headers.get("End")
        valid = validate_date(end_check)
        if valid:
            end_date = valid

    if "Start" in headers:
        start_check = headers.get("Start")
        valid = validate_date(start_check)
        if valid:
            Log.Debug("We have a vv start date, we'll use that.")
            start_date = valid

    if "Interval" in headers:
        int_check = headers.get("Interval")
        if int(int_check) == int_check:
            interval = int_check

    if start_date & end_date:
        return [start_date, end_date]

    if start_date & interval:
        start_date = datetime.datetime.strftime(datetime.datetime.strptime(
            start_date, DATE_STRUCTURE) + datetime.timedelta(days=interval), DATE_STRUCTURE)
        return [start_date, end_date]

    if end_date & interval:
        start_date = datetime.datetime.strftime(datetime.datetime.strptime(
            end_date, DATE_STRUCTURE) - datetime.timedelta(days=interval), DATE_STRUCTURE)
        return [start_date, end_date]

    # Default behavior is to return 365 (or specified interval) days worth of records from today.
    Log.Debug("Returning default interval")
    if interval is False:
        interval = 365
    end_int = datetime.datetime.now()
    start_int = end_int - datetime.timedelta(days=interval)
    start_date = datetime.datetime.strftime(start_int, DATE_STRUCTURE)
    end_date = datetime.datetime.strftime(end_int, DATE_STRUCTURE)
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
