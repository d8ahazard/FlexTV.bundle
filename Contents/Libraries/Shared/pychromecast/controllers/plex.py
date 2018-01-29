"""
Controller to interface with the Plex-app.
"""
from . import BaseController
from ..config import APP_PLEX

STREAM_TYPE_UNKNOWN = "UNKNOWN"
STREAM_TYPE_BUFFERED = "BUFFERED"
STREAM_TYPE_LIVE = "LIVE"
MESSAGE_TYPE = 'type'

TYPE_PLAY = "PLAY"
TYPE_PAUSE = "PAUSE"
TYPE_STOP = "STOP"
TYPE_STEPFORWARD = "STEPFORWARD"
TYPE_STEPBACKWARD = "STEPBACK"
TYPE_PREVIOUS = "PREVIOUS"
TYPE_NEXT = "NEXT"
TYPE_LOAD = "LOAD"
TYPE_SEEK = "SEEK"


class PlexController(BaseController):
    """ Controller to interact with Plex namespace. """

    def __init__(self):
        super(PlexController, self).__init__(
            "urn:x-cast:plex", "9AC194DC")

        self.app_id = "9AC194DC"
        self.namespace = "urn:x-cast:plex"
        self.request_id = 0
        self.media_session_id = 0
        self.receiver = None
        self.last_message = "No messages sent"

    def set_volume(self, percent):
        percent = float(percent) / 100
        self._socket_client.receiver_controller.set_volume(percent)

    def volume_up(self, cast):
        cast.volume_up()

    def volume_down(self, cast):
        cast.volume_down()

    def mute(self, cast, status):
        cast.set_volume_muted(status)

    def stop(self):
        """ Send stop command. """
        self.request_id += 1
        self.send_message({MESSAGE_TYPE: TYPE_STOP})

    def pause(self):
        """ Send pause command. """
        self.request_id += 1
        self.send_message({MESSAGE_TYPE: TYPE_PAUSE})

    def play(self):
        """ Send play command. """
        self.request_id += 1
        self.send_message({MESSAGE_TYPE: TYPE_PLAY})

    def previous(self):
        """ Send previous command. """
        self.request_id += 1
        self.send_message({MESSAGE_TYPE: TYPE_PREVIOUS})

    def next(self):
        """ Send next command. """
        self.request_id += 1
        self.send_message({MESSAGE_TYPE: TYPE_NEXT})

    def get_last_message(self):
        return self.last_message

    def play_media(self, item,type):
        def app_launched_callback():
            self.set_load(item, type)

        receiver_ctrl = self._socket_client.receiver_controller
        receiver_ctrl.launch_app(self.app_id,
                                 callback_function=app_launched_callback)

    def set_load(self, params,type):
        self.namespace = "urn:x-cast:com.google.cast.media"
        playQueueID = params['Queueid']
        self.request_id += 1  # Update
        # Session ID
        if (type == 'audio') | (type == 'group'):
        # if params['Transcodervideo'] != "true":
            tv = True
        else:
            tv = False
        # TODO: Get the play queue type.

        address = params['Serveruri'].split(":")[1]
        port = params['Serveruri'].split(":")[2]
        msg = {
          "type": "LOAD",
          "requestId": self.request_id,
          "sessionId": self.request_id,   #Does this need to be static?
          "media": {
            "contentId": params['Contentid'],
            "streamType": "BUFFERED",
            "customData": {
              "playQueueType": params['Contenttype'],  #TODO: GET THIS RIGHT
              "providerIdentifier": "com.plexapp.plugins.library",
              "containerKey": "/playQueues/{}?own=1&window=200".format(playQueueID),
              "offset": params['Offset'],
              "directPlay": True,
              "directStream": True,
              "audioBoost": 100,
              "server": {
                "machineIdentifier": params["Serverid"],
                "transcoderVideo": True,
                "transcoderVideoRemuxOnly": False,
                "transcoderAudio": True,
                "version": "1.11.0.4666",
                "myPlexSubscription": True,
                "isVerifiedHostname": True,
                "protocol": "https",
                "address": address,
                "port": port,
                "accessToken": params["Token"]
              },
              "primaryServer": {
                "machineIdentifier": params["Serverid"],
                "transcoderVideo": True,
                "transcoderVideoRemuxOnly": False,
                "transcoderAudio": True,
                "version": "1.11.0.4666",
                "myPlexSubscription": True,
                "isVerifiedHostname": True,
                "protocol": "https",
                "address": address,
                "port": port,
                "accessToken": params["Token"]
              },
              "user": {
                "username": {"username": params["Username"]}
              }
            }
          },
          "autoplay": True,
          "currentTime": params['Offset']
        }
        # msg = {
        #     "type": "LOAD",
        #     "requestId": self.request_id,
        #     "sessionId": "python_player",  # Update
        #     "autoplay": True,
        #     "currentTime": 0,
        #     "media": {
        #         "contentId": params['Contentid'],
        #         "streamType": 'BUFFERED',
        #         "contentType": params['Contenttype'],
        #         "customData": {
        #             "offset": params['Offset'],
        #             "server": {
        #                 "machineIdentifier": params["Serverid"],
        #                 "transcoderVideo": True,  # Need to find a variables for this
        #                 "transcoderVideoRemuxOnly": False,  # Need to find a variable for this
        #                 "transcoderAudio": True,  # Need to find a variable for this
        #                 "version": "1.1",   # TODO: Look this up proper-like
        #                 "myPlexSubscription": True,  # TODO: Look this up proper-like too
        #                 "isVerifiedHostname": True,  # Need to find a variable for this
        #                 "protocol": "https",
        #                 "address": address,
        #                 "port": port,
        #                 "accessToken": params["Token"],
        #             },
        #             "user": {"username": params["Username"]},
        #             "containerKey": "/playQueues/{}?own=1&window=200".format(playQueueID),
        #         },
        #     }
        # }

        self.send_message(msg, inc_session_id=True)
        self.namespace = "urn:x-cast:plex"
        self.last_message = msg

    def receive_message(self, message, data):
        """ Called when a media message is received. """
        if data[MESSAGE_TYPE] == TYPE_MEDIA_STATUS:
            return True

        else:
            return False
