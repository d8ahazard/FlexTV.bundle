"""
This is a custom Object Class that we can use to emulate the XML structure of Plex's API output
For each container type you want to create, specify self.name, and an optional list of
accpetable attributes.

"""
import datetime

ObjectClass = getattr(getattr(Redirect, "_object_class"), "__bases__")[0]


class CustomContainer(ObjectClass):

    def __init__(self, attributes=None, children=None):
        ObjectClass.__init__(self, "")
        self.children = children
        self.attributes = attributes
        self.SetHeader("Content-Type", "application/xml")
        self.items = []

    def Content(self):
        xml = self.to_xml()
        return xml

    def add(self, obj):
        if self.children is None:
            append = True
        else:
            append = False
            for child in self.children:
                if obj.name == child:
                    append = True

        if append is True:
            self.items.append(obj)
        else:
            Log.Error("Child type %s is not allowed" % obj.name)

    def to_xml(self):
        string = ""
        string += ('<' + self.name)

        if self.show_size is True:
            size = str(len(self.items))
            string += (' size="' + size + '"')

        if self.dict is not None:
            for key, value in self.dict.items():
                allowed = True
                if self.attributes is not None:
                    allowed = False
                    for attribute in self.attributes:
                        if key == attribute:
                            allowed = True

                if allowed is True:
                    value = str(value)
                    value = value.replace("&", "&amp;")
                    value = value.replace("<", "&lt;")
                    value = value.replace(">", "&gt;")
                    value = value.replace("\"", "&quot;")
                    string += (" " + key + '="' + value + '"')
                else:
                    Log.Error("Attribute " + key + " is not allowed in this container.")

        count = len(self.items)
        if count >= 1:
            string += '>\n'
            for obj in self.items:
                if type(obj) == str:
                    Log.Error("Here's a string: '%s" % obj)
                else:
                    string += obj.to_xml()

            string += '</' + self.name + '>'

        else:
            string += '/>\n'

        return string


# Class to emulate proper Plex media container
# TODO: Auto grab version number from init
class MediaContainer(CustomContainer):
    def __init__(self, dict=None):
        self.show_size = True
        self.dict = dict
        self.name = "MediaContainer"
        CustomContainer.__init__(self)


# Class to emulate proper Plex media container
class MetaContainer(CustomContainer):
    def __init__(self, dict=None):
        self.show_size = True
        self.dict = dict
        self.name = "MetaData"
        CustomContainer.__init__(self)


# Class to emulate proper Plex device container
class StatContainer(CustomContainer):
    def __init__(self, dict=None):
        self.show_size = False
        self.dict = dict
        self.name = "Tag"
        allowed_attributes = None
        allowed_children = [
            "Connection"
        ]

        CustomContainer.__init__(self, allowed_attributes, allowed_children)


class UserContainer(CustomContainer):
    def __init__(self, dict=None):
        self.show_size = False
        self.dict = dict
        self.name = "User"
        allowed_attributes = None
        allowed_children = [
            "View", "Media", "Stats"
        ]

        CustomContainer.__init__(self, allowed_attributes, allowed_children)


class ViewContainer(CustomContainer):
    def __init__(self, dict=None):
        self.show_size = False
        self.dict = dict
        self.name = "View"
        allowed_attributes = None
        allowed_children = None

        CustomContainer.__init__(self, allowed_attributes, allowed_children)


class AnyContainer(CustomContainer):
    def __init__(self, dict=None, name="Any", show_size=True):
        self.show_size = show_size
        self.dict = dict
        self.name = name
        allowed_attributes = None
        allowed_children = None

        CustomContainer.__init__(self, allowed_attributes, allowed_children)


class ZipObject(ObjectClass):
    def __init__(self, data):
        ObjectClass.__init__(self, "")
        self.zipdata = data
        self.SetHeader("Content-Type", "application/zip")

    def Content(self):
        self.SetHeader("Content-Disposition",
                       'attachment; filename="' + datetime.datetime.now().strftime("Logs_%y%m%d_%H-%M-%S.zip")
                       + '"')
        return self.zipdata
