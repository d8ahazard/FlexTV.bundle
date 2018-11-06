import json

ObjectClass = getattr(getattr(Redirect, "_object_class"), "__bases__")[0]


class FlexContainer(ObjectClass):

    def __init__(self, tag="MediaContainer", attributes=None, show_size=True,
                 allowed_attributes=None, allowed_children=None, limit=False):
        ObjectClass.__init__(self, "")
        encoding = "xml"
        self.container_start = 0
        self.container_size = False
        for key, value in Request.Headers.items():
            if (key == "Accept") | (key == "X-Plex-Accept"):
                if (value == "application/json") | (value == "json"):
                    encoding = "json"
                    break
            if (key == "Container-Start") | (key == "X-Plex-Container-Start"):
                self.container_start = int(value)
            if (key == "Container-Size") | (key == "X-Plex-Container-Size"):
                if limit:
                    self.container_size = int(value)
        self.encoding = encoding
        self.tag = tag
        self.child_string = ""
        self.child_strings = []
        self.children = []
        self.attributes = attributes
        self.show_size = show_size
        self.allowed_attributes = allowed_attributes
        self.allowed_children = allowed_children
        if self.tag == "MediaContainer":
            if self.encoding == "xml":
                self.SetHeader("Content-Type", "application/xml")
            else:
                self.SetHeader("Content-Type", "application/json")

    def Content(self):
        if self.encoding == "xml":
            encoded = self.to_xml()
            return encoded
        else:
            encoded = self.to_json()
            return encoded

    def add(self, obj):
        self.children.append(obj)
        new_string = obj.to_xml()
        self.child_strings.append(new_string)

    def set(self, key, value):
        if self.attributes is None:
            self.attributes = {}
        self.attributes[key] = value

    def get(self, key):
        if self.attributes is not None:
            return self.attributes.get(key) or None
        return None

    def size(self):
        if self.children is None:
            return 0
        else:
            return len(self.children)

    def to_xml(self):
        self_tag = str(self.tag).capitalize()

        self_attributes = self.attributes
        if self.container_size:
            container_max = self.container_start + self.container_size
            if len(self.child_strings) > container_max:
                self.child_strings = self.child_strings[self.container_start:container_max]

        child_strings = self.child_strings

        if self.show_size is True:
            if self_attributes is None:
                self_attributes = {}
            if "size" in self_attributes:
                self_attributes["oldSize"] = self_attributes["size"]
            if self.children is None:
                self_size = 0
            else:
                self_size = len(self.child_strings)
            self_attributes["size"] = self_size

        attribute_string = ""
        if self_attributes is not None:
            for key in self_attributes:
                allowed = True
                if self.allowed_attributes is not None:
                    allowed = False
                    if key in self.allowed_attributes:
                        allowed = True

                value = self_attributes.get(key)
                if allowed:
                    if value not in [None, ""]:
                        replace = {
                            "&": "&amp;",
                            "<": "&lt;",
                            ">": "&gt;",
                            "\"": "&quot;"
                        }
                        for search, replace in replace.items():
                            value = str(value).replace(search, replace)

                        attribute_string += ' %s="%s"' % (key, value)
                else:
                    Log.Error("Attribute " + key + " is not allowed in this container.")

        if len(child_strings) == 0:
            string = "<%s%s/>" % (self_tag, attribute_string)
        else:
            child_string = " ".join(child_strings)
            string = "<%s%s>%s</%s>\n" % (self_tag, attribute_string, child_string, self_tag)

        return string

    def to_json(self):

        json_obj = self.attributes
        if json_obj is None:
            json_obj = {}
        self_size = 0
        if self.show_size is True:
            if "size" in json_obj:
                json_obj["oldSize"] = json_obj["size"]
            if self.children is not None:
                if self.container_size:
                    container_max = self.container_start + self.container_size
                    if len(self.children) > container_max:
                        self.children = self.children[self.container_start:container_max]
                self_size = len(self.children)
            json_obj["size"] = self_size

        for child in self.children:
            child_dict = child.to_json()
            (key, value) = child_dict
            child_list = json_obj.get(key) or []
            child_list.append(value)
            json_obj[key] = child_list

        if self.tag == "MediaContainer":
            result = {
                self.tag: json_obj
            }
            if self.show_size:
                result['size'] = self_size
            json_string = json.dumps(result, sort_keys=False, indent=4, separators=(',', ': '))

            return json_string
        else:
            self_tag = str(self.tag).capitalize()
            Log.Debug("Appending child %s " % self_tag)
            result = (self_tag, json_obj)

            return result
