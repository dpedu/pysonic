from collections import defaultdict
from bs4 import BeautifulSoup
import re
import cherrypy
import json

CALLBACK_RE = re.compile(r'^[a-zA-Z0-9_]+$')

response_formats = defaultdict(lambda: "render_xml")
response_formats["json"] = "render_json"
response_formats["jsonp"] = "render_jsonp"

response_headers = defaultdict(lambda: "text/xml; charset=utf-8")
response_headers["json"] = "application/json; charset=utf-8"
response_headers["jsonp"] = "text/javascript; charset=utf-8"


def formatresponse(func):
    """
    Decorator for rendering ApiResponse responses based on requested response type
    """
    def wrapper(*args, **kwargs):
        response = func(*args, **kwargs)
        response_format = kwargs.get("f", "xml")
        callback = kwargs.get("callback", None)
        cherrypy.response.headers['Content-Type'] = response_headers[response_format]
        renderer = getattr(response, response_formats[response_format])
        if response_format == "jsonp":
            if callback is None:
                return response.render_xml().encode('UTF-8')  # copy original subsonic behavior
            else:
                return renderer(callback).encode('UTF-8')
        return renderer().encode('UTF-8')
    return wrapper


class ApiResponse(object):
    def __init__(self, status="ok", version="1.15.0"):
        """
        ApiResponses are python data structures that can be converted to other formats. The response has a status and a
        version. The response data structure is stored in self.data and follows these rules:
        - self.data is a dict
        - the dict's values become either child nodes or attributes, named by the key
        - lists become many oner one child
        - dict values are not allowed
        - all other types (str, int, NoneType) are attributes
        :param status:
        :param version:
        """
        self.status = status
        self.version = version
        self.data = defaultdict(lambda: list())

    def add_child(self, _type, _parent="", _real_parent=None, **kwargs):
        kwargs = {k: v for k, v in kwargs.items() if v or type(v) is int}  # filter out empty keys (0 is ok)
        parent = _real_parent if _real_parent else self.get_child(_parent)
        m = defaultdict(lambda: list())
        m.update(dict(kwargs))
        parent[_type].append(m)
        return m

    def get_child(self, _path):
        parent_path = _path.split(".")
        parent = self.data
        for item in parent_path:
            if not item:
                continue
            parent = parent.get(item)[0]
        return parent

    def set_attrs(self, _path, **attrs):
        parent = self.get_child(_path)
        if type(parent) not in (dict, defaultdict):
            raise Exception("wot")
        parent.update(attrs)

    def render_json(self):
        def _flatten_json(item):
            """
            Convert defaultdicts to dicts and remove lists where node has 1 or no child
            """
            listed_attrs = ["folder"]
            d = {}
            for k, v in item.items():
                if type(v) is list:
                    if len(v) > 1:
                        d[k] = []
                        for subitem in v:
                            d[k].append(_flatten_json(subitem))
                    elif len(v) == 1:
                        d[k] = _flatten_json(v[0])
                    else:
                        d[k] = {}
                else:
                    d[k] = [v] if k in listed_attrs else v
            return d

        data = _flatten_json(self.data)
        return json.dumps({"subsonic-response": dict(status=self.status, version=self.version, **data)}, indent=4)

    def render_jsonp(self, callback):
        assert CALLBACK_RE.match(callback), "Invalid callback"
        return "{}({});".format(callback, self.render_json())

    def render_xml(self):
        text_attrs = ['largeImageUrl', 'musicBrainzId', 'smallImageUrl', 'mediumImageUrl', 'lastFmUrl', 'biography',
                      'folder']
        selftext_attrs = ['value']
        # These attributes will be placed in <hello>{{ value }}</hello> tags instead of hello="{{ value }}" on parent
        doc = BeautifulSoup('', features='lxml-xml')
        root = doc.new_tag("subsonic-response", xmlns="http://subsonic.org/restapi",
                           status=self.status,
                           version=self.version)
        doc.append(root)

        def _render_xml(node, parent):
            """
            For every key in the node dict, the parent gets a new child tag with name == key
            If the value is a dict, it becomes the new tag's attrs
            If the value is a list, the parent gets many new tags with each dict as attrs
            If the value is str int etc, parent gets attrs
            """
            for key, value in node.items():
                if type(value) in (dict, defaultdict):
                    tag = doc.new_tag(key)
                    parent.append(tag)
                    tag.attrs.update(value)
                elif type(value) is list:
                    for item in value:
                        tag = doc.new_tag(key)
                        parent.append(tag)
                        _render_xml(item, tag)
                else:
                    if key in text_attrs:
                        tag = doc.new_tag(key)
                        parent.append(tag)
                        tag.append(str(value))
                    elif key in selftext_attrs:
                        parent.append(str(value))
                    else:
                        parent.attrs[key] = value
        _render_xml(self.data, root)
        return doc.prettify()
