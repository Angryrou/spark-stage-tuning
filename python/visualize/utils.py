# Author(s): Chenghao Lyu <chenghao at cs dot umass dot edu>
#
# Description: TODO
#
# Created at 29/06/2023

import networkx as nx
import os, json, urllib.request
from IPython.display import Image, display
import seaborn as sns
import matplotlib.pyplot as plt


class JsonUtils(object):

    @staticmethod
    def load_json(file):
        assert os.path.exists(file), FileNotFoundError(file)
        with open(file) as f:
            try:
                return json.load(f)
            except:
                raise Exception(f"{f} cannot be parsed as a JSON file")

    @staticmethod
    def load_json_from_str(s: str):
        return json.loads(s)

    @staticmethod
    def print_dict(d: dict):
        print(json.dumps(d, indent=2))

    @staticmethod
    def load_json_from_url(url_str, timeout=10):
        try:
            with urllib.request.urlopen(url_str, timeout=timeout) as url:
                data = json.load(url)
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except:
            raise Exception(f"failed to load from {url_str}")
        return data

    @staticmethod
    def extract_json_list(l, keys: list):
        for key in keys:
            for l_ in l:
                assert key in l_, f"{key} is not in {l_}"
        return [[l_[key] for l_ in l] for key in keys]

    @staticmethod
    def save_json(d, des_file):
        json_data = json.dumps(d, indent=2)
        with open(des_file, "w") as f:
            f.write(json_data)

    @staticmethod
    def dump2str(d, indent: int = None):
        return json.dumps(d, indent=indent)


def plot_nx_graph(G: nx.DiGraph, node_id2name: dict, dir_name: str, title: str, prefix: bool = True,
                  color: str = None, fillcolor: str = None, edge_colors=None, edge_styles=None,
                  jupyter: bool = False, out_format: str = "pdf"):
    p = nx.drawing.nx_pydot.to_pydot(G)
    for i, name in node_id2name.items():
        found_nodes = p.get_node(str(i))
        assert len(found_nodes) == 1
        node = found_nodes[0]
        if prefix:
            node.set_label(f"{i}-{name}")
        else:
            node.set_label(name)
        if color is not None:
            node.set("color", color)
            if fillcolor is not None:
                node.set("style", "filled")
                node.set("fillcolor", color)
    if edge_colors is not None:
        for edge in p.get_edges():
            edge_type = edge.obj_dict['attributes']['type']
            assert edge_type in edge_colors
            edge.set_color(edge_colors[edge_type])
    if edge_styles is not None:
        for edge in p.get_edges():
            edge_type = edge.obj_dict['attributes']['type']
            assert edge_type in edge_styles
            edge.set_style(edge_styles[edge_type])
    dir_to_save = dir_name
    os.makedirs(dir_to_save, exist_ok=True)
    if out_format == "png":
        p.write_png(dir_to_save + '/' + title + '.png')
        if jupyter:
            display(Image(dir_to_save + '/' + title + '.png'))
    elif out_format == "pdf":
        p.write_pdf(dir_to_save + '/' + title + '.pdf')
        if jupyter:
            p.write_png(dir_to_save + '/' + title + '.png')
            display(Image(dir_to_save + '/' + title + '.png'))

