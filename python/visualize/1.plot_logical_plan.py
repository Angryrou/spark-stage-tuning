# Author(s): Chenghao Lyu <chenghao at cs dot umass dot edu>
#
# Description: for plot logical plan
#
# Created at 29/06/2023


import os, argparse

import networkx as nx

from visualize.utils import JsonUtils, plot_nx_graph


class Args():
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("-o", "--out-path", type=str, default="../../outs")
        self.parser.add_argument("-f", "--file-name", type=str, default="TPCDS_1-1.json")
        self.parser.add_argument("-p", "--fig-path", type=str, default="./figs")

    def parse(self):
        return self.parser.parse_args()


args = Args().parse()
file = os.path.join(args.out_path, args.file_name)
fig_title = args.file_name.split(".")[0]
fig_path = args.fig_path
assert os.path.exists(file)
j = JsonUtils.load_json(file)
links = j["links"]
edge_types = ["Operator", "Subquery"]

G = nx.DiGraph()
name_dict = {}
for link in links:
    src, dst = link["fromId"], link["toId"]
    G.add_edge(src, dst, type=link["linkType"])
    if dst not in name_dict:
        name_dict[dst] = link["toName"]
    else:
        assert name_dict[dst] == link["toName"]
    if src not in name_dict:
        name_dict[src] = link["fromName"]
    else:
        assert name_dict[src] == link["fromName"]
    print(f"({src}){name_dict[src]} -> ({dst}){name_dict[dst]}")

edge_styles = {
    "Operator": "solid",
    "Subquery": "dashed"
}
plot_nx_graph(G, node_id2name=name_dict, dir_name=fig_path, title=fig_title,
              edge_colors=None, edge_styles=edge_styles, prefix=True)

for i, o in name_dict.items():
    print(i, o, j["operators"][str(i)]["className"].split(".")[-1])