# cording: UTF-8
from SPARQLWrapper import SPARQLWrapper, JSON
import re
import subprocess
import sys
from neo4jrestclient.client import GraphDatabase
import requests
import spotlight

resource_pattern = re.compile("^http://ja\.dbpedia\.org/resource/(.+)$")
triple_pattern = re.compile("^<http://ja\.dbpedia\.org/resource/(.+)> <(.+)> <http://ja\.dbpedia\.org/resource/(.+)> \.$")

def setIndex():

    print("\t|-creating list ...")
    output_dict = open("WikipediaRelatedness/wikiRelate/dict.tsv", 'w')
    output_tmp = open("WikipediaRelatedness/wikiRelate/tmp.tsv", 'w')
    nodes = set()
    i = 0
    for line in open("./link.ttl", 'r'):

        m =triple_pattern.match(line)
        if not m:
            continue

        s, p, o = m.groups()
        nodes.add(s)
        nodes.add(o)
        output_tmp.write(s+"\t"+o+"\n")

    print("\t|-finished creating list")

    print("\t|-creating dictionary")
    index = 0
    nodes_dict = dict()
    for node in nodes:
        nodes_dict[node] = index
        output_dict.write(node+"\t"+str(index)+"\n")
        index += 1
    output_dict.close()
    output_tmp.close()
    del nodes
    print("\t|-finished creating dictionary")

    print("\t|-creating links ...")
    output_link = open("WikipediaRelatedness/wikiRelate/link.tsv", 'w')
    i = 0
    for link in open("WikipediaRelatedness/wikiRelate/tmp.tsv", 'r'):
        s, o = link[:-1].split("\t")
        output_link.write(str(nodes_dict[s])+"\t"+str(nodes_dict[o])+"\n")
        output_link.write(str(nodes_dict[o])+"\t"+str(nodes_dict[s])+"\n")
    output_link.close()
    del nodes_dict
    print("\t|-finished creating links")

    print("\t|-creating index ...")
    subprocess.run("./runGraphIndex.sh", shell=True)
    print("\t|-finished creating index")


def abstFetcher(sparql, limmit, offset):
    ### prefix
    query = "prefix ontology: <http://dbpedia.org/ontology/>\n"
    ### query
    query = query+"select *\n"
    query = query+"where {\n"
    query = query+"?s ontology:abstract ?o.\n"
    query = query+"} limit "+str(limmit)+" offset "+str(offset)

    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    return sparql.query().convert()

def linkFetcher(sparql, node):
    ### prefix
    query = "prefix resource: <http://ja.dbpedia.org/resource/>\n"
    query = query+"prefix ontology: <http://dbpedia.org/ontology/>\n"
    ### query
    query = query+"select *\n"
    query = query+"where {\n"
    query = query+"resource:" + node + " ontology:wikiPageWikiLink ?o.\n"
    query = query+"} limit 1000"

    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)

    return sparql.query().convert()

def hintFetcher(abst, links):
    hints = set()
    for candi in links:
        if candi in abst:
            hints.add(candi)

    return list(hints)

def hintRank(node_dict, node, hint_dict):
    
    output = open("./WikipediaRelatedness/wikiRelate/query.tsv", "w")
    tmp = 0
    for hint in hint_dict.keys():
        if hint in list(node_dict.keys()):
            output.write(node_dict[node]+"\t"+node_dict[hint]+"\n")
            tmp += 1
    output.close()

    if tmp == 0:
        return []
    
    # compute relatedness
    subprocess.run("./runWikiRelate.sh", shell=True)
    
    relatedness = open("WikipediaRelatedness/wikiRelate/relatedness.tsv", "r").readlines()
    relatedness = list(map(lambda x: ((hint_dict[dict_reverse(node_dict, x.split("\t")[1])], dict_reverse(node_dict, x.split("\t")[1])), float(x.split("\t")[2][:-1])), relatedness))
    relatedness.sort(key=lambda x: x[1], reverse=True)
    
    return relatedness

def dict_reverse(d, value):
    keys = [k for k, v in d.items() if v == value]
    return keys[0]

def storeHint(node, hints):

    url = "http://" + sys.argv[1] + ":" + sys.argv[2] + "@133.1.244.71:7474/db/data/"
    gdb = GraphDatabase(url)
    #gdb.query("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r", data_contents=True)

    # store node
    n = gdb.nodes.create(name=node)
    n.labels.add("Node")

    # store hints
    rank = 1
    for item in hints:
        hint, score = item
        if score > 0.0:
            hint, resource = hint
            h = gdb.nodes.create(name=hint, wikipedia=resource, rank=rank, score=score)
            h.labels.add("Hint")
            n.relationships.create("hint", h, f=node, t=hint)
            rank += 1

if __name__ == "__main__":

    # print("start phase 1 ...")
    # setIndex()
    # print("finished phase 1 ")

    # url = "http://" + sys.argv[1] + ":" + sys.argv[2] + "@133.1.244.71:7474/db/data/"
    # gdb = GraphDatabase(url)
    # gdb.query("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r", data_contents=True)

    print("start phease 2 ...")
    print("\t|-building dictionary ...")
    sparql = SPARQLWrapper('http://ja.dbpedia.org/sparql')
    node_dict = dict()
    for line in open("WikipediaRelatedness/wikiRelate/dict.tsv", 'r'):
        k, v = line[:-1].split("\t")
        node_dict[k] = v
    print("\t|-finished building dictionary")

    print("\t|-storing hints ...")
    spotlight_server = 'http://133.1.244.71:2250/rest/annotate'
    for i in range(0, 200):
        limit = 10000
        offset = i * limit
        results = abstFetcher(sparql, limit, offset)

        for result in (results["results"]["bindings"]):
            if result["o"]["xml:lang"] == "ja":
                node = re.findall(resource_pattern, result["s"]["value"])[0]
                if node in list(node_dict.keys()):
                    abst = result["o"]["value"]
                    try:
                        annotations = spotlight.annotate(spotlight_server, abst)
                        hints = dict()
                        for item in annotations:
                            hints[re.findall(resource_pattern, item["URI"])[0]] = item["surfaceForm"]
                        hints = hintRank(node_dict, node, hints)
                        storeHint(node, hints)

                    except spotlight.SpotlightException:
                        pass
                    except ValueError:
                        pass
                    except requests.exceptions.HTTPError:
                        pass



