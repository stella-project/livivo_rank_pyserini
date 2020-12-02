import os
from pyserini.index.__main__ import JIndexCollection
from pyserini.search import SimpleSearcher
import jsonlines


class Ranker(object):

    def __init__(self):
        self.idx = None

    def index(self):

        try:
            os.mkdir('./convert/')
        except OSError as error:
            print(error)

        with jsonlines.open('./convert/output.jsonl', mode='w') as writer:
            for file in os.listdir("./data/livivo/documents/"):
                if file.endswith(".jsonl"):
                    with jsonlines.open(os.path.join("./data/livivo/documents", file)) as reader:
                        for obj in reader:
                            title = obj.get('TITLE') or ''
                            title = title[0] if type(title) is list else title
                            abstract = obj.get('ABSTRACT') or ''
                            abstract = abstract[0] if type(abstract) is list else abstract
                            try:
                                doc = {'id': obj.get('DBRECORDID'),
                                       'contents': ' '.join([title, abstract])}
                                writer.write(doc)
                            except Exception as e:
                                print(e)

        try:
            os.mkdir('./indexes/')
        except OSError as error:
            print(error)

        args = ["-collection", "JsonCollection",
                "-generator", "DefaultLuceneDocumentGenerator",
                "-threads", "1",
                "-input", "./convert",
                "-index", "./indexes/livivo",
                "-storePositions",
                "-storeDocvectors",
                "-storeRaw"]

        JIndexCollection.main(args)
        self.searcher = SimpleSearcher('indexes/livivo')

    def rank_publications(self, query, page, rpp):

        itemlist = []

        if query is not None:
            if self.idx is None:
                try:
                    self.searcher = SimpleSearcher('indexes/livivo')
                except Exception as e:
                    print('No index available: ', e)

            if self.searcher is not None:
                hits = self.searcher.search(query, k=(page+1)*rpp)

                itemlist = [hit.docid for hit in hits[page*rpp:(page+1)*rpp]]

        return {
            'page': page,
            'rpp': rpp,
            'query': query,
            'itemlist': itemlist,
            'num_found': len(itemlist)
        }


class Recommender(object):

    def __init__(self):
        self.idx = None

    def index(self):
        pass

    def recommend_datasets(self, item_id, page, rpp):

        itemlist = []

        return {
            'page': page,
            'rpp': rpp,
            'item_id': item_id,
            'itemlist': itemlist,
            'num_found': len(itemlist)
        }

    def recommend_publications(self, item_id, page, rpp):

        itemlist = []

        return {
            'page': page,
            'rpp': rpp,
            'item_id': item_id,
            'itemlist': itemlist,
            'num_found': len(itemlist)
        }
