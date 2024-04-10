import copy
from sentence_transformers import SentenceTransformer, util
from confluent_kafka import Producer, Consumer

class CiGi_Embedder():
    def __init__(self, consumed_data):
        self.model = None
        self.output_data = None
        self.consumed_data = consumed_data
        ## Models
        # all-mpnet-base-v2, input_tokens: 384, embeddings_vector: 768, words: 288
        # multi-qa-mpnet-base-dot-v1, input_tokens: 512, embeddings_vector: 768, words: 384
        # all-distilroberta-v1, input_tokens: 512, embeddings_vector: 768, words: 384
        # all-MiniLM-L12-v2, input_tokens: 256, embeddings_vector: 384, words: 192
        # gtr-t5-large, input_tokens: 512, embeddings_vector: 768, words: 384
        # following the rule of thumb that the ratio between words and tokens is 3:4
        self.words_per_model ={
            "all-mpnet-base-v2" : 288,
            "multi-qa-mpnet-base-dot-v1" : 384,
            "all-distilroberta-v1" : 384,
            "all-MiniLM-L12-v2" : 192,
            "gtr-t5-large" : 384
        }

    def digest_input(self):
        ## CHECK IF LOADED INPUT HAS JSON/DICT FORMAT
        if type(self.consumed_data) is not dict:
            # print(self.consumed_data)
            return "not_dict"

        ## CHECK IF ANY JSON FIELDS ARE MISSING
        correct_keys = ["metadata_id","text","part","chapter","word_count","language","model"]
        existing_keys = list(self.consumed_data.keys())
        missing_keys = list(set(correct_keys) - set(existing_keys))
        if missing_keys:
            return missing_keys

        ## CHECK IF ANY JSON FIELDS ARE EMPTY
        empty_keys = []
        for key,value in self.consumed_data.items():
            if value==None or value=="":
                empty_keys.append(key)
        if empty_keys:
            return empty_keys

        ## CHECK IF INPUTS ARE OF CORRECT TYPE
        correct_data_types = {
            "metadata_id": str,
            "text": str,
            "part": int,
            "chapter": str,
            "word_count": int,
            "language": str,
            "model": str
        }
        wrong_data_types = {}
        for key,value in self.consumed_data.items():
            data_type = correct_data_types[key]
            if not isinstance(value, data_type):
                wrong_data_types[key] = str(type(value))
                # wrong_data_types[key] = type(value)
        if wrong_data_types:
            return wrong_data_types

        ## CHECK IF WORD COUNTS IS UNDER A IMPOSED LIMIT
        words_diff = self.consumed_data["word_count"] - self.words_per_model[self.consumed_data["model"]]
        if words_diff > 0:
            return words_diff


        return True

    def initialize_embedder(self):
        self.model = SentenceTransformer("./models/" + self.consumed_data["model"])
        return str(self.model.device)

    def embed(self):
        try:
            embeddings = self.model.encode(self.consumed_data["text"], normalize_embeddings=True).tolist()
        except Exception as e:
            return e
        self.output_data = copy.deepcopy(self.consumed_data)
        self.output_data["embeddings"] = embeddings
        return embeddings

    def output_data(self):
        return self.output_data


