import copy
import unittest
import json
import melange_extractor
from sentence_transformers import util
from testcontainers.kafka import KafkaContainer
from confluent_kafka import Producer, Consumer
from dotenv import load_dotenv


class MyTestCase(unittest.TestCase):
    ## Models
    # all-mpnet-base-v2, input_tokens: 384, embeddings_vector: 768, words: 288
    # multi-qa-mpnet-base-dot-v1, input_tokens: 512, embeddings_vector: 768, words: 384
    # all-distilroberta-v1, input_tokens: 512, embeddings_vector: 768, words: 384
    # all-MiniLM-L12-v2, input_tokens: 256, embeddings_vector: 384, words: 192
    # gtr-t5-large, input_tokens: 512, embeddings_vector: 768, words: 384
    # following the rule of thumb that the ratio between words and tokens is 3:4
    def setUp(self):
        self.text = """Observing the sources of data used to train the DL model at every paper, large datasets of images are mainly used, containing thousands of images in some cases, either real ones (e.g. (Mohanty et al., 2016; Reyes et al., 2015; Dyrmann et al., 2016a)), or synthetic produced by the authors (Rahnemoonfar and Sheppard, 2017; Dyrmann et al., 2016b). Some datasets originate from well-known and publicly-available datasets such as PlantVillage, LifeCLEF, MalayaKew, UC Merced and Flavia (see Appendix C), while others constitute sets of real images collected by the authors for their research needs (e.g. (Sladojevic et al., 2016; Bargoti and Underwood, 2016; Xinshao and Cheng, 2015; Sψrensen et al., 2017)). Papers dealing with land cover, crop type classification and yield estimation, as well as some papers related to weed detection employ a smaller number of images (e.g. tens of images), produced by UAV (Lu et al., 2017; Rebetez et al., 2016; Milioto et al., 2017), airborne (Chen et al., 2014; Luus et al., 2015) or satellitebased remote sensing (Kussul et al., 2017; Minh et al., 2017; Ienco et al., 2017; Ruίwurm and Kφrner, 2017). A particular paper investigating segmentation of root and soil uses images from X-ray tomography (Douarre et al., 2016). Moreover, some papers use text data, collected either from repositories (Kuwata and Shibasaki, 2015; Sehgal et al., 2017) or field sensors (Song et al., 2016; Demmers et al., 2010, 2012). In general, the more complicated the problem to be solved, the more data is required. For example, problems involving large number of classes to identify (Mohanty et al., 2016; Reyes et al., 2015; Xinshao and Cheng, 2015) and/or small Variation among the classes (Luus et al., 2015; Ruίwurm and Kφrner, 2017; Yalcin, 2017; Namin et al., 2017; Xinshao and Cheng, 2015), require large number of input images to train their models. In general, the more complicated the problem to be solved, the more data is required. For example, problems involving large number of classes to identify (Mohanty et al., 2016; Reyes et al., 2015; Xinshao and Cheng, 2015) and/or small Variation among the classes (Luus et al., 2015; Ruίwurm and Kφrner, 2017; Yalcin, 2017; Namin et al., 2017; Xinshao and Cheng, 2015), require large number of input images to train their models. In general, the more complicated the problem to be solved, the more data is required."""
        self.query = {"query" : "Is machine learning used in agriculture?"}
        self.corpus_texts = {
            "Economics": "The economic viability of the financial sector, in a globalized economy is in danger due to inflation.",
            "Cerebral": "Brain activity is crucial for the health and well-being of the human body, leading to immunity against diseases.",
            "Mathematics": "In the mathematical world, everything can be modeled through differential equations, derivatives and algebra.",
            "Political_science": "The political landscape of a democracy can be valued though its laws and republican elections of the parliament",
            "DL_agriculture": "Agriculture is a very important area, where Deep Learning can be utilized through image processing to improve yield and crop quality.",
            "Muscle_hypertrophy": "The main aim of training is towards the well-being, the muscle growth and overall body strength, as these factors affect health greatly."}

    def test_wrong_json_type(self):
        sample_json = []

        embedder = melange_extractor.CiGi_Embedder(sample_json)
        self.assertEqual(embedder.digest_input(), "not_dict", "JSON PROVIDED IS NOT OF CORRECT FORMAT")

    def test_missing_input_json_fields(self):
        sample_json = {
            "metadata_id": "12345",
            "text": "The input text to be summarized. It can be a long article, document, or any textual content.",
            "part": 2,
            "chapter": "Methods",
            "word_count": 17,
            "language": "en",
            "model": "all-mpnet-base-v2"
        }
        del sample_json['model']
        del sample_json['language']
        embedder = melange_extractor.CiGi_Embedder(sample_json)
        self.assertNotEqual(embedder.digest_input(), [], "MISSING INPUT JSON FIELDS DETECTED")

    def test_empty_input_json_fields(self):
        sample_json = {
            "metadata_id": "12345",
            "text": "The input text to be summarized. It can be a long article, document, or any textual content.",
            "part": 2,
            "chapter": "Methods",
            "word_count": 17,
            "language": "en",
            "model": "all-mpnet-base-v2"
        }
        sample_json['part'] = None
        sample_json['chapter'] = ""
        embedder = melange_extractor.CiGi_Embedder(sample_json)
        self.assertNotEqual(embedder.digest_input(), [], "EMPTY INPUT JSON FIELDS DETECTED")

    def test_data_types_input_json_fields(self):
        sample_json = {
            "metadata_id": 12345,
            "text": "The input text to be summarized. It can be a long article, document, or any textual content.",
            "part": True,
            "chapter": {},
            "word_count": "100",
            "language": ["en"],
            "model": "all-mpnet-base-v2"
        }

        embedder = melange_extractor.CiGi_Embedder(sample_json)
        self.assertNotEqual(embedder.digest_input(), {}, "WRONG DATA TYPES INPUT JSON FIELDS DETECTED")

    def test_text_word_length(self):
        sample_json = {
              "metadata_id": "12345",
              "text": self.text,
              "part": 2,
              "chapter": "Methods",
              "word_count": 600,
              "language": "en",
              "model": "all-mpnet-base-v2"
            }

        embedder = melange_extractor.CiGi_Embedder(sample_json)
        self.assertGreaterEqual(embedder.digest_input(), 0, "TEXT WORDS COUNT ABOVE MODEL'S LIMIT")


    # def test_model_initialization(self):
    #     sample_json = {
    #           "metadata_id": "12345",
    #           "text": "The input text to be summarized. It can be a long article, document, or any textual content.",
    #           "part": 2,
    #           "chapter": "Methods",
    #           "word_count": 100,
    #           "language": "en",
    #           "model": "all-mpnet-base-v2"
    #         }
    #     embedder = melange_extractor.CiGi_Embedder(sample_json)
    #     embedder.digest_input()
    #     self.assertEqual(embedder.initialize_embedder().startswith("cuda"), True, "MODEL IS NOT USING CUDA")

    def test_embeddings_length(self):
        sample_json = {
            "metadata_id": "12345",
            "text": self.text,
            "part": 2,
            "chapter": "Methods",
            "word_count": 100,
            "language": "en",
            "model": "all-mpnet-base-v2"
        }
        embedder = melange_extractor.CiGi_Embedder(sample_json)
        embedder.digest_input()
        embedder.initialize_embedder()
        embeddings = embedder.embed()
        embeddings_length = len(embeddings)
        self.assertEqual(embeddings_length, 768, "ERROR IN EMBEDDINGS SIZE")

    def test_correct_semantic_option(self):
        sample_json = {
            "metadata_id": "12345",
            "text": self.query["query"],
            "part": 2,
            "chapter": "Methods",
            "word_count": 100,
            "language": "en",
            "model": "all-mpnet-base-v2"
        }

        embedder = melange_extractor.CiGi_Embedder(sample_json)
        embedder.digest_input()
        embedder.initialize_embedder()
        query_emb = embedder.embed()

        texts_emb = {}
        for topic,text in self.corpus_texts.items():
            sample_json["text"] = text
            embedder = melange_extractor.CiGi_Embedder(sample_json)
            embedder.digest_input()
            embedder.initialize_embedder()
            embeddings = embedder.embed()
            texts_emb[topic] = embeddings

        results = {}
        for topic,text_emb in texts_emb.items():
            results[topic] = util.cos_sim(query_emb, text_emb).item()

        selected_text = max(results, key=results.get)
        self.assertEqual(selected_text, "DL_agriculture", "ERROR IN SEMANTIC RESULTS")

    def test_kafka_produce(self):
        sample_json = {
            "metadata_id": "12345",
            "text": self.query["query"],
            "part": 2,
            "chapter": "Methods",
            "word_count": 100,
            "language": "en",
            "model": "all-mpnet-base-v2"
        }
        embedder = melange_extractor.CiGi_Embedder(sample_json)
        embedder.digest_input()
        embedder.initialize_embedder()
        embedder.embed()
        embedder_output = json.dumps(embedder.output_data)

        with KafkaContainer(image='confluentinc/cp-kafka:7.5.0') as kafka_container:
            bootstrap_server = kafka_container.get_bootstrap_server()

            config = {
                'bootstrap.servers': bootstrap_server
            }
            topic = "kafka_test"

            def delivery_callback(err, msg):
                if err:
                    print('ERROR: Message failed delivery: {}'.format(err))
                    print("Failed to deliver message: %s" % (str(msg)))
                else:
                    if msg.value() == None:
                        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                            topic=msg.topic(), key=msg.key().decode('utf-8'), value=""))
                    else:
                        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

            # Create Producer instance
            producer = Producer(config)
            producer.produce(topic, key="kafka_produce_test", value=embedder_output, callback=delivery_callback)
            producer.flush()

            config = {
                'bootstrap.servers': bootstrap_server,
                'group.id': 'python_example_group_1',
                'auto.offset.reset': 'earliest'}
            # Create Consumer instance
            consumer = Consumer(config)
            consumer.subscribe([topic])

            # Poll for new messages from Kafka and print them.
            try:
                while True:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        # Initial message consumption may take up to
                        # `session.timeout.ms` for the consumer group to
                        # rebalance and start consuming
                        print("Waiting...")
                    elif msg.error():
                        print("ERROR: %s".format(msg.error()))
                    else:
                        # Extract the (optional) key and value, and print.
                        if msg.value() == None:
                            print("Consumed event to topic {topic}: key = {key:12} value = {value:12}".format(
                                topic=msg.topic(), key=msg.key().decode('utf-8'), value=""))
                        else:
                            print("Consumed event to topic {topic}: key = {key:12} value = {value:12}".format(
                                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

                            consumed_data = json.loads(msg.value())
                            print(consumed_data)
                            self.assertIsInstance(consumed_data, dict, "ERROR IN RECEIVED JSON")
                            break
            except KeyboardInterrupt:
                pass
            finally:
                # Leave group and commit final offsets
                consumer.close()


if __name__ == '__main__':
    unittest.main()
