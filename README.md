## Melange Extractor Coverage Report

For LLM model download (place in "models" folder):

```
# Make sure you have git-lfs installed (https://git-lfs.com)
git lfs install
git clone https://huggingface.co/sentence-transformers/all-mpnet-base-v2
```

For coverage report:

- coverage run test_melange_extractor.py
- coverage report -i
- (if needed) coverage xml -i

#### Deployment
In order to run the Docker image, you must have NVIDIA Container Toolkit installed both on the host and the container runtime (https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html).

##### Dev

`docker run -d --name melange-extractor --gpus all --restart unless-stopped --pull always sciohub/melange-extractor:dev`

##### Prod

`docker run -d --name melange-extractor --gpus all --restart unless-stopped --pull always sciohub/melange-extractor:prod`