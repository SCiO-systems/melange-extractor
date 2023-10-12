FROM nvidia/cuda:11.7.1-runtime-ubuntu22.04

ENV PYTHON_VERSION=3.10

WORKDIR /app

# OS dependencies
RUN apt-get update && apt-get upgrade -y 
RUN apt-get install pip sudo software-properties-common git curl -y
RUN curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | bash
RUN apt-get install git-lfs -y
RUN git lfs install
RUN add-apt-repository ppa:deadsnakes/ppa -y && apt-get update
RUN apt-get install python${PYTHON_VERSION}

# LLMs download
RUN git clone https://huggingface.co/sentence-transformers/all-mpnet-base-v2 models/all-mpnet-base-v2

# Python packages installation
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "main.py"]