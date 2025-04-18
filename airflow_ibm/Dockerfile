FROM quay.io/astronomer/astro-runtime:12.8.0

COPY requirements.txt .
RUN /usr/local/bin/install-python-dependencies

COPY .env .env
ENV $(cat .env | xargs)

COPY . .
