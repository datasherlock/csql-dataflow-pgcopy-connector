FROM python:3.12-slim
COPY --from=apache/beam_python3.12_sdk:2.58.1 /opt/apache/beam /opt/apache/beam

# Copy Flex Template launcher binary from the launcher image, which makes it
# possible to use the image as a Flex Template base image.
COPY --from=gcr.io/dataflow-templates-base/python312-template-launcher-base:latest /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

# Location to store the pipeline artifacts.
ARG WORKDIR=/template
WORKDIR ${WORKDIR}
COPY main.py .
COPY setup.py .

# Uncomment this if you are using a private repo
# COPY pip.conf /etc/pip.conf

COPY requirements.txt .
COPY common common
COPY pipelines pipelines
COPY sinks sinks
COPY sources sources

# Installing exhaustive list of dependencies from a requirements.txt helps to ensure that every time Docker container
# image is built, the Python dependencies stay the same. Using '--no-cache-dir' reduces image size.
RUN pip install --no-cache-dir -r requirements.txt

# For more informaiton, see: https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
# Because this image will be used as custom sdk container image, and it already
# installs the dependencies from the requirements.txt, we can omit
# the FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE directive here
# to reduce pipeline submission time.

# Optionally, verify that dependencies are not conflicting.
# A conflict may or may not be significant for your pipeline.
RUN pip check
# Optionally, list all installed dependencies.
# The output can be used to seed requirements.txt for reproducible builds.
RUN pip freeze
# Set the entrypoint to Apache Beam SDK launcher, which allows this image
# to be used as an SDK container image.
ENTRYPOINT ["/opt/apache/beam/boot"]