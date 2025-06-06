FROM bitnami/spark:3.5.0

USER root

# Install Python 3 and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean

# Install required Python libraries
RUN pip3 install nibabel boto3 mne

# Switch back to non-root user
USER 1001
