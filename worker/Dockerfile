# Ingestion worker image. Built by AWS CodeBuild during ./deploy.sh (no local Docker needed)
# from a source bundle containing src/, worker/, and lambda-requirements.txt.
FROM public.ecr.aws/docker/library/python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY lambda-requirements.txt worker/requirements.txt ./
RUN python -m pip install --no-cache-dir --require-hashes -r lambda-requirements.txt \
 && python -m pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY worker/ worker/
# Infrastructure modules never enter the runtime image.
RUN rm -f src/stack.py src/config.py \
 && chmod -R a+rX /app \
 && useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin worker
USER 10001

CMD ["python", "-m", "worker.worker"]
