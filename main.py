import argparse
import boto3
import ast
from opentelemetry import metrics
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    PeriodicExportingMetricReader,
)
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

'''
    Пример запуска скрипта:
        ./venv/bin/python3 main.py \
            --s3_endpoint_url 'https://storage.yandexcloud.net' \
            --s3_region_name 'ru-central1' \
            --s3_aws_access_key_id 'a1b2c3d4aqweqwe' \
            --s3_aws_secret_access_key 'Yer123sdf456sdf790' \
            --s3_bucket_name 'some-name' \
            --otel_attributes "{'namespace': 'monitoring','role': 'metrics'}"
'''

def get_s3_bucket_size_bytes(
    s3_endpoint_url: str,
    s3_region_name: str,
    s3_aws_access_key_id: str,
    s3_aws_secret_access_key: str,
    s3_bucket_name: str,
) -> float: 
    session = boto3.Session()
    s3 = session.client(service_name='s3', endpoint_url=s3_endpoint_url, region_name=s3_region_name,
                        aws_access_key_id=s3_aws_access_key_id, aws_secret_access_key=s3_aws_secret_access_key)

    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket = s3_bucket_name)

    total_size = 0
    for page in page_iterator:
        items = page['Contents']
        for item in items:
            total_size += item['Size']

    mbSize = total_size / 1024 / 1024
    print(f"Total size of objects in {s3_bucket_name} bucket: {mbSize:.2f} MBytes")
    return total_size

# OpenTelemetry
def send_metric(
    otel_service_name: str,
    otel_endpoint: str,
    otel_metric_name: str,
    otel_attributes: str,
    value: int | float,
):
    exporter = OTLPMetricExporter(endpoint=otel_endpoint,)
    metric_reader = PeriodicExportingMetricReader(exporter)
    resource = Resource(attributes={
        SERVICE_NAME: otel_service_name
    })
    provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics.set_meter_provider(provider)
    meter = metrics.get_meter("shared_meter")
    s3_size_metric = meter.create_gauge(name=otel_metric_name, description="Size of S3 bucket in bytes")# , unit="bytes"
    s3_size_metric.set(
        amount=value,
        attributes=otel_attributes
    )
    provider.force_flush()

def collect_metric(
    s3_endpoint_url: str,
    s3_region_name: str,
    s3_aws_access_key_id: str,
    s3_aws_secret_access_key: str,
    s3_bucket_name: str,
    otel_service_name: str,
    otel_endpoint: str,
    otel_metric_name: str,
    otel_attributes: str,
):
    s3_size_bytes = get_s3_bucket_size_bytes(
        s3_endpoint_url=s3_endpoint_url,
        s3_region_name=s3_region_name,
        s3_aws_access_key_id=s3_aws_access_key_id,
        s3_aws_secret_access_key=s3_aws_secret_access_key,
        s3_bucket_name=s3_bucket_name,
    )
    send_metric(
        otel_service_name=otel_service_name,
        otel_endpoint=otel_endpoint,
        otel_metric_name=otel_metric_name,
        otel_attributes=otel_attributes,
        value=s3_size_bytes
    )


def main():
    # Аргументы
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3_endpoint_url", type=str, help="URL s3-сервиса. Например, 'https://storage.yandexcloud.net'")
    parser.add_argument("--s3_region_name", type=str, help="Название региона. Например, 'ru-central1'")
    parser.add_argument("--s3_aws_access_key_id", type=str, help="access_key_id")
    parser.add_argument("--s3_aws_secret_access_key", type=str, help="aws_secret_access_key")
    parser.add_argument("--s3_bucket_name", type=str, help="Название бакета")
    # Аргументы OpenTelemetry
    parser.add_argument("--otel_service_name", type=str, default='s3_monitoring', 
                        help="Название сервиса, который формирует метрики. Данный аргумент отображается в лейбле job.")
    parser.add_argument("--otel_endpoint", default='http://localhost:4317', type=str, help="URL-адрес коллектора. Например, 'http://localhost:4317'")
    parser.add_argument("--otel_metric_name", default='s3_size', type=str, help="Название метрики. Например, 's3_size'")
    help_attributes = "Поле аттрибутов в виде строкового представления dict. "
    help_attributes += "Например, \"{'namespace': 'monitoring','role': 'logs'}\""
    parser.add_argument("--otel_attributes", type=str, help=help_attributes)
    args = parser.parse_args()

    collect_metric(
        s3_endpoint_url=args.s3_endpoint_url,
        s3_region_name=args.s3_region_name,
        s3_aws_access_key_id=args.s3_aws_access_key_id,
        s3_aws_secret_access_key=args.s3_aws_secret_access_key,
        s3_bucket_name=args.s3_bucket_name,
        otel_service_name=args.otel_service_name,
        otel_endpoint=args.otel_endpoint,
        otel_metric_name=args.otel_metric_name,
        otel_attributes=ast.literal_eval(args.otel_attributes),
    )

if __name__ == "__main__":
    main()