from datetime import timedelta
from google.protobuf.duration_pb2 import Duration

from feast import FileSource, Entity, Feature, FeatureView, ValueType

driver = Entity(name="driver_id", join_key="driver_id", value_type=ValueType.INT64,)

# driver_stats_source = BigQuerySource(
#     table_ref="feast-oss.demo_data.driver_hourly_stats",
#     event_timestamp_column="datetime",
#     created_timestamp_column="created",
# )

driver_hourly_stats = FileSource(
    path="/Users/pengju.pan/gitcode/feast-driver-ranking-tutorial/driver_ranking/data/driver_stats.parquet",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)


driver_hourly_stats_view = FeatureView(
    name="driver_hourly_stats",  
    entities=["driver_id"],
    ttl=timedelta(weeks=52), # 保存feature的时间,0:永久保存；
    features=[
        Feature(name="conv_rate", dtype=ValueType.FLOAT),
        Feature(name="acc_rate", dtype=ValueType.FLOAT),
        Feature(name="avg_daily_trips", dtype=ValueType.INT64),
    ],
    batch_source=driver_hourly_stats,   # 数据来源
    online=True,
    tags={"team": "driver_performance"},
)