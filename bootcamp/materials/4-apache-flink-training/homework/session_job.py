import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session


def create_processed_events_source_kafka(t_env):
    table_name = "processed_events_kafka"
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            user_agent VARCHAR
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{os.environ.get("KAFKA_WEB_TRAFFIC_TOPIC")}',
            'properties.bootstrap.servers' = '{os.environ.get("KAFKA_BOOTSTRAP_SERVERS")}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_ip_activity_sink_postgres(t_env):
    table_name = "ip_activity_postgres"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            session_start TIMESTAMP,
            session_end TIMESTAMP,
            event_count BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{os.environ.get("POSTGRES_TABLE")}',
            'username' = '{os.environ.get("POSTGRES_USER")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD")}'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def process_events(t_env, source_table, sink_table):
    # Define a session window with a 5-minute gap based on the event_time column
    t_env.from_path(source_table) \
        .window(Session.with_gap(lit(5).minutes).on(col("event_time")).as_("session_window")) \
        .group_by(col("ip"), col("session_window")) \
        .select(
            col("ip"),
            col("session_window").start.alias("session_start"),  # Start time of the session
            col("session_window").end.alias("session_end"),      # End time of the session
            (col("session_window").end - col("session_window").start).alias("session_duration"),  # Duration of the session
            col("event_time").count.alias("event_count"),  # Total number of events in the session
            (col("event_count") / (col("session_duration").cast(DataTypes.DOUBLE()))).alias("avg_event_count_per_session")  # Average event count per session
        ) \
        .insert_into(sink_table)

def main():
    # Create the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)

    # Create source and sink tables
    source_table = create_processed_events_source_kafka(t_env)
    sink_table = create_ip_activity_sink_postgres(t_env)

    # Process events and insert results into the sink table
    process_events(t_env, source_table, sink_table)

    # Execute the Flink job
    env.execute("Sessionization Job")

if __name__ == "__main__":
    main()