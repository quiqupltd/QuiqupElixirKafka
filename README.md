# QuiqupElixirKafka

Wanted to make use of the cool work done by Sigstr here: https://bitbucket.org/sigstr/sigstr-elixir-kafka/

But ran into issues using it, so forked and updated to meet our needs.

----

This module allows you to connect to Kafka and consume messages without crashing your app when Kafka goes offline. In the event of a Kafka outage it will attempt to reconnect once a minute.

## Step 1

Add QuiqupElixirKafka to `mix.exs`:

```elixir
def deps do
  [
    # OTHER STUFF,
    {:quiqup_elixir_kafka, git: "https://github.com/quiqupltd/QuiqupElixirKafka.git"}
  ]
end
```

## Step 2

`mix deps.get`

## Step 3

Add universal Kafka config values to `config.exs`:

```elixir
config :kafka_ex,
  disable_default_worker: true,
  use_ssl: false,
  kafka_version: "1.1"
```

Add your dev Kafka broker to `dev.exs`:

```elixir
config :kafka_ex, brokers: [{"localhost", 9092}]
```

Production Kafka brokers are specified by environment variable:
`KAFKA_SERVERS=broker1:9092,broker2:9093,broker3:9094`

## Step 4

Implement one or more GenConsumers in your project as described in the [KafkaEx docs](https://hexdocs.pm/kafka_ex/KafkaEx.GenConsumer.html#content).

## Step 5

Start KafkaMonitor and ConsumerSupervisor in `application.ex`:

```elixir
kafka_genconsumers = [
  %{
    id: MyGenConsumer,
    start: {KafkaEx.ConsumerGroup, :start_link, [MyGenConsumer, "my-consumer-group", ["my-topic"]]}
  }
]

children = [
  # OTHER STUFF,
  worker(QuiqupElixirKafka.KafkaMonitor, [[]]),
  worker(QuiqupElixirKafka.ConsumerSupervisor, [kafka_genconsumers]),
]

opts = [strategy: :one_for_one, name: MyApp.Supervisor]
Supervisor.start_link(children, opts)
```